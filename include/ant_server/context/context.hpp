#pragma once

#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <typeindex>
#include <typeinfo>

#include <sys/eventfd.h>

#include "absl/container/flat_hash_map.h"
#include "ant_server/context/io_command.hpp"
#include "ant_server/type.hpp"
#include "liburing.h"

// Base class for services registered inside Context
struct BaseService {
  virtual ~BaseService() = default;
};

// ============================================================================
// Context: Dedicated Single-Threaded IO Reactor (io_uring Demultiplexer)
// 100% Lock-Free: Exclusively owned and driven by a single IO thread.
// ============================================================================
class Context {
 public:
  explicit Context(std::size_t entries = 256, Scheduler* scheduler = nullptr, Executor* executor = nullptr)
      : scheduler_(scheduler), executor_(executor) {
    io_uring_queue_init(entries, &ring_, 0);
    io_uring_register_files_sparse(&ring_, static_cast<unsigned>(entries * 4));
    wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  }

  ~Context() {
    if (wakeup_fd_ >= 0) {
      close(wakeup_fd_);
    }
    io_uring_queue_exit(&ring_);
  }

  // Lock-free single-threaded SQE submission
  inline io_uring_sqe* GetSqe() noexcept { return io_uring_get_sqe(&ring_); }
  inline void Submit() noexcept { io_uring_submit(&ring_); }

  // Thread-safe producer entry point. Context accepts an explicit mailbox,
  // never an arbitrary callback; mailbox commands remain domain-typed.
  void Notify(std::shared_ptr<IoCommandMailbox> mailbox) {
    mailboxes_.Push(std::move(mailbox));
    Wakeup();
  }

  std::size_t PendingCommandCount() const noexcept { return mailboxes_.ApproximateSize(); }

  // Safe from every thread: unlike the old NOP-based implementation this does
  // not touch the io_uring submission queue.
  // why there is a write rather than a co_await write_awaiter
  void Wakeup() noexcept {
    if (wakeup_fd_ >= 0) {
      uint64_t one = 1;
      ssize_t result = write(wakeup_fd_, &one, sizeof(one));
      (void)result;  // EAGAIN means a previous notification is already pending.
    }
  }

  void SetExecutor(Executor* executor) noexcept { executor_ = executor; }
  Executor* GetExecutor() const noexcept { return executor_; }

  void SetScheduler(Scheduler* scheduler) noexcept { scheduler_ = scheduler; }
  Scheduler* GetScheduler() const noexcept { return scheduler_; }

  // Lock-free single-threaded service container for IO reactor
  template <typename ServiceType>
  ServiceType& UseService() {
    std::type_index id(typeid(ServiceType));
    auto it = services_.find(id);
    if (it != services_.end()) {
      return static_cast<ServiceType&>(*it->second);
    }
    auto new_service = std::make_unique<ServiceType>(*this);
    auto& ref = *new_service;
    services_[id] = std::move(new_service);
    return ref;
  }

  inline int ProcessEvents(int wait_nr = 1) {
    ArmWakeupPoll();
    int ret = io_uring_submit_and_wait(&ring_, wait_nr);
    if (ret < 0) {
      return ret;
    }
    unsigned head = 0;
    unsigned count = 0;
    struct io_uring_cqe* cqe;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      void* user_data = io_uring_cqe_get_data(cqe);
      if (user_data) {
        auto* handler = static_cast<IOHandler*>(user_data);
        handler->prepare_complete(cqe->res, cqe->flags);
        handler->on_complete();
      }
      count++;
    }
    io_uring_cq_advance(&ring_, count);
    return count;
  }

  void Start() {
    running_ = true;
    while (running_) {
      int ret = ProcessEvents(1);
      if (ret < 0) {
        if (ret == -EINTR) {
          continue;
        }
        break;
      }
    }
  }

  void Stop() {
    running_ = false;
    Wakeup();
  }

 private:
  struct WakeupHandler final : IOHandler {
    explicit WakeupHandler(Context& context) : context_(context) {}
    void on_complete() override { context_.OnWakeup(); }
    Context& context_;
  };

  void ArmWakeupPoll() {
    if (wakeup_poll_armed_ || wakeup_fd_ < 0) {
      return;
    }
    io_uring_sqe* sqe = GetSqe();
    if (!sqe) {
      return;
    }
    io_uring_prep_poll_add(sqe, wakeup_fd_, POLLIN);
    io_uring_sqe_set_data(sqe, &wakeup_handler_);
    wakeup_poll_armed_ = true;
  }

  void OnWakeup() {
    wakeup_poll_armed_ = false;
    uint64_t ignored;
    while (read(wakeup_fd_, &ignored, sizeof(ignored)) == sizeof(ignored)) {
    }
    mailboxes_.Drain([](std::shared_ptr<IoCommandMailbox>&& mailbox) { mailbox->DrainCommandsOnIoThread(); });
    ArmWakeupPoll();
  }

  struct io_uring ring_;
  uint32_t timeout_ms_ {0};
  std::size_t thread_id_ {0};
  bool running_ {false};
  int wakeup_fd_ {-1};
  bool wakeup_poll_armed_ {false};
  WakeupHandler wakeup_handler_ {*this};
  MpscQueue<std::shared_ptr<IoCommandMailbox>> mailboxes_;
  absl::flat_hash_map<std::type_index, std::unique_ptr<BaseService>> services_;
  Scheduler* scheduler_ {nullptr};
  Executor* executor_ {nullptr};
};
