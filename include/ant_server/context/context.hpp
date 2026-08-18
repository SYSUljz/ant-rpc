#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <typeindex>
#include <typeinfo>

#include "absl/container/flat_hash_map.h"
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
  }

  ~Context() { io_uring_queue_exit(&ring_); }

  // Lock-free single-threaded SQE submission
  inline io_uring_sqe* GetSqe() noexcept { return io_uring_get_sqe(&ring_); }
  inline void Submit() noexcept { io_uring_submit(&ring_); }

  inline void Wakeup() {
    io_uring_sqe* sqe = GetSqe();
    if (sqe) {
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data(sqe, nullptr);
      Submit();
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
  struct io_uring ring_;
  uint32_t timeout_ms_ {0};
  std::size_t thread_id_ {0};
  bool running_ {false};
  absl::flat_hash_map<std::type_index, std::unique_ptr<BaseService>> services_;
  Scheduler* scheduler_ {nullptr};
  Executor* executor_ {nullptr};
};
