#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <sys/eventfd.h>

#include "ant_rpc/context/io_command.hpp"
#include "ant_rpc/type.hpp"

// Backend-independent control plane. It intentionally owns no read/write or
// accept operation API: epoll and io_uring have different IO semantics.
class ContextControl {
 public:
  ContextControl(Scheduler& scheduler, Executor& executor, TimerKeeper& timer_keeper)
      : scheduler_(&scheduler),
        executor_(&executor),
        timer_keeper_(&timer_keeper),
        wakeup_fd_(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {}
  ContextControl(const ContextControl&) = delete;
  ContextControl& operator=(const ContextControl&) = delete;
  virtual ~ContextControl() {
    if (wakeup_fd_ >= 0) {
      close(wakeup_fd_);
    }
  }

  void PostCommand(IoCommand command) {
    commands_.Push(std::move(command));
    if (!notified_.exchange(true, std::memory_order_release)) {
      Wakeup();
    }
  }

  void Notify(std::shared_ptr<IoCommandMailbox> mailbox) {
    mailboxes_.Push(std::move(mailbox));
    if (!notified_.exchange(true, std::memory_order_release)) {
      Wakeup();
    }
  }
  [[nodiscard]] std::size_t PendingCommandCount() const noexcept {
    return commands_.ApproximateSize() + mailboxes_.ApproximateSize();
  }
  void Wakeup() noexcept {
    if (wakeup_fd_ < 0) {
      return;
    }
    uint64_t one = 1;
    const ssize_t ignored = write(wakeup_fd_, &one, sizeof(one));
    (void)ignored;  // EAGAIN means a notification is already pending.
  }
  [[nodiscard]] int wakeup_fd() const noexcept { return wakeup_fd_; }
  Executor* GetExecutor() const noexcept { return executor_; }
  Scheduler& GetScheduler() const noexcept { return *scheduler_; }
  TimerKeeper& GetTimerKeeper() const noexcept { return *timer_keeper_; }

  using IoCommandHandler = void (*)(IoCommand&&, bool defer_write);
  using IoCommandFlushHandler = void (*)(void* connection);

  inline static IoCommandHandler s_server_dispatcher {nullptr};
  inline static IoCommandHandler s_channel_dispatcher {nullptr};
  inline static IoCommandFlushHandler s_server_flush {nullptr};
  inline static IoCommandFlushHandler s_channel_flush {nullptr};

  static void SetServerDispatcher(IoCommandHandler handler, IoCommandFlushHandler flush = nullptr) noexcept {
    s_server_dispatcher = handler;
    s_server_flush = flush;
  }
  static void SetChannelDispatcher(IoCommandHandler handler, IoCommandFlushHandler flush = nullptr) noexcept {
    s_channel_dispatcher = handler;
    s_channel_flush = flush;
  }

 protected:
  void DrainMailboxesOnIoThread() {
    uint64_t ignored;
    while (read(wakeup_fd_, &ignored, sizeof(ignored)) == sizeof(ignored)) {
    }
    notified_.store(false, std::memory_order_release);
    DrainCommandsOnIoThread();
    mailboxes_.Drain([](std::shared_ptr<IoCommandMailbox>&& mailbox) { mailbox->DrainCommandsOnIoThread(); });
  }

  void DrainCommandsOnIoThread() {
    touched_server_conns_.clear();
    touched_channel_conns_.clear();

    // Phase 1: Drain all commands, enqueueing frames to respective connection outbound queues
    commands_.Drain([this](IoCommand&& cmd) {
      if (static_cast<uint8_t>(cmd.type) <= static_cast<uint8_t>(IoCommand::Type::kServerClose)) {
        if (s_server_dispatcher) {
          if (cmd.type == IoCommand::Type::kServerCompleteInbound) {
            void* conn = cmd.connection;
            s_server_dispatcher(std::move(cmd), /*defer_write=*/true);
            touched_server_conns_.push_back(conn);
          } else {
            s_server_dispatcher(std::move(cmd), /*defer_write=*/false);
          }
        }
      } else {
        if (s_channel_dispatcher) {
          if (cmd.type == IoCommand::Type::kChannelSendFrame) {
            void* conn = cmd.connection;
            s_channel_dispatcher(std::move(cmd), /*defer_write=*/true);
            touched_channel_conns_.push_back(conn);
          } else {
            s_channel_dispatcher(std::move(cmd), /*defer_write=*/false);
          }
        }
      }
    });

    // Phase 2: For each touched connection, trigger batched frame send
    if (!touched_server_conns_.empty()) {
      if (s_server_flush) {
        if (touched_server_conns_.size() > 1) {
          std::sort(touched_server_conns_.begin(), touched_server_conns_.end());
          touched_server_conns_.erase(
              std::unique(touched_server_conns_.begin(), touched_server_conns_.end()),
              touched_server_conns_.end());
        }
        for (void* conn : touched_server_conns_) {
          s_server_flush(conn);
        }
      }
      touched_server_conns_.clear();
    }

    if (!touched_channel_conns_.empty()) {
      if (s_channel_flush) {
        if (touched_channel_conns_.size() > 1) {
          std::sort(touched_channel_conns_.begin(), touched_channel_conns_.end());
          touched_channel_conns_.erase(
              std::unique(touched_channel_conns_.begin(), touched_channel_conns_.end()),
              touched_channel_conns_.end());
        }
        for (void* conn : touched_channel_conns_) {
          s_channel_flush(conn);
        }
      }
      touched_channel_conns_.clear();
    }
  }

 private:
  MpscQueue<IoCommand> commands_;
  MpscQueue<std::shared_ptr<IoCommandMailbox>> mailboxes_;
  std::vector<void*> touched_server_conns_;
  std::vector<void*> touched_channel_conns_;
  std::atomic<bool> notified_ {false};
  Scheduler* scheduler_ {nullptr};
  Executor* executor_ {nullptr};
  TimerKeeper* timer_keeper_ {nullptr};
  int wakeup_fd_ {-1};
};
