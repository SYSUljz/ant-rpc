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

  using IoCommandHandler = void (*)(IoCommand&&);

  inline static IoCommandHandler s_server_dispatcher {nullptr};
  inline static IoCommandHandler s_channel_dispatcher {nullptr};

  static void SetServerDispatcher(IoCommandHandler handler) noexcept {
    s_server_dispatcher = handler;
  }
  static void SetChannelDispatcher(IoCommandHandler handler) noexcept {
    s_channel_dispatcher = handler;
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
    commands_.Drain([](IoCommand&& cmd) {
      if (static_cast<uint8_t>(cmd.type) <= static_cast<uint8_t>(IoCommand::Type::kServerClose)) {
        if (s_server_dispatcher) {
          s_server_dispatcher(std::move(cmd));
        }
      } else {
        if (s_channel_dispatcher) {
          s_channel_dispatcher(std::move(cmd));
        }
      }
    });
  }

 private:
  MpscQueue<IoCommand> commands_;
  MpscQueue<std::shared_ptr<IoCommandMailbox>> mailboxes_;
  std::atomic<bool> notified_ {false};
  Scheduler* scheduler_ {nullptr};
  Executor* executor_ {nullptr};
  TimerKeeper* timer_keeper_ {nullptr};
  int wakeup_fd_ {-1};
};
