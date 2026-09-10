#pragma once

#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include <sys/eventfd.h>

#include "ant_server/context/io_command.hpp"
#include "ant_server/type.hpp"

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

  void Notify(std::shared_ptr<IoCommandMailbox> mailbox) {
    mailboxes_.Push(std::move(mailbox));
    Wakeup();
  }
  [[nodiscard]] std::size_t PendingCommandCount() const noexcept { return mailboxes_.ApproximateSize(); }
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

 protected:
  void DrainMailboxesOnIoThread() {
    uint64_t ignored;
    while (read(wakeup_fd_, &ignored, sizeof(ignored)) == sizeof(ignored)) {
    }
    mailboxes_.Drain([](std::shared_ptr<IoCommandMailbox>&& mailbox) { mailbox->DrainCommandsOnIoThread(); });
  }

 private:
  MpscQueue<std::shared_ptr<IoCommandMailbox>> mailboxes_;
  Scheduler* scheduler_ {nullptr};
  Executor* executor_ {nullptr};
  TimerKeeper* timer_keeper_ {nullptr};
  int wakeup_fd_ {-1};
};
