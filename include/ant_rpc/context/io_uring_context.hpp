#pragma once

#include <poll.h>

#include <cerrno>

#include "ant_rpc/context/context_control.hpp"
#include "liburing.h"

class IoUringContext : public ContextControl {
 public:
  IoUringContext(std::size_t entries, Scheduler& scheduler, Executor& executor, TimerKeeper& timer_keeper)
      : ContextControl(scheduler, executor, timer_keeper) {
    io_uring_queue_init(entries, &ring_, 0);
    io_uring_register_files_sparse(&ring_, static_cast<unsigned>(entries * 4));
  }
  ~IoUringContext() override { io_uring_queue_exit(&ring_); }

  io_uring_sqe* GetSqe() noexcept { return io_uring_get_sqe(&ring_); }
  void Submit() noexcept { io_uring_submit(&ring_); }

  int ProcessEvents(int wait_nr = 1) {
    ArmWakeupPoll();
    const int result = io_uring_submit_and_wait(&ring_, wait_nr);
    if (result < 0) {
      return result;
    }
    unsigned head = 0, count = 0;
    io_uring_cqe* cqe = nullptr;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      auto* handler = static_cast<IOHandler*>(io_uring_cqe_get_data(cqe));
      if (handler == &wakeup_handler_) {
        wakeup_poll_armed_ = false;
      }
      if (handler) {
        handler->prepare_complete(cqe->res, cqe->flags);
        handler->on_complete();
      }
      ++count;
    }
    io_uring_cq_advance(&ring_, count);
    return static_cast<int>(count);
  }
  void Stop() {
    running_ = false;
    Wakeup();
  }

 private:
  struct WakeupHandler final : IOHandler {
    explicit WakeupHandler(IoUringContext& context) : context(context) {}
    void on_complete() override { context.DrainMailboxesOnIoThread(); }
    IoUringContext& context;
  };
  void ArmWakeupPoll() {
    if (wakeup_poll_armed_ || wakeup_fd() < 0) {
      return;
    }
    auto* sqe = GetSqe();
    if (!sqe) {
      return;
    }
    io_uring_prep_poll_add(sqe, wakeup_fd(), POLLIN);
    io_uring_sqe_set_data(sqe, &wakeup_handler_);
    wakeup_poll_armed_ = true;
  }

  io_uring ring_ {};
  bool running_ {false};
  bool wakeup_poll_armed_ {false};
  WakeupHandler wakeup_handler_ {*this};
};
