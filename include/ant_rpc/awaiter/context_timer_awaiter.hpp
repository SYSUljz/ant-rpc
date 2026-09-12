#pragma once

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <utility>

#include "ant_rpc/context/context.hpp"
#include "ant_rpc/scheduler/timer_keeper.hpp"

namespace ant_rpc {

// A timer completion belongs to the Context that armed it. TimerKeeper may run
// callbacks on a worker, but it only posts this explicit mailbox; the suspended
// coroutine itself is always resumed by Context::OnWakeup on the IO thread.
class ContextTimerAwaiter {
 public:
  ContextTimerAwaiter(Context& context, TimerKeeper& timer_keeper, std::chrono::milliseconds delay)
      : state_(std::make_shared<State>(context)), timer_keeper_(timer_keeper), delay_(delay) {}

  ~ContextTimerAwaiter() {
    if (timer_id_ != 0 && state_->armed.exchange(false, std::memory_order_acq_rel)) {
      timer_keeper_.CancelTimer(timer_id_);
    }
  }

  bool await_ready() const noexcept { return delay_.count() <= 0; }

  void await_suspend(std::coroutine_handle<> handle) {
    state_->handle = handle;
    std::weak_ptr<State> weak_state = state_;
    timer_id_ = timer_keeper_.AddTimer(delay_, [weak_state] {
      if (auto state = weak_state.lock()) {
        state->Fire();
      }
    });
  }

  void await_resume() noexcept {}

 private:
  struct State final : IoCommandMailbox, std::enable_shared_from_this<State> {
    explicit State(Context& context) : context(context) {}

    void Fire() {
      if (armed.exchange(false, std::memory_order_acq_rel)) {
        context.Notify(shared_from_this());
      }
    }

    void DrainCommandsOnIoThread() override {
      if (auto continuation = std::exchange(handle, nullptr)) {
        continuation.resume();
      }
    }

    Context& context;
    std::atomic<bool> armed {true};
    std::coroutine_handle<> handle {nullptr};  // Written before timer registration; read on Context only.
  };

  std::shared_ptr<State> state_;
  TimerKeeper& timer_keeper_;
  std::chrono::milliseconds delay_;
  uint64_t timer_id_ {0};
};

}  // namespace ant_rpc
