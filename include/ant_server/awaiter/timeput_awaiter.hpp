#ifndef ANT_TIMEOUT_AWAITER_HPP
#define ANT_TIMEOUT_AWAITER_HPP

#include <chrono>
#include <coroutine>
#include <memory>
#include <stop_token>
#include <utility>

#include "ant_server/context/context.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "ant_server/type.hpp"
#include "ant_server/utils/sleep.hpp"

// ============================================================================
// TimeoutAwaiter: Coroutine Timeout & Cancellation Primitive
// Uses dedicated TimerKeeper. Decoupled from IO Context.
// ============================================================================
template <typename CoroTaskFunc>
struct TimeoutAwaiter {
  TimeoutAwaiter(TimerKeeper& tk, std::chrono::milliseconds delay, CoroTaskFunc&& task_func)
      : timer_keeper_(tk), delay_(delay), task_func_(std::forward<CoroTaskFunc>(task_func)) {}

  TimeoutAwaiter(std::chrono::milliseconds delay, CoroTaskFunc&& task_func)
      : timer_keeper_(ant_server::GetEffectiveTimerKeeper()),
        delay_(delay),
        task_func_(std::forward<CoroTaskFunc>(task_func)) {}

  bool await_ready() { return false; }

  void await_suspend(std::coroutine_handle<> handle) {
    parent_handle_ = handle;
    source_ = std::make_shared<std::stop_source>();

    // Launch child coroutine task via lambda
    child_task_ = task_func_();
    if (child_task_.handle) {
      child_task_.handle.promise().set_stop_token(source_->get_token());
      child_task_.handle.promise().set_continuation(parent_handle_);
    } else {
      parent_handle_.resume();
      return;
    }

    timer_id_ = timer_keeper_.AddTimer(delay_, [source = source_]() {
      if (!source->stop_requested()) {
        source->request_stop();
      }
    });
  }

  void await_resume() {
    if (timer_id_ != 0) {
      timer_keeper_.CancelTimer(timer_id_);
    }
  }

 private:
  TimerKeeper& timer_keeper_;
  std::chrono::milliseconds delay_;
  uint64_t timer_id_ {0};
  CoroTaskFunc task_func_;
  HttpTask child_task_;
  std::shared_ptr<std::stop_source> source_;
  std::coroutine_handle<> parent_handle_;
};

template <typename CoroTaskFunc>
inline auto with_timeout(TimerKeeper& tk, std::chrono::milliseconds delay, CoroTaskFunc&& task_func) {
  return TimeoutAwaiter<CoroTaskFunc> {tk, delay, std::forward<CoroTaskFunc>(task_func)};
}

template <typename CoroTaskFunc>
inline auto with_timeout(std::chrono::milliseconds delay, CoroTaskFunc&& task_func) {
  return TimeoutAwaiter<CoroTaskFunc> {delay, std::forward<CoroTaskFunc>(task_func)};
}

template <typename CoroTaskFunc>
inline auto with_timeout(Context& ctx, std::chrono::milliseconds delay, CoroTaskFunc&& task_func) {
  return TimeoutAwaiter<CoroTaskFunc> {ctx.GetTimerKeeper(), delay, std::forward<CoroTaskFunc>(task_func)};
}

#endif
