#pragma once

#include <chrono>
#include <coroutine>
#include <stdexcept>

#include "ant_rpc/scheduler/timer_keeper.hpp"
#include "ant_rpc/type.hpp"

namespace ant_rpc {

// Helper to resolve an active TimerKeeper from explicit pointer or global scheduler
inline TimerKeeper& GetEffectiveTimerKeeper(TimerKeeper* explicit_tk = nullptr) {
  if (explicit_tk) {
    return *explicit_tk;
  }
  if (g_scheduler) {
    return g_scheduler->GetTimerKeeper();
  }
  throw std::runtime_error("sleep_for requires an active TimerKeeper or running Scheduler");
}

// ============================================================================
// SleepAwaiter: 100% Zero-Allocation Coroutine Sleep Awaiter
// Decoupled from IO Context: registers timer with dedicated TimerKeeper (Timing Wheel).
// When the timer expires, the TimerKeeper automatically schedules the CoroTask
// onto the Worker Executor via lock-free P2C load balancing.
// ============================================================================
struct SleepAwaiter {
  TimerKeeper& timer_keeper_;
  std::chrono::milliseconds delay_;
  uint64_t timer_id_ {0};
  CoroTask task_;

  explicit SleepAwaiter(TimerKeeper& tk, std::chrono::milliseconds delay) : timer_keeper_(tk), delay_(delay) {}

  explicit SleepAwaiter(std::chrono::milliseconds delay) : timer_keeper_(GetEffectiveTimerKeeper()), delay_(delay) {}

  bool await_ready() const noexcept { return delay_.count() <= 0; }

  template <typename PromiseType>
  void await_suspend(std::coroutine_handle<PromiseType> h) {
    task_.init(h);
    // Zero-allocation: passes embedded CoroTask inside the coroutine frame to the Timing Wheel
    timer_id_ = timer_keeper_.AddTimer(delay_, &task_);
  }

  void await_resume() const noexcept {}
};

// ============================================================================
// Business-Level sleep_for coroutine helper functions
//
// Usage 1 (Standard / Inside Worker or Scheduler):
//   co_await sleep_for(50ms);
//   co_await sleep_for(std::chrono::milliseconds(50));
//
// Usage 2 (Explicit TimerKeeper):
//   co_await sleep_for(timer_keeper, 50ms);
// ============================================================================
template <typename Rep, typename Period>
inline auto sleep_for(TimerKeeper& tk, std::chrono::duration<Rep, Period> duration) {
  return SleepAwaiter {tk, std::chrono::duration_cast<std::chrono::milliseconds>(duration)};
}

template <typename Rep, typename Period>
inline auto sleep_for(std::chrono::duration<Rep, Period> duration) {
  return SleepAwaiter {std::chrono::duration_cast<std::chrono::milliseconds>(duration)};
}

}  // namespace ant_rpc

using ant_rpc::sleep_for;
using ant_rpc::SleepAwaiter;
