#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <variant>

#include "ant_rpc/coroutine/task.hpp"
#include "ant_rpc/type.hpp"

namespace ant_rpc {

// Result of racing two lazy Tasks. The index identifies the task that first
// completed; the corresponding value is held in values.
template <typename First, typename Second>
struct WhenAnyResult {
  std::size_t index {0};
  std::variant<First, Second> value;
};

template <typename First, typename Second>
class WhenAnyAwaiter {
 public:
  WhenAnyAwaiter(Task<First> first, Task<Second> second)
      : state_(std::make_shared<State>(std::move(first), std::move(second))) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> parent) {
    state_->parent = parent;
    state_->starting.store(true, std::memory_order_relaxed);
    RunFirst(state_);
    RunSecond(state_);
    state_->starting.store(false, std::memory_order_release);
    // A lazy Task can complete synchronously. Returning false avoids resuming
    // parent from inside await_suspend, which would otherwise be re-entrant.
    return state_->winner.load(std::memory_order_acquire) == -1;
  }

  WhenAnyResult<First, Second> await_resume() {
    if (state_->winner.load(std::memory_order_acquire) == 0) {
      if (state_->first_exception) {
        std::rethrow_exception(state_->first_exception);
      }
      return {0, std::variant<First, Second> {std::in_place_index<0>, std::move(*state_->first_result)}};
    }
    if (state_->second_exception) {
      std::rethrow_exception(state_->second_exception);
    }
    return {1, std::variant<First, Second> {std::in_place_index<1>, std::move(*state_->second_result)}};
  }

 private:
  struct State {
    State(Task<First> first_task, Task<Second> second_task)
        : first(std::move(first_task)), second(std::move(second_task)) {}

    Task<First> first;
    Task<Second> second;
    std::optional<First> first_result;
    std::optional<Second> second_result;
    std::exception_ptr first_exception;
    std::exception_ptr second_exception;
    std::atomic<int> winner {-1};
    std::atomic<bool> starting {false};
    std::coroutine_handle<> parent {nullptr};
  };

  static void Complete(const std::shared_ptr<State>& state, int index) {
    int expected = -1;
    if (state->winner.compare_exchange_strong(expected, index, std::memory_order_release, std::memory_order_relaxed) &&
        !state->starting.load(std::memory_order_acquire)) {
      state->parent.resume();
    }
  }

  static DetachedTask RunFirst(std::shared_ptr<State> state) {
    try {
      state->first_result.emplace(co_await std::move(state->first));
    } catch (...) {
      state->first_exception = std::current_exception();
    }
    Complete(state, 0);
  }

  static DetachedTask RunSecond(std::shared_ptr<State> state) {
    try {
      state->second_result.emplace(co_await std::move(state->second));
    } catch (...) {
      state->second_exception = std::current_exception();
    }
    Complete(state, 1);
  }

  std::shared_ptr<State> state_;
};

template <typename First, typename Second>
auto when_any(Task<First> first, Task<Second> second) {
  return WhenAnyAwaiter<First, Second> {std::move(first), std::move(second)};
}

}  // namespace ant_rpc
