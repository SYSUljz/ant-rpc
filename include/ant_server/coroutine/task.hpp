#pragma once

#include <coroutine>
#include <exception>
#include <utility>

namespace ant_server {

template <typename T = void>
class Task {
 public:
  struct promise_type {
    std::coroutine_handle<> continuation {nullptr};
    T result;
    std::exception_ptr exception {nullptr};

    Task get_return_object() { return Task {std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
      bool await_ready() noexcept { return false; }
      std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
        if (h.promise().continuation) {
          return h.promise().continuation;
        }
        return std::noop_coroutine();
      }
      void await_resume() noexcept {}
    };

    FinalAwaiter final_suspend() noexcept { return {}; }

    template <typename Value>
    void return_value(Value&& val) {
      result = std::forward<Value>(val);
    }

    void unhandled_exception() { exception = std::current_exception(); }
  };

  std::coroutine_handle<promise_type> handle {nullptr};

  Task() = default;
  explicit Task(std::coroutine_handle<promise_type> h) : handle(h) {}
  ~Task() {
    if (handle) {
      handle.destroy();
    }
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  Task(Task&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle) {
        handle.destroy();
      }
      handle = other.handle;
      other.handle = nullptr;
    }
    return *this;
  }

  bool await_ready() const noexcept { return !handle || handle.done(); }

  std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
    handle.promise().continuation = caller;
    return handle;
  }

  T await_resume() {
    if (handle.promise().exception) {
      std::rethrow_exception(handle.promise().exception);
    }
    return std::move(handle.promise().result);
  }
};

template <>
class Task<void> {
 public:
  struct promise_type {
    std::coroutine_handle<> continuation {nullptr};
    std::exception_ptr exception {nullptr};

    Task get_return_object() { return Task {std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_always initial_suspend() noexcept { return {}; }

    struct FinalAwaiter {
      bool await_ready() noexcept { return false; }
      std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
        if (h.promise().continuation) {
          return h.promise().continuation;
        }
        return std::noop_coroutine();
      }
      void await_resume() noexcept {}
    };

    FinalAwaiter final_suspend() noexcept { return {}; }

    void return_void() noexcept {}

    void unhandled_exception() { exception = std::current_exception(); }
  };

  std::coroutine_handle<promise_type> handle {nullptr};

  Task() = default;
  explicit Task(std::coroutine_handle<promise_type> h) : handle(h) {}
  ~Task() {
    if (handle) {
      handle.destroy();
    }
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  Task(Task&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle) {
        handle.destroy();
      }
      handle = other.handle;
      other.handle = nullptr;
    }
    return *this;
  }

  bool await_ready() const noexcept { return !handle || handle.done(); }

  std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
    handle.promise().continuation = caller;
    return handle;
  }

  void await_resume() {
    if (handle.promise().exception) {
      std::rethrow_exception(handle.promise().exception);
    }
  }
};

}  // namespace ant_server

namespace ant_rpc {
using ant_server::Task;
}
