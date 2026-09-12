#ifndef ANT_TYPE
#define ANT_TYPE

#include <coroutine>
#include <exception>
#include <new>
#include <stop_token>

#include "ant_rpc/constants.hpp"

enum EventType { EVENT_ACCEPT, EVENT_READ, EVENT_WRITE, EVENT_CLOSE, EVENT_TIMER };

struct IOHandler {
  virtual void on_complete() = 0;
  virtual ~IOHandler() = default;
  void prepare_complete(int res, uint32_t flags) {
    res_ = res;
    flags_ = flags;
  }

  int res_;
  uint32_t flags_;
};

struct Context;
struct Scheduler;
struct TaskNode;

struct Executor {
  virtual ~Executor() = default;
  virtual void schedule(TaskNode* task) = 0;
  virtual void schedule(TaskNode* task, std::size_t thread_id) { schedule(task); }
};

class Scheduler;
class TimerKeeper;

inline thread_local Executor* g_executor = nullptr;
inline thread_local Scheduler* g_scheduler = nullptr;
inline thread_local std::size_t g_thread_id {0};

inline thread_local Context* g_local_context = nullptr;
inline Context& GetCurrentContext() { return *g_local_context; }
enum class CancelState { pending, canceled };
struct DetachedTask {
  struct promise_type {
    DetachedTask get_return_object() { return {}; }
    std::suspend_never initial_suspend() { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { std::terminate(); }
  };
};
struct HttpTask {
  struct promise_type {
    std::stop_token token;
    std::coroutine_handle<> continuation {nullptr};
    void set_continuation(std::coroutine_handle<> h) { continuation = h; }

    struct FinalAwaiter {
      bool await_ready() noexcept { return false; }
      template <typename Promise>
      std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
        if (auto parent = h.promise().continuation) {
          return parent;
        }
        return std::noop_coroutine();
      }
      void await_resume() noexcept {};
    };

    std::suspend_never initial_suspend() { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
    HttpTask get_return_object() { return HttpTask {std::coroutine_handle<promise_type>::from_promise(*this)}; }

    void return_void() {}
    void unhandled_exception() { std::terminate(); }

    void set_stop_token(std::stop_token t) { token = std::move(t); }

    std::stop_token get_stop_token() const { return token; }
  };
  std::coroutine_handle<promise_type> handle {nullptr};
};
struct alignas(kCacheLineSize) TaskNode {
  TaskNode* next {nullptr};
  void (*execute)(TaskNode* self) noexcept;

  void run() {
    if (execute) {
      execute(this);
    }
  }
};
struct CoroTask : public TaskNode {
  std::coroutine_handle<> handle {nullptr};

  CoroTask() noexcept {
    execute = [](TaskNode* self) noexcept {
      auto* coro_node = static_cast<CoroTask*>(self);
      if (coro_node->handle) {
        coro_node->handle.resume();
      }
    };
  }

  explicit CoroTask(std::coroutine_handle<> h) noexcept : CoroTask() { handle = h; }

  void init(std::coroutine_handle<> h) noexcept { handle = h; }
};

template <typename F>
struct LambdaTask : public TaskNode {
  F func;

  explicit LambdaTask(F&& f) : func(std::forward<F>(f)) {
    execute = [](TaskNode* self) noexcept {
      auto* lambda_node = static_cast<LambdaTask<F>*>(self);
      lambda_node->func();
    };
  }
};

template <typename F>
auto make_lambda_task(F&& f) {
  return LambdaTask<std::decay_t<F>> {std::forward<F>(f)};
}
struct TypeErasedTask : public TaskNode {
  static constexpr size_t kSboSize = ant_rpc::constants::kTaskSboSize;

  alignas(8) char sbo_buffer[kSboSize];
  void (*destroy_fn)(TaskNode* self) noexcept {nullptr};
  bool auto_delete {false};

  template <typename F>
  explicit TypeErasedTask(F&& f, bool auto_delete = false) : auto_delete(auto_delete) {
    using DecayedF = std::decay_t<F>;
    static_assert(sizeof(DecayedF) <= kSboSize, "Closure too large for SBO!");

    new (sbo_buffer) DecayedF(std::forward<F>(f));

    execute = [](TaskNode* self) noexcept {
      auto* node = static_cast<TypeErasedTask*>(self);
      auto* func = reinterpret_cast<DecayedF*>(node->sbo_buffer);
      (*func)();
      if (node->auto_delete) {
        delete node;
      }
    };

    destroy_fn = [](TaskNode* self) noexcept {
      auto* node = static_cast<TypeErasedTask*>(self);
      auto* func = reinterpret_cast<DecayedF*>(node->sbo_buffer);
      func->~DecayedF();
    };
  }

  ~TypeErasedTask() {
    if (destroy_fn) {
      destroy_fn(this);
    }
  }
};

// for cpp26 p2300 sender/receiver
template <typename OpState>
struct SenderTask : public TaskNode {
  OpState* op_state {nullptr};

  explicit SenderTask(OpState* op) : op_state(op) {
    execute = [](TaskNode* self) noexcept {
      auto* node = static_cast<SenderTask<OpState>*>(self);
      if (node->op_state) {
        // std::execution::set_value(std::move(*node->op_state));
      }
    };
  }
};

#endif
// Todo: use Clang to check Google C++ Style Guide
