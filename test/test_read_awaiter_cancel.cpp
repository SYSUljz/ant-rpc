#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <stop_token>
#include <thread>

#include <gtest/gtest.h>
#include <sys/socket.h>

#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/type.hpp"

// Coroutine task type with initial_suspend = suspend_always to allow setting token into promise
struct TestCancelTask {
  struct promise_type {
    std::stop_token token;

    TestCancelTask get_return_object() {
      return TestCancelTask {std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { std::terminate(); }

    void set_stop_token(std::stop_token t) { token = std::move(t); }
    std::stop_token get_stop_token() const { return token; }
  };

  std::coroutine_handle<promise_type> handle {nullptr};
};

static TestCancelTask async_read_coro(Context& ctx, int fd, std::atomic<int>& out_res, std::atomic<bool>& finished) {
  char buf[1024];
  out_res.store(co_await ReadAwaiter(ctx, fd, buf, sizeof(buf), /*is_fixed=*/false), std::memory_order_release);
  finished.store(true, std::memory_order_release);
}

struct StartCoroutineCommand final : IoCommandMailbox {
  explicit StartCoroutineCommand(TestCancelTask& task) : task(task) {}
  void DrainCommandsOnIoThread() override { task.handle.resume(); }
  TestCancelTask& task;
};

struct RequestStopCommand final : IoCommandMailbox {
  explicit RequestStopCommand(std::stop_source& source) : source(source) {}
  void DrainCommandsOnIoThread() override { source.request_stop(); }
  std::stop_source& source;
};

static bool WaitUntil(const std::atomic<bool>& value) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!value.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return value.load(std::memory_order_acquire);
}

TEST(ReadAwaiterTest, ManualStopRequestTriggersECanceledAndSafelyExits) {
  Scheduler scheduler(1, 1);
  Context& ctx = scheduler.GetIOContext(0);
  scheduler.Start();
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

  std::atomic<int> read_result {0};
  std::atomic<bool> coro_finished {false};

  // 2. Create std::stop_source and coroutine instance
  std::stop_source stop_source;
  TestCancelTask task = async_read_coro(ctx, fds[0], read_result, coro_finished);

  // 3. Pass token to promise
  task.handle.promise().set_stop_token(stop_source.get_token());

  ctx.Notify(std::make_shared<StartCoroutineCommand>(task));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_FALSE(coro_finished.load());

  ctx.Notify(std::make_shared<RequestStopCommand>(stop_source));

  // 7. Verify ReadAwaiter returned -ECANCELED and coroutine finished safely
  EXPECT_TRUE(WaitUntil(coro_finished));
  EXPECT_EQ(read_result.load(), -ECANCELED);

  close(fds[0]);
  close(fds[1]);
  scheduler.Stop();
}

TEST(ReadAwaiterTest, PreRequestedStopTriggersECanceled) {
  Scheduler scheduler(1, 1);
  Context& ctx = scheduler.GetIOContext(0);
  scheduler.Start();
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

  std::atomic<int> read_result {0};
  std::atomic<bool> coro_finished {false};

  std::stop_source stop_source;
  stop_source.request_stop();

  TestCancelTask task = async_read_coro(ctx, fds[0], read_result, coro_finished);
  task.handle.promise().set_stop_token(stop_source.get_token());
  ctx.Notify(std::make_shared<StartCoroutineCommand>(task));

  EXPECT_TRUE(WaitUntil(coro_finished));
  EXPECT_EQ(read_result.load(), -ECANCELED);

  close(fds[0]);
  close(fds[1]);
  scheduler.Stop();
}
