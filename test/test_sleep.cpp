#include <atomic>
#include <chrono>
#include <coroutine>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "ant_server/type.hpp"
#include "ant_server/utils/sleep.hpp"

using namespace std::chrono_literals;

// 1. Basic test: sleep_for with explicit TimerKeeper
TEST(SleepTest, BasicSleepWithTimerKeeper) {
  WorkStealingExecutor executor(2);
  executor.Start();

  TimerKeeper timer_keeper(executor);
  timer_keeper.Start();

  std::atomic<bool> finished {false};
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;

  [](TimerKeeper& tk, std::atomic<bool>& out_finished, std::chrono::steady_clock::time_point& start,
     std::chrono::steady_clock::time_point& end) -> DetachedTask {
    start = std::chrono::steady_clock::now();
    co_await sleep_for(tk, 50ms);
    end = std::chrono::steady_clock::now();
    out_finished.store(true, std::memory_order_release);
  }(timer_keeper, finished, start_time, end_time);

  auto wait_start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - wait_start > 3s) {
      break;
    }
    std::this_thread::sleep_for(5ms);
  }

  EXPECT_TRUE(finished.load());
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  EXPECT_GE(elapsed_ms, 40);  // Allow small margin

  timer_keeper.Stop();
  executor.Stop();
}

// 2. Zero-duration sleep should return immediately without suspending
TEST(SleepTest, ZeroDurationReturnsImmediately) {
  WorkStealingExecutor executor(1);
  TimerKeeper timer_keeper(executor);
  bool finished = false;

  [](TimerKeeper& tk, bool& out_finished) -> DetachedTask {
    co_await sleep_for(tk, 0ms);
    out_finished = true;
  }(timer_keeper, finished);

  EXPECT_TRUE(finished);
}

// 3. Test sleep_for inside Scheduler Worker without passing Context or TimerKeeper
TEST(SleepTest, SleepInsideSchedulerWorkerResumesOnExecutor) {
  Scheduler scheduler(/*n_workers=*/3, /*n_io_threads=*/1);
  std::atomic<bool> finished {false};
  std::atomic<bool> resumed_on_worker {false};

  scheduler.Start();

  std::this_thread::sleep_for(50ms);

  // Schedule a coroutine task onto the Scheduler Worker
  auto task = std::make_unique<LambdaTask<std::function<void()>>>([&scheduler, &finished, &resumed_on_worker]() {
    [](Scheduler& s, std::atomic<bool>& out_finished, std::atomic<bool>& out_resumed) -> DetachedTask {
      // 100% zero context needed: automatic lookup of TimerKeeper on Worker thread via Scheduler
      co_await sleep_for(50ms);

      // Verify that after sleep, we resumed on a Worker thread inside the Executor!
      if (g_executor == &s.GetExecutor()) {
        out_resumed.store(true, std::memory_order_release);
      }
      out_finished.store(true, std::memory_order_release);
    }(scheduler, finished, resumed_on_worker);
  });

  scheduler.schedule(task.get());

  auto wait_start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - wait_start > 3s) {
      break;
    }
    std::this_thread::sleep_for(5ms);
  }

  EXPECT_TRUE(finished.load());
  EXPECT_TRUE(resumed_on_worker.load());

  scheduler.Stop();
}

// 4. Concurrent sleep test with multiple staggered timers
TEST(SleepTest, ConcurrentStaggeredSleep) {
  Scheduler scheduler(/*n_workers=*/4, /*n_io_threads=*/1);
  constexpr size_t kNumCoroutines = 20;
  std::atomic<size_t> completed_count {0};

  scheduler.Start();

  std::this_thread::sleep_for(50ms);

  std::vector<std::unique_ptr<TaskNode>> tasks;
  tasks.reserve(kNumCoroutines);

  for (size_t i = 0; i < kNumCoroutines; ++i) {
    auto task = std::make_unique<LambdaTask<std::function<void()>>>([&completed_count, i]() {
      [](size_t index, std::atomic<size_t>& counter) -> DetachedTask {
        auto delay = std::chrono::milliseconds(20 + (index % 4) * 10);
        co_await sleep_for(delay);
        counter.fetch_add(1, std::memory_order_relaxed);
      }(i, completed_count);
    });
    scheduler.schedule(task.get());
    tasks.push_back(std::move(task));
  }

  auto wait_start = std::chrono::steady_clock::now();
  while (completed_count.load(std::memory_order_relaxed) < kNumCoroutines) {
    if (std::chrono::steady_clock::now() - wait_start > 4s) {
      break;
    }
    std::this_thread::sleep_for(10ms);
  }

  EXPECT_EQ(completed_count.load(), kNumCoroutines);

  scheduler.Stop();
}
