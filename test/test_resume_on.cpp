#include <atomic>
#include <chrono>
#include <coroutine>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/awaiter/resume_on.hpp"
#include "ant_server/scheduler/executor.hpp"
#include "ant_server/type.hpp"

// 1. Basic test: Hop from a non-worker std::thread to Worker Executor
TEST(ResumeOnAwaiterTest, BasicHopFromNonWorkerThread) {
  WorkStealingExecutor executor(3);
  executor.Start();

  std::atomic<bool> finished {false};
  std::atomic<bool> verified_on_executor {false};

  std::thread external_thread([&]() {
    [](Executor& exec, std::atomic<bool>& out_finished, std::atomic<bool>& out_verified) -> DetachedTask {
      // Before resume_on, we are in the external std::thread
      EXPECT_EQ(g_executor, nullptr);

      // Hop onto the executor
      co_await resume_on(exec);

      // Now we must be executing inside the worker executor!
      if (g_executor == &exec) {
        out_verified.store(true, std::memory_order_release);
      }
      out_finished.store(true, std::memory_order_release);
    }(executor, finished, verified_on_executor);
  });

  external_thread.join();

  auto start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  EXPECT_TRUE(finished.load());
  EXPECT_TRUE(verified_on_executor.load());

  executor.Stop();
}

// 2. Test explicit ResumeOnAwaiter struct constructor
TEST(ResumeOnAwaiterTest, DirectResumeOnAwaiterUsage) {
  WorkStealingExecutor executor(2);
  executor.Start();

  std::atomic<bool> finished {false};
  std::atomic<bool> verified {false};

  [](Executor& exec, std::atomic<bool>& out_finished, std::atomic<bool>& out_verified) -> DetachedTask {
    co_await ResumeOnAwaiter(exec);

    if (g_executor == &exec) {
      out_verified.store(true, std::memory_order_release);
    }
    out_finished.store(true, std::memory_order_release);
  }(executor, finished, verified);

  auto start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  EXPECT_TRUE(finished.load());
  EXPECT_TRUE(verified.load());

  executor.Stop();
}

// 3. Test multi-hop between two distinct Executors
TEST(ResumeOnAwaiterTest, MultiHopBetweenExecutors) {
  WorkStealingExecutor executor_a(2);
  WorkStealingExecutor executor_b(2);

  executor_a.Start();
  executor_b.Start();

  std::atomic<bool> finished {false};
  std::vector<std::string> hop_log;
  std::mutex log_mtx;

  [](Executor& exec_a, Executor& exec_b, std::atomic<bool>& out_finished, std::vector<std::string>& log,
     std::mutex& mtx) -> DetachedTask {
    // 1. Hop to Executor A
    co_await resume_on(exec_a);
    {
      std::lock_guard<std::mutex> lk(mtx);
      if (g_executor == &exec_a) {
        log.push_back("ExecA");
      }
    }

    // 2. Hop to Executor B
    co_await resume_on(exec_b);
    {
      std::lock_guard<std::mutex> lk(mtx);
      if (g_executor == &exec_b) {
        log.push_back("ExecB");
      }
    }

    // 3. Hop back to Executor A
    co_await resume_on(exec_a);
    {
      std::lock_guard<std::mutex> lk(mtx);
      if (g_executor == &exec_a) {
        log.push_back("ExecA_again");
      }
    }

    out_finished.store(true, std::memory_order_release);
  }(executor_a, executor_b, finished, hop_log, log_mtx);

  auto start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(3)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  EXPECT_TRUE(finished.load());
  ASSERT_EQ(hop_log.size(), 3);
  EXPECT_EQ(hop_log[0], "ExecA");
  EXPECT_EQ(hop_log[1], "ExecB");
  EXPECT_EQ(hop_log[2], "ExecA_again");

  executor_a.Stop();
  executor_b.Stop();
}

// 5. Stress test: Concurrent massive coroutine hopping from multiple background threads
TEST(ResumeOnAwaiterTest, ConcurrentMassiveCoroutineHopping) {
  constexpr size_t kNumWorkers = 4;
  constexpr size_t kBackgroundThreads = 8;
  constexpr size_t kCoroutinesPerThread = 500;
  constexpr size_t kTotalCoroutines = kBackgroundThreads * kCoroutinesPerThread;

  WorkStealingExecutor executor(kNumWorkers);
  executor.Start();

  std::atomic<size_t> completed_count {0};
  std::vector<std::thread> bg_threads;
  bg_threads.reserve(kBackgroundThreads);

  for (size_t t = 0; t < kBackgroundThreads; ++t) {
    bg_threads.emplace_back([&executor, &completed_count]() {
      for (size_t i = 0; i < kCoroutinesPerThread; ++i) {
        [](Executor& exec, std::atomic<size_t>& counter) -> DetachedTask {
          co_await resume_on(exec);
          counter.fetch_add(1, std::memory_order_relaxed);
        }(executor, completed_count);
      }
    });
  }

  for (auto& th : bg_threads) {
    if (th.joinable()) {
      th.join();
    }
  }

  auto start = std::chrono::steady_clock::now();
  while (completed_count.load(std::memory_order_relaxed) < kTotalCoroutines) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(5)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(completed_count.load(), kTotalCoroutines);

  executor.Stop();
}
