#include <atomic>
#include <chrono>
#include <coroutine>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_server/awaiter/resume_on.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/type.hpp"

// 1. Test basic WorkStealingExecutor multi-threaded scheduling
TEST(ExecutorTest, WorkStealingExecutorExecutesTasksConcurrently) {
  constexpr size_t kNumWorkers = 4;
  constexpr size_t kNumTasks = 1000;

  WorkStealingExecutor executor(kNumWorkers);
  executor.Start();

  std::atomic<size_t> completed_tasks {0};
  std::vector<std::unique_ptr<LambdaTask<std::function<void()>>>> tasks;
  tasks.reserve(kNumTasks);

  for (size_t i = 0; i < kNumTasks; ++i) {
    auto task = std::make_unique<LambdaTask<std::function<void()>>>(
        [&completed_tasks]() { completed_tasks.fetch_add(1, std::memory_order_relaxed); });
    executor.schedule(task.get());
    tasks.push_back(std::move(task));
  }

  // Wait for completion
  auto start = std::chrono::steady_clock::now();
  while (completed_tasks.load(std::memory_order_relaxed) < kNumTasks) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(3)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(completed_tasks.load(), kNumTasks);
  executor.Stop();
}

// 2. Test simulating external async DB thread hopping back via resume_on
struct AsyncDbSimulation {
  static DetachedTask SimulateDbWorkflow(Executor& executor, std::atomic<bool>& out_finished,
                                         std::atomic<bool>& out_resumed_on_worker) {
    // Phase 1: In initial thread
    // Simulate spawning an external DB thread that will complete later
    std::thread db_thread([&executor, &out_finished, &out_resumed_on_worker]() {
      // Simulate DB query delay
      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      // Hop back onto the worker executor
      [](Executor& exec, std::atomic<bool>& finished, std::atomic<bool>& resumed_on_worker) -> DetachedTask {
        co_await resume_on(exec);
        // Verify we are running inside the executor
        if (g_executor == &exec) {
          resumed_on_worker.store(true, std::memory_order_release);
        }
        finished.store(true, std::memory_order_release);
      }(executor, out_finished, out_resumed_on_worker);
    });

    db_thread.detach();
    co_return;
  }
};

TEST(ExecutorTest, ResumeOnSwitchesFromExternalDbThreadToWorkerExecutor) {
  WorkStealingExecutor executor(2);
  executor.Start();

  std::atomic<bool> finished {false};
  std::atomic<bool> resumed_on_worker {false};

  AsyncDbSimulation::SimulateDbWorkflow(executor, finished, resumed_on_worker);

  auto start = std::chrono::steady_clock::now();
  while (!finished.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(3)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(finished.load());
  EXPECT_TRUE(resumed_on_worker.load());

  executor.Stop();
}

// 3. Test Scheduler with separated IO and Worker threads
TEST(SchedulerTest, SeparatedIOAndWorkerLifecycle) {
  Scheduler scheduler(/*n_workers=*/3, /*n_io_threads=*/1);

  EXPECT_EQ(scheduler.NumWorkers(), 3);
  EXPECT_EQ(scheduler.NumIOThreads(), 1);

  std::atomic<int> worker_executed {0};

  // Launch scheduler non-blockingly
  scheduler.Start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Schedule task via Scheduler's Executor interface
  auto task = make_lambda_task([&worker_executed]() { worker_executed.store(42, std::memory_order_release); });

  scheduler.schedule(&task);

  auto start = std::chrono::steady_clock::now();
  while (worker_executed.load(std::memory_order_acquire) != 42) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(worker_executed.load(), 42);

  scheduler.Stop();
}
