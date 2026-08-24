#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <thread>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

#include "absl/synchronization/mutex.h"
#include "ant_server/constants.hpp"
#include "ant_server/scheduler/mpmc_queue.hpp"
#include "ant_server/type.hpp"
#include "ant_server/utils/random.hpp"

// ============================================================================
// WorkStealingExecutor: Pure Compute / Coroutine Task Executor (Worker Pool)
// Implements the Executor interface for decoupled, non-IO task scheduling.
// Each worker owns a mutex-protected ready deque. This intentionally favors a
// reviewable, testable concurrency boundary over a custom lock-free work-stealing
// deque. P2C is retained only as a load-balancing policy for external producers.
// ============================================================================
class WorkStealingExecutor : public Executor {
 public:
  struct WorkerState {
    int thread_id {0};
    alignas(kCacheLineSize) absl::Mutex ready_mu;
    std::deque<TaskNode*> ready_queue_;
    alignas(kCacheLineSize) uint64_t tick {0};
    alignas(kCacheLineSize) std::atomic<bool> is_parked {false};

    absl::Mutex park_mu;
    absl::CondVar park_cv;

    // This is the only shared task container for a worker. Producers append at
    // the back; the owner pops locally from the back while thieves take from
    // the front. The mutex makes TaskNode::next ownership irrelevant here.
    void push_ready(TaskNode* task) {
      if (!task) {
        return;
      }
      absl::MutexLock lock(&ready_mu);
      ready_queue_.push_back(task);
    }

    TaskNode* pop_local() {
      absl::MutexLock lock(&ready_mu);
      if (ready_queue_.empty()) {
        return nullptr;
      }
      TaskNode* task = ready_queue_.back();
      ready_queue_.pop_back();
      return task;
    }

    TaskNode* steal_one() {
      absl::MutexLock lock(&ready_mu);
      if (ready_queue_.empty()) {
        return nullptr;
      }
      TaskNode* task = ready_queue_.front();
      ready_queue_.pop_front();
      return task;
    }

    std::size_t approximate_load() {
      absl::MutexLock lock(&ready_mu);
      return ready_queue_.size();
    }

    bool has_ready_work() {
      absl::MutexLock lock(&ready_mu);
      return !ready_queue_.empty();
    }

    void unpark() {
      if (is_parked.load(std::memory_order_relaxed)) {
        absl::MutexLock lock(&park_mu);
        is_parked.store(false, std::memory_order_relaxed);
        park_cv.Signal();
      }
    }

    void park_and_wait(const std::atomic<bool>& running) {
      absl::MutexLock lock(&park_mu);
      if (!running.load(std::memory_order_relaxed)) {
        return;
      }
      is_parked.store(true, std::memory_order_relaxed);
      while (running.load(std::memory_order_relaxed) && is_parked.load(std::memory_order_relaxed) &&
             !has_ready_work()) {
        if (park_cv.WaitWithTimeout(&park_mu, absl::Microseconds(100))) {
          break;  // Timed out
        }
      }
      is_parked.store(false, std::memory_order_relaxed);
    }
  };

  explicit WorkStealingExecutor(std::size_t nthreads = std::max<std::size_t>(1, std::thread::hardware_concurrency()))
      : nthreads_(nthreads) {
    workers_.reserve(nthreads_);
    for (std::size_t i = 0; i < nthreads_; ++i) {
      auto w = std::make_unique<WorkerState>();
      w->thread_id = static_cast<int>(i);
      workers_.push_back(std::move(w));
    }
  }

  ~WorkStealingExecutor() override { Stop(); }

  // General task schedule (P2C load-balanced dispatch from external / IO / DB threads)
  void schedule(TaskNode* task) override {
    if (!task) {
      return;
    }

    // 1. If called from inside a worker thread, prioritize its own local slot/queue
    if (g_executor == this && g_thread_id < workers_.size()) {
      schedule(task, g_thread_id);
      return;
    }

    // 2. If called from external (IO thread, DB pool, or other executors), use P2C
    if (nthreads_ == 1) {
      workers_[0]->push_ready(task);
      workers_[0]->unpark();
      return;
    }

    std::size_t w1 = ant_server::fast_random() % nthreads_;
    std::size_t w2 = (w1 + 1 + (ant_server::fast_random() % (nthreads_ - 1))) % nthreads_;

    std::size_t load1 = workers_[w1]->approximate_load();
    std::size_t load2 = workers_[w2]->approximate_load();
    std::size_t target = (load1 <= load2) ? w1 : w2;

    workers_[target]->push_ready(task);
    workers_[target]->unpark();
  }

  // Targeted schedule with worker thread_id hint
  void schedule(TaskNode* task, std::size_t thread_id) override {
    if (!task) {
      return;
    }

    if (thread_id >= workers_.size()) {
      global_queue_.Push(task);
      wake_any_worker();
      return;
    }

    auto& w = *workers_[thread_id];

    // The owner and external producers use the same mutex-protected deque.
    if (g_executor == this && g_thread_id == thread_id) {
      w.push_ready(task);
    } else {
      // From another thread targeting this specific worker
      w.push_ready(task);
      w.unpark();
    }
  }

  void SetScheduler(Scheduler* scheduler) noexcept { scheduler_ = scheduler; }

  void WorkerLoop(int thread_id) {
    g_thread_id = static_cast<std::size_t>(thread_id);
    g_executor = this;
    g_scheduler = scheduler_;
    g_local_context = nullptr;  // Worker threads do NOT own io_uring context

    auto& w = *workers_[thread_id];
    int empty_spins = 0;

    while (running_.load(std::memory_order_relaxed)) {
      w.tick++;
      TaskNode* task = nullptr;

      // 1. Every 61 ticks, check global queue to prevent starvation
      if (w.tick % ant_server::constants::kGlobalCheckInterval == 0) {
        task = global_queue_.Pop();
      }

      // 2. Pop local work. Local execution is LIFO; thieves take FIFO below.
      if (!task) {
        task = w.pop_local();
      }

      // 3. Steal one FIFO task from another worker.
      if (!task) {
        task = steal_task(thread_id);
      }

      // 4. Check global queue again
      if (!task) {
        task = global_queue_.Pop();
      }

      // 5. Execute task or backoff & park
      if (task) {
        empty_spins = 0;
        task->run();
      } else {
        empty_spins++;
        if (empty_spins < 16) {
#if defined(__x86_64__) || defined(_M_X64)
          _mm_pause();
#elif defined(__aarch64__)
          asm volatile("yield" ::: "memory");
#else
          std::this_thread::yield();
#endif
        } else if (empty_spins < 32) {
          std::this_thread::yield();
        } else {
          w.park_and_wait(running_);
          empty_spins = 0;
        }
      }
    }
  }

  void Start() {
    running_.store(true, std::memory_order_release);
    threads_.reserve(nthreads_);
    for (std::size_t i = 0; i < nthreads_; ++i) {
      threads_.emplace_back([this, i]() { this->WorkerLoop(static_cast<int>(i)); });
    }
  }

  void Stop() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
      for (auto& w : workers_) {
        w->unpark();
      }
      for (auto& t : threads_) {
        if (t.joinable()) {
          t.join();
        }
      }
      threads_.clear();
    }
  }

  std::size_t NumWorkers() const noexcept { return nthreads_; }

 private:
  TaskNode* steal_task(int thief_id) {
    if (nthreads_ <= 1) {
      return nullptr;
    }

    std::size_t start = ant_server::fast_random() % nthreads_;
    for (std::size_t i = 0; i < nthreads_; ++i) {
      std::size_t victim_id = (start + i) % nthreads_;
      if (static_cast<int>(victim_id) == thief_id) {
        continue;
      }

      auto& victim = *workers_[victim_id];

      if (auto* task = victim.steal_one()) {
        return task;
      }
    }
    return nullptr;
  }

  void wake_any_worker() {
    for (auto& w : workers_) {
      if (w->is_parked.load(std::memory_order_relaxed)) {
        w->unpark();
        break;
      }
    }
  }

  std::vector<std::unique_ptr<WorkerState>> workers_;
  std::vector<std::thread> threads_;
  IntrusiveSpinLockQueue global_queue_;
  std::atomic<bool> running_ {false};
  std::size_t nthreads_;
  Scheduler* scheduler_ {nullptr};
};
