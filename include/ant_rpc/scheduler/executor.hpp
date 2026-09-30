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
#include "ant_rpc/constants.hpp"
#include "ant_rpc/platform.hpp"
#include "ant_rpc/scheduler/mpmc_queue.hpp"
#include "ant_rpc/scheduler/spmc_queue.hpp"
#include "ant_rpc/type.hpp"
#include "ant_rpc/utils/random.hpp"
#include "concurrentqueue/concurrentqueue.h"

// ============================================================================
// WorkStealingExecutor: Pure Compute / Coroutine Task Executor (Worker Pool)
// Implements the Executor interface for decoupled, non-IO task scheduling.
// Each worker owns a lock-free Chase-Lev SPMC local queue for local tasks,
// and a lock-free MPSC inbound queue for external/IO producers.
class WorkStealingExecutor : public Executor {
 public:
  struct WorkerState;
  static inline thread_local WorkerState* g_current_worker = nullptr;

  struct WorkerState {
    int thread_id {0};
    alignas(kCacheLineSize) SPMCQueue<TaskNode*, 1024> local_queue_;
    alignas(kCacheLineSize) moodycamel::ConcurrentQueue<TaskNode*> inbound_queue_;
    alignas(kCacheLineSize) uint64_t tick {0};
    alignas(kCacheLineSize) std::atomic<bool> is_parked {false};

    absl::Mutex park_mu;
    absl::CondVar park_cv;

    void push_ready(TaskNode* task) {
      if (!task) {
        return;
      }
      if (g_current_worker == this) {
        if (local_queue_.Push(task)) {
          return;
        }
      }
      inbound_queue_.enqueue(task);
    }

    TaskNode* pop_local() {
      if (auto* task = local_queue_.TryPop()) {
        return task;
      }
      TaskNode* task = nullptr;
      if (inbound_queue_.try_dequeue(task)) {
        return task;
      }
      return nullptr;
    }

    TaskNode* steal_one() {
      if (auto* task = local_queue_.Steal()) {
        return task;
      }
      TaskNode* task = nullptr;
      if (inbound_queue_.try_dequeue(task)) {
        return task;
      }
      return nullptr;
    }

    std::size_t approximate_load() const noexcept {
      return static_cast<std::size_t>(local_queue_.Size()) + inbound_queue_.size_approx();
    }

    bool has_ready_work() const noexcept {
      return !local_queue_.Empty() || inbound_queue_.size_approx() > 0;
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

    // 1. If called from inside a worker thread of this executor, prioritize its own local queue
    if (g_current_worker != nullptr && g_executor == this) {
      g_current_worker->push_ready(task);
      g_current_worker->unpark();
      return;
    }

    // 2. If called from external (IO thread, DB pool, or other executors), use P2C
    if (nthreads_ == 1) {
      workers_[0]->push_ready(task);
      workers_[0]->unpark();
      return;
    }

    std::size_t w1 = ant_rpc::fast_random() % nthreads_;
    std::size_t w2 = (w1 + 1 + (ant_rpc::fast_random() % (nthreads_ - 1))) % nthreads_;

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
    w.push_ready(task);
    w.unpark();
  }

  void SetScheduler(Scheduler* scheduler) noexcept { scheduler_ = scheduler; }

  void WorkerLoop(int thread_id) {
    ant_rpc::platform::SetIndexedThreadName("ant-wrk", static_cast<std::size_t>(thread_id));
    g_thread_id = static_cast<std::size_t>(thread_id);
    g_executor = this;
    g_scheduler = scheduler_;
    g_local_context = nullptr;  // Worker threads do NOT own io_uring context

    auto& w = *workers_[thread_id];
    g_current_worker = &w;
    int empty_spins = 0;

    while (running_.load(std::memory_order_relaxed)) {
      w.tick++;
      TaskNode* task = nullptr;

      // 1. Every 61 ticks, check global queue to prevent starvation
      if (w.tick % ant_rpc::constants::kGlobalCheckInterval == 0) {
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

    std::size_t start = ant_rpc::fast_random() % nthreads_;
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
  ConcurrentGlobalQueue global_queue_;
  std::atomic<bool> running_ {false};
  std::size_t nthreads_;
  Scheduler* scheduler_ {nullptr};
};
