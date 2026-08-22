#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "ant_server/constants.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/scheduler/executor.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "ant_server/type.hpp"

// ============================================================================
// Scheduler: Orchestrates Dedicated IO Reactors (io_uring), Worker Executor,
// and Background TimerKeeper (Intrusive Timing Wheel).
// Strict Mode A: 100% Separation of Concerns.
// ============================================================================
class Scheduler : public Executor {
 public:
  explicit Scheduler(std::size_t n_workers, std::size_t n_io_threads = 2,
                     std::size_t uring_size = ant_server::constants::kDefaultServerUringSize)
      : n_workers_(n_workers), n_io_threads_(n_io_threads), uring_size_(uring_size) {
    worker_executor_ = std::make_unique<WorkStealingExecutor>(n_workers_);
    timer_keeper_ = std::make_unique<TimerKeeper>(*worker_executor_);

    io_contexts_.reserve(n_io_threads_);
    for (std::size_t i = 0; i < n_io_threads_; ++i) {
      auto ctx = std::make_unique<Context>(uring_size_, this, worker_executor_.get());
      io_contexts_.push_back(std::move(ctx));
    }

    worker_executor_->SetScheduler(this);
  }

  // Smart default constructor matching machine topology
  Scheduler()
      : Scheduler(std::thread::hardware_concurrency() > 2 ? std::thread::hardware_concurrency() - 2 : 1,
                  std::thread::hardware_concurrency() > 2 ? 2 : 1) {}

  ~Scheduler() override { Stop(); }

  // Executor interface delegation
  void schedule(TaskNode* task) override { worker_executor_->schedule(task); }
  void schedule(TaskNode* task, std::size_t thread_id) override { worker_executor_->schedule(task, thread_id); }
  void schedule(TaskNode* task, int thread_id) { schedule(task, static_cast<std::size_t>(thread_id)); }

  // Accessors
  Executor& GetExecutor() noexcept { return *worker_executor_; }
  WorkStealingExecutor& GetWorkerExecutor() noexcept { return *worker_executor_; }
  TimerKeeper& GetTimerKeeper() noexcept { return *timer_keeper_; }

  Context& GetIOContext(std::size_t index) {
    if (index >= io_contexts_.size()) {
      return *io_contexts_[0];
    }
    return *io_contexts_[index];
  }

  // Round-robin assignment for callers that do not provide a load metric.
  Context& GetNextIOContext() {
    std::size_t idx = next_io_idx_.fetch_add(1, std::memory_order_relaxed) % io_contexts_.size();
    return *io_contexts_[idx];
  }

  // P2C avoids a contended global "least loaded" counter.  It is used only
  // when assigning new connections/channels; existing sockets stay pinned to
  // their original Context.
  Context& GetBalancedIOContext() {
    const std::size_t first = next_io_idx_.fetch_add(1, std::memory_order_relaxed) % io_contexts_.size();
    const std::size_t second = next_io_idx_.fetch_add(1, std::memory_order_relaxed) % io_contexts_.size();
    return io_contexts_[first]->PendingCommandCount() <= io_contexts_[second]->PendingCommandCount()
               ? *io_contexts_[first]
               : *io_contexts_[second];
  }

  // Worker-facing IO wakeup API. The mailbox owns concrete command types;
  // Scheduler deliberately exposes no SQE/ring access.
  void NotifyIO(std::size_t io_index, std::shared_ptr<IoCommandMailbox> mailbox) {
    GetIOContext(io_index).Notify(std::move(mailbox));
  }

  std::size_t NumWorkers() const noexcept { return n_workers_; }
  std::size_t NumIOThreads() const noexcept { return n_io_threads_; }

  void IOLoop(int io_id) {
    g_thread_id = static_cast<std::size_t>(io_id);
    g_local_context = io_contexts_[io_id].get();
    g_executor = worker_executor_.get();
    g_scheduler = this;

    auto& ctx = *io_contexts_[io_id];

    while (running_.load(std::memory_order_relaxed)) {
      int ret = ctx.ProcessEvents(1);
      if (ret < 0) {
        if (ret == -EINTR) {
          continue;
        }
        break;
      }
    }
  }

  void Start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }

    // 1. Start dedicated background timing wheel keeper
    timer_keeper_->Start();

    // 2. Start pure worker threads
    worker_executor_->Start();

    // 3. Start dedicated IO reactor threads
    io_threads_.reserve(n_io_threads_);
    for (std::size_t i = 0; i < n_io_threads_; ++i) {
      io_threads_.emplace_back([this, i]() { this->IOLoop(static_cast<int>(i)); });
    }
  }

  void Wait() {
    for (auto& t : io_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  void Stop() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
      // 1. Stop all IO contexts (wake them up from io_uring_submit_and_wait)
      for (auto& ctx : io_contexts_) {
        ctx->Stop();
      }

      // 2. Join and clean up all IO threads
      for (auto& t : io_threads_) {
        if (t.joinable()) {
          t.join();
        }
      }
      io_threads_.clear();

      // 3. Stop background timing wheel keeper
      timer_keeper_->Stop();

      // 4. Stop worker executor
      worker_executor_->Stop();
    }
  }

 private:
  std::unique_ptr<WorkStealingExecutor> worker_executor_;
  std::unique_ptr<TimerKeeper> timer_keeper_;
  std::vector<std::unique_ptr<Context>> io_contexts_;
  std::vector<std::thread> io_threads_;
  std::atomic<bool> running_ {false};
  std::atomic<std::size_t> next_io_idx_ {0};
  std::size_t n_workers_;
  std::size_t n_io_threads_;
  std::size_t uring_size_;
};
