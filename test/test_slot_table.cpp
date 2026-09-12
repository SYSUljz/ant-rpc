#include <array>
#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "ant_rpc/rpc/controller.hpp"
#include "ant_rpc/rpc/slot_table.hpp"

namespace {

// This translation unit exercises the slot state machine exclusively. Keep
// test bodies focused on state transitions rather than repeating the library
// namespace on every expectation.
using namespace ant_rpc::rpc;

struct CountingTask final : TaskNode {
  explicit CountingTask(std::atomic<int>& count) : count_(count) {
    execute = [](TaskNode* self) noexcept { static_cast<CountingTask*>(self)->count_.fetch_add(1); };
  }

  std::atomic<int>& count_;
};

struct CountingClosure final : google::protobuf::Closure {
  explicit CountingClosure(std::atomic<int>& count) : count_(count) {}
  void Run() override { count_.fetch_add(1, std::memory_order_relaxed); }

  std::atomic<int>& count_;
};

struct OrderedTask final : TaskNode {
  OrderedTask(std::atomic<int>& order, std::atomic<int>& observed_order)
      : order_(order), observed_order_(observed_order) {
    execute = [](TaskNode* self) noexcept {
      auto* task = static_cast<OrderedTask*>(self);
      task->observed_order_.store(task->order_.fetch_add(1, std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    };
  }

  std::atomic<int>& order_;
  std::atomic<int>& observed_order_;
};

struct OrderedClosure final : google::protobuf::Closure {
  OrderedClosure(std::atomic<int>& order, std::atomic<int>& observed_order)
      : order_(order), observed_order_(observed_order) {}
  void Run() override {
    observed_order_.store(order_.fetch_add(1, std::memory_order_relaxed) + 1, std::memory_order_relaxed);
  }

  std::atomic<int>& order_;
  std::atomic<int>& observed_order_;
};

struct RecordingExecutor final : Executor {
  void schedule(TaskNode* task) override {
    schedules.fetch_add(1, std::memory_order_relaxed);
    task->run();
  }

  std::atomic<int> schedules {0};
};

ContinuationTarget TestContinuationTarget() {
  static RecordingExecutor executor;
  return ContinuationTarget::Worker(executor);
}

TEST(SlotTableTest, CancelDuringArmingIsDeliveredByPublish) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_EQ(ran.load(), 0) << "ARMING must not run the continuation";
  EXPECT_FALSE(controller.Failed());

  EXPECT_EQ(slots.PublishSlot(cid), PublishOutcome::kCanceled);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
}

TEST(SlotTableTest, NotifyOnCancelRunsOnceBeforeNormalCompletionContinuation) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> order {0};
  std::atomic<int> task_order {0};
  std::atomic<int> notify_order {0};
  OrderedTask task(order, task_order);
  OrderedClosure notify(order, notify_order);
  butil::IOBuf body;

  controller.NotifyOnCancel(&notify);
  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CompleteSlot(cid, body));

  EXPECT_EQ(notify_order.load(), 1);
  EXPECT_EQ(task_order.load(), 2);
  controller.StartCancel();
  EXPECT_EQ(notify_order.load(), 1);
}

TEST(SlotTableTest, TimeoutDuringArmingIsDeliveredByPublish) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.TimeoutSlot(cid));
  EXPECT_EQ(ran.load(), 0) << "ARMING must not run the continuation";

  EXPECT_EQ(slots.PublishSlot(cid), PublishOutcome::kTimedOut);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ETIMEOUT);
}

TEST(SlotTableTest, FirstPendingResolutionWins) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_FALSE(slots.TimeoutSlot(cid));
  EXPECT_EQ(slots.PublishSlot(cid), PublishOutcome::kCanceled);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
}

TEST(SlotTableTest, PublishedSlotResolvesImmediately) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
}

TEST(SlotTableTest, PublishedSlotTimesOutImmediately) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);

  EXPECT_TRUE(slots.TimeoutSlot(cid));
  EXPECT_EQ(ran.load(), 1);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ETIMEOUT);
}

TEST(SlotTableTest, ResponseCannotCompleteAnArmingSlot) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  butil::IOBuf body;

  EXPECT_FALSE(slots.CompleteSlot(cid, body)) << "a response may only resolve a published slot";
  EXPECT_EQ(ran.load(), 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  EXPECT_TRUE(slots.CompleteSlot(cid, body));
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, LateResponseAfterCancellationIsIgnored) {
  SlotTable<1> slots;
  RpcController first_controller;
  RpcController second_controller;
  std::atomic<int> first_ran {0};
  std::atomic<int> second_ran {0};
  CountingTask first_task(first_ran);
  CountingTask second_task(second_ran);
  butil::IOBuf body;

  const uint64_t first_cid = slots.AllocateSlot(&first_task, nullptr, &first_controller, TestContinuationTarget());
  ASSERT_NE(first_cid, 0);
  ASSERT_EQ(slots.PublishSlot(first_cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CancelSlot(first_cid));
  ASSERT_EQ(first_ran.load(), 1);

  const uint64_t second_cid = slots.AllocateSlot(&second_task, nullptr, &second_controller, TestContinuationTarget());
  ASSERT_NE(second_cid, 0);
  ASSERT_NE(second_cid, first_cid) << "slot reuse must advance the correlation-id version";
  ASSERT_EQ(slots.PublishSlot(second_cid), PublishOutcome::kInFlight);

  EXPECT_FALSE(slots.CompleteSlot(first_cid, body)) << "a late response must not resolve a recycled slot";
  EXPECT_EQ(second_ran.load(), 0);
  EXPECT_TRUE(slots.CompleteSlot(second_cid, body));
  EXPECT_EQ(second_ran.load(), 1);

  const auto stats = slots.response_completion_stats();
  EXPECT_EQ(stats.completed, 1U);
  EXPECT_EQ(stats.late_response, 1U);
  EXPECT_EQ(stats.unknown_correlation_id, 0U);
}

TEST(SlotTableTest, LateTimeoutAfterSlotReuseCannotResolveNewCall) {
  SlotTable<1> slots;
  RpcController first_controller;
  RpcController second_controller;
  std::atomic<int> first_ran {0};
  std::atomic<int> second_ran {0};
  CountingTask first_task(first_ran);
  CountingTask second_task(second_ran);
  butil::IOBuf body;

  const uint64_t first_cid = slots.AllocateSlot(&first_task, nullptr, &first_controller, TestContinuationTarget());
  ASSERT_NE(first_cid, 0);
  ASSERT_EQ(slots.PublishSlot(first_cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.TimeoutSlot(first_cid));

  const uint64_t second_cid = slots.AllocateSlot(&second_task, nullptr, &second_controller, TestContinuationTarget());
  ASSERT_NE(second_cid, 0);
  ASSERT_NE(first_cid, second_cid);
  ASSERT_EQ(slots.PublishSlot(second_cid), PublishOutcome::kInFlight);

  EXPECT_FALSE(slots.TimeoutSlot(first_cid));
  EXPECT_TRUE(slots.CompleteSlot(second_cid, body));
  EXPECT_FALSE(second_controller.Failed());
  EXPECT_EQ(first_ran.load(), 1);
  EXPECT_EQ(second_ran.load(), 1);
}

TEST(SlotTableTest, UnknownCorrelationIdIsCountedAndIgnored) {
  SlotTable<4> slots;
  butil::IOBuf body;

  EXPECT_FALSE(slots.CompleteSlot(SlotTable<4>::MakeCorrelationId(1, 99), body));
  const auto stats = slots.response_completion_stats();
  EXPECT_EQ(stats.completed, 0U);
  EXPECT_EQ(stats.unknown_correlation_id, 1U);
  EXPECT_EQ(stats.late_response, 0U);
}

TEST(SlotTableTest, CompletionIsAlwaysScheduledOnWorkerExecutor) {
  SlotTable<4> slots;
  RpcController controller;
  RecordingExecutor executor;
  std::atomic<int> ran {0};
  CountingTask task(ran);
  butil::IOBuf body;

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, ContinuationTarget::Worker(executor));
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CompleteSlot(cid, body));

  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(executor.schedules.load(), 1);
}

TEST(SlotTableTest, ResponseWinsWhenItCompletesBeforeClose) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);
  butil::IOBuf body;

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CompleteSlot(cid, body));
  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 0U);

  EXPECT_FALSE(controller.Failed());
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, CloseWinsAndMakesSubsequentResponseLate) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);
  butil::IOBuf body;

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 1U);
  EXPECT_FALSE(slots.CompleteSlot(cid, body));

  EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(slots.response_completion_stats().late_response, 1U);
}

TEST(SlotTableTest, CancelWinsWhenItRacesCloseFirst) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CancelSlot(cid));
  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 0U);

  EXPECT_EQ(controller.ErrorCode(), RPC_ECANCELED);
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, CloseWinsWhenItRacesCancelFirst) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 1U);
  EXPECT_FALSE(slots.CancelSlot(cid));

  EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, TimeoutAndCloseResolveExactlyOnce) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.TimeoutSlot(cid));
  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 0U);

  EXPECT_EQ(controller.ErrorCode(), RPC_ETIMEOUT);
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, CloseWinsWhenItRacesTimeoutFirst) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 1U);
  EXPECT_FALSE(slots.TimeoutSlot(cid));

  EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableConcurrencyTest, ResponseAndTimeoutRaceHasExactlyOneWinner) {
  constexpr int kIterations = 512;
  for (int iteration = 0; iteration < kIterations; ++iteration) {
    SlotTable<1> slots;
    RpcController controller;
    std::atomic<int> ran {0};
    CountingTask task(ran);
    butil::IOBuf body;

    const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
    ASSERT_NE(cid, 0);
    ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);

    std::barrier start {3};
    std::atomic<bool> response_won {false};
    std::atomic<bool> timeout_won {false};
    std::thread response_thread([&] {
      start.arrive_and_wait();
      response_won.store(slots.CompleteSlot(cid, body), std::memory_order_release);
    });
    std::thread timeout_thread([&] {
      start.arrive_and_wait();
      timeout_won.store(slots.TimeoutSlot(cid), std::memory_order_release);
    });
    start.arrive_and_wait();
    response_thread.join();
    timeout_thread.join();

    ASSERT_NE(response_won.load(std::memory_order_acquire), timeout_won.load(std::memory_order_acquire));
    EXPECT_EQ(ran.load(std::memory_order_acquire), 1);
    if (response_won.load(std::memory_order_acquire)) {
      EXPECT_FALSE(controller.Failed());
    } else {
      EXPECT_EQ(controller.ErrorCode(), RPC_ETIMEOUT);
    }
  }
}

TEST(SlotTableConcurrencyTest, ResponseCancelAndTimeoutRaceCompletesExactlyOnce) {
  constexpr int kIterations = 512;
  for (int iteration = 0; iteration < kIterations; ++iteration) {
    SlotTable<1> slots;
    RpcController controller;
    std::atomic<int> ran {0};
    CountingTask task(ran);
    butil::IOBuf body;

    const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
    ASSERT_NE(cid, 0);
    ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);

    std::barrier start {4};
    std::atomic<bool> response_won {false};
    std::atomic<bool> cancel_won {false};
    std::atomic<bool> timeout_won {false};
    std::thread response_thread([&] {
      start.arrive_and_wait();
      response_won.store(slots.CompleteSlot(cid, body), std::memory_order_release);
    });
    std::thread cancel_thread([&] {
      start.arrive_and_wait();
      cancel_won.store(slots.CancelSlot(cid), std::memory_order_release);
    });
    std::thread timeout_thread([&] {
      start.arrive_and_wait();
      timeout_won.store(slots.TimeoutSlot(cid), std::memory_order_release);
    });
    start.arrive_and_wait();
    response_thread.join();
    cancel_thread.join();
    timeout_thread.join();

    const int winner_count = static_cast<int>(response_won.load(std::memory_order_acquire)) +
                             static_cast<int>(cancel_won.load(std::memory_order_acquire)) +
                             static_cast<int>(timeout_won.load(std::memory_order_acquire));
    EXPECT_EQ(winner_count, 1);
    EXPECT_EQ(ran.load(std::memory_order_acquire), 1);
  }
}

TEST(SlotTableConcurrencyTest, ConcurrentAllocationAndReuseNeverHandsOutOneCellTwice) {
  constexpr std::size_t kCapacity = 32;
  constexpr int kThreadCount = 8;
  constexpr int kIterationsPerThread = 2000;

  SlotTable<kCapacity> slots;
  ClientMetrics metrics;
  slots.SetClientMetrics(metrics);
  std::array<std::atomic<int>, kCapacity> owners {};
  std::atomic<int> failures {0};
  std::atomic<int> completions {0};
  std::barrier start {kThreadCount};
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);

  for (int thread_index = 0; thread_index < kThreadCount; ++thread_index) {
    threads.emplace_back([&] {
      RpcController controller;
      CountingTask task(completions);
      butil::IOBuf body;
      start.arrive_and_wait();

      for (int iteration = 0; iteration < kIterationsPerThread; ++iteration) {
        controller.RecordStart();
        const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
        if (cid == 0) {
          failures.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        const uint32_t slot_id = SlotTable<kCapacity>::GetSlotId(cid);
        if (owners[slot_id].fetch_add(1, std::memory_order_acq_rel) != 0) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
        if (slots.PublishSlot(cid) != PublishOutcome::kInFlight) {
          failures.fetch_add(1, std::memory_order_relaxed);
          owners[slot_id].fetch_sub(1, std::memory_order_acq_rel);
          continue;
        }

        // CompleteSlot recycles the cell before it returns. Stop considering
        // this thread its owner just before the terminal transition; the cell
        // cannot be allocated by another thread until CompleteSlot pushes it
        // back onto the mutex-protected free list.
        if (owners[slot_id].fetch_sub(1, std::memory_order_acq_rel) != 1) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
        if (!slots.CompleteSlot(cid, body)) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
  EXPECT_EQ(completions.load(std::memory_order_acquire), kThreadCount * kIterationsPerThread);
  EXPECT_EQ(slots.active_slot_count(), 0U);
  EXPECT_EQ(metrics.calls_completed.Value(), kThreadCount * kIterationsPerThread);
  EXPECT_EQ(metrics.calls_succeeded.Value(), kThreadCount * kIterationsPerThread);
  EXPECT_EQ(metrics.call_errors.Value(), 0);
  EXPECT_EQ(metrics.active_in_flight.Value(), 0);
}

TEST(SlotTableTest, StaleTerminalEventsCannotChangeReusedSlotLifecycle) {
  SlotTable<1> slots;
  RpcController old_controller;
  RpcController new_controller;
  std::atomic<int> old_completions {0};
  std::atomic<int> new_completions {0};
  CountingTask old_task(old_completions);
  CountingTask new_task(new_completions);
  butil::IOBuf body;

  const uint64_t old_cid = slots.AllocateSlot(&old_task, nullptr, &old_controller, TestContinuationTarget());
  ASSERT_NE(old_cid, 0);
  ASSERT_EQ(slots.PublishSlot(old_cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CompleteSlot(old_cid, body));

  const uint64_t new_cid = slots.AllocateSlot(&new_task, nullptr, &new_controller, TestContinuationTarget());
  ASSERT_NE(new_cid, 0);
  ASSERT_NE(new_cid, old_cid);
  ASSERT_EQ(SlotTable<1>::GetSlotId(new_cid), SlotTable<1>::GetSlotId(old_cid));
  ASSERT_EQ(slots.PublishSlot(new_cid), PublishOutcome::kInFlight);

  std::barrier start {4};
  std::thread stale_timeout([&] {
    start.arrive_and_wait();
    for (int i = 0; i < 2000; ++i) {
      EXPECT_FALSE(slots.TimeoutSlot(old_cid));
    }
  });
  std::thread stale_cancel([&] {
    start.arrive_and_wait();
    for (int i = 0; i < 2000; ++i) {
      EXPECT_FALSE(slots.CancelSlot(old_cid));
    }
  });
  std::thread stale_response([&] {
    start.arrive_and_wait();
    for (int i = 0; i < 2000; ++i) {
      EXPECT_FALSE(slots.CompleteSlot(old_cid, body));
    }
  });
  start.arrive_and_wait();
  stale_timeout.join();
  stale_cancel.join();
  stale_response.join();

  EXPECT_EQ(new_completions.load(std::memory_order_acquire), 0);
  EXPECT_FALSE(new_controller.Failed());
  EXPECT_TRUE(slots.CompleteSlot(new_cid, body));
  EXPECT_EQ(new_completions.load(std::memory_order_acquire), 1);
}

TEST(SlotTableTest, ConfiguredMaxInFlightBoundsAllocatedSlots) {
  SlotTable<2> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);
  butil::IOBuf body;

  const uint64_t first =
      slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget(), nullptr, /*max_in_flight=*/1);
  ASSERT_NE(first, 0);
  EXPECT_EQ(slots.active_slot_count(), 1U);
  EXPECT_EQ(slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget(), nullptr,
                               /*max_in_flight=*/1),
            0U);

  ASSERT_EQ(slots.PublishSlot(first), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CompleteSlot(first, body));
  EXPECT_EQ(slots.active_slot_count(), 0U);
  EXPECT_NE(slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget(), nullptr,
                               /*max_in_flight=*/1),
            0U);
}

TEST(SlotTableTest, LocalFailurePreservesItsErrorCodeAndReleasesQuota) {
  SlotTable<1> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid =
      slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget(), nullptr, /*max_in_flight=*/1);
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.FailSlot(cid, RPC_EOVERLOAD, "outbound queue full"));

  EXPECT_EQ(controller.ErrorCode(), RPC_EOVERLOAD);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(slots.active_slot_count(), 0U);
}

TEST(SlotTableTest, TerminalFailureFailsEveryPendingCall) {
  SlotTable<4> slots;
  RpcController first_controller;
  RpcController second_controller;
  std::atomic<int> first_ran {0};
  std::atomic<int> second_ran {0};
  CountingTask first_task(first_ran);
  CountingTask second_task(second_ran);

  const uint64_t first = slots.AllocateSlot(&first_task, nullptr, &first_controller, TestContinuationTarget());
  const uint64_t second = slots.AllocateSlot(&second_task, nullptr, &second_controller, TestContinuationTarget());
  ASSERT_NE(first, 0);
  ASSERT_NE(second, 0);
  ASSERT_EQ(slots.PublishSlot(first), PublishOutcome::kInFlight);
  ASSERT_EQ(slots.PublishSlot(second), PublishOutcome::kInFlight);

  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 2U);
  EXPECT_TRUE(first_controller.Failed());
  EXPECT_TRUE(second_controller.Failed());
  EXPECT_EQ(first_controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(second_controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(first_ran.load(), 1);
  EXPECT_EQ(second_ran.load(), 1);
  EXPECT_EQ(slots.active_slot_count(), 0U);
}

TEST(SlotTableTest, FailAllActiveSlotsDefersArmingCallUntilPublish) {
  SlotTable<4> slots;
  RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, TestContinuationTarget());
  ASSERT_NE(cid, 0);

  EXPECT_EQ(slots.FailAllActiveSlots(RPC_ECONN_FAILED, "peer closed"), 0U)
      << "ARMING calls are not yet in the active published-call set";
  EXPECT_FALSE(controller.Failed()) << "ARMING must retain caller-owned objects until PublishSlot";
  EXPECT_EQ(ran.load(), 0);
  EXPECT_EQ(slots.active_slot_count(), 1U);

  EXPECT_EQ(slots.PublishSlot(cid), PublishOutcome::kFailed);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), RPC_ECONN_FAILED);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(slots.active_slot_count(), 0U);
}

}  // namespace
