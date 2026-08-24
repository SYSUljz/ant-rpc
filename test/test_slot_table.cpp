#include <atomic>

#include <gtest/gtest.h>

#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/slot_table.hpp"

namespace {

struct CountingTask final : TaskNode {
  explicit CountingTask(std::atomic<int>& count) : count_(count) {
    execute = [](TaskNode* self) noexcept { static_cast<CountingTask*>(self)->count_.fetch_add(1); };
  }

  std::atomic<int>& count_;
};

TEST(SlotTableTest, CancelDuringArmingIsDeliveredByPublish) {
  ant_server::rpc::SlotTable<4> slots;
  ant_server::rpc::RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, {});
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_EQ(ran.load(), 0) << "ARMING must not run the continuation";
  EXPECT_FALSE(controller.Failed());

  EXPECT_EQ(slots.PublishSlot(cid), ant_server::rpc::PublishOutcome::kCanceled);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), ant_server::rpc::RPC_ECANCELED);
}

TEST(SlotTableTest, TimeoutDuringArmingIsDeliveredByPublish) {
  ant_server::rpc::SlotTable<4> slots;
  ant_server::rpc::RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, {});
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.TimeoutSlot(cid));
  EXPECT_EQ(ran.load(), 0) << "ARMING must not run the continuation";

  EXPECT_EQ(slots.PublishSlot(cid), ant_server::rpc::PublishOutcome::kTimedOut);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_TRUE(controller.Failed());
  EXPECT_EQ(controller.ErrorCode(), ant_server::rpc::RPC_ETIMEOUT);
}

TEST(SlotTableTest, FirstPendingResolutionWins) {
  ant_server::rpc::SlotTable<4> slots;
  ant_server::rpc::RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, {});
  ASSERT_NE(cid, 0);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_FALSE(slots.TimeoutSlot(cid));
  EXPECT_EQ(slots.PublishSlot(cid), ant_server::rpc::PublishOutcome::kCanceled);
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(controller.ErrorCode(), ant_server::rpc::RPC_ECANCELED);
}

TEST(SlotTableTest, PublishedSlotResolvesImmediately) {
  ant_server::rpc::SlotTable<4> slots;
  ant_server::rpc::RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, {});
  ASSERT_NE(cid, 0);
  ASSERT_EQ(slots.PublishSlot(cid), ant_server::rpc::PublishOutcome::kInFlight);

  EXPECT_TRUE(slots.CancelSlot(cid));
  EXPECT_EQ(ran.load(), 1);
  EXPECT_EQ(controller.ErrorCode(), ant_server::rpc::RPC_ECANCELED);
}

TEST(SlotTableTest, ResponseCannotCompleteAnArmingSlot) {
  ant_server::rpc::SlotTable<4> slots;
  ant_server::rpc::RpcController controller;
  std::atomic<int> ran {0};
  CountingTask task(ran);

  const uint64_t cid = slots.AllocateSlot(&task, nullptr, &controller, {});
  ASSERT_NE(cid, 0);
  butil::IOBuf body;

  EXPECT_FALSE(slots.CompleteSlot(cid, body)) << "a response may only resolve a published slot";
  EXPECT_EQ(ran.load(), 0);
  ASSERT_EQ(slots.PublishSlot(cid), ant_server::rpc::PublishOutcome::kInFlight);
  EXPECT_TRUE(slots.CompleteSlot(cid, body));
  EXPECT_EQ(ran.load(), 1);
}

TEST(SlotTableTest, LateResponseAfterCancellationIsIgnored) {
  ant_server::rpc::SlotTable<1> slots;
  ant_server::rpc::RpcController first_controller;
  ant_server::rpc::RpcController second_controller;
  std::atomic<int> first_ran {0};
  std::atomic<int> second_ran {0};
  CountingTask first_task(first_ran);
  CountingTask second_task(second_ran);
  butil::IOBuf body;

  const uint64_t first_cid = slots.AllocateSlot(&first_task, nullptr, &first_controller, {});
  ASSERT_NE(first_cid, 0);
  ASSERT_EQ(slots.PublishSlot(first_cid), ant_server::rpc::PublishOutcome::kInFlight);
  ASSERT_TRUE(slots.CancelSlot(first_cid));
  ASSERT_EQ(first_ran.load(), 1);

  const uint64_t second_cid = slots.AllocateSlot(&second_task, nullptr, &second_controller, {});
  ASSERT_NE(second_cid, 0);
  ASSERT_NE(second_cid, first_cid) << "slot reuse must advance the correlation-id version";
  ASSERT_EQ(slots.PublishSlot(second_cid), ant_server::rpc::PublishOutcome::kInFlight);

  EXPECT_FALSE(slots.CompleteSlot(first_cid, body)) << "a late response must not resolve a recycled slot";
  EXPECT_EQ(second_ran.load(), 0);
  EXPECT_TRUE(slots.CompleteSlot(second_cid, body));
  EXPECT_EQ(second_ran.load(), 1);
}

}  // namespace
