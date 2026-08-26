#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include <google/protobuf/message.h>

#include "ant_server/constants.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc {

// Slot State definitions.
//
// ARMING is the pre-publish state: the sender still owns request/controller while building the
// frame. Cancel and timeout do not dispatch during this phase; they atomically become a pending
// resolution that PublishSlot() delivers after the sender has stopped touching caller-owned data.
enum class SlotState : uint32_t {
  FREE = 0,
  ARMING = 1,
  CANCEL_PENDING = 2,
  TIMEOUT_PENDING = 3,
  IN_FLIGHT = 4,
  COMPLETED = 5,
  TIMED_OUT = 6,
  CANCELED = 7,
  FAILED = 8,
};

// Result of handing an ARMING slot to the resolver side. A pending failure is
// delivered by PublishSlot itself, never by the thread that requested it.
enum class PublishOutcome : uint8_t { kInFlight, kCanceled, kTimedOut, kInvalid };

// Where a slot's continuation should run once the slot resolves.
// A null executor means "run inline on the resolving thread", which is what blocking call paths
// want (their continuation only signals a Notification and must not be queued behind a worker).
struct ContinuationTarget {
  Executor* executor {nullptr};
  uint32_t thread_id {0};
};

// Captures the current thread's executor affinity. Coroutine call paths use this so that a
// resumed coroutine wakes up on the thread it suspended on, keeping g_local_context /
// g_thread_id consistent (io_uring rings are single-thread owned and must not be crossed).
inline ContinuationTarget CurrentContinuationTarget() noexcept {
  return ContinuationTarget {g_executor, static_cast<uint32_t>(g_thread_id)};
}

// Type-erased cancel functor stored inside a caller-owned std::stop_callback.
// Deliberately trivial (24 bytes, no allocation) so it never lands in std::function.
struct SlotCancelFn {
  void (*cancel)(void* table, uint64_t correlation_id) noexcept {nullptr};
  void* table {nullptr};
  uint64_t correlation_id {0};

  void operator()() const noexcept {
    if (cancel) {
      cancel(table, correlation_id);
    }
  }
};

// Storage for a slot's cancellation registration. Owned by the CALLER (coroutine frame or the
// stack of a blocking call), never by the slot: the slot is recycled by whichever thread resolves
// it, so a callback living there would be concurrently constructed and destroyed.
using SlotStopCallback = std::optional<std::stop_callback<SlotCancelFn>>;

// Align to 64 bytes (1 CPU Cache Line) to prevent false sharing under high concurrency
struct alignas(ant_server::constants::kCacheLineSize) CallSlot {
  std::atomic<uint32_t> version {0};
  std::atomic<uint32_t> state {static_cast<uint32_t>(SlotState::FREE)};
  TaskNode* task {nullptr};
  google::protobuf::Message* response_msg {nullptr};
  RpcController* controller {nullptr};
  butil::IOBuf* raw_resp_body {nullptr};
  Executor* executor {nullptr};
  uint32_t origin_thread {0};
  uint32_t next_free {0};
};

static_assert(sizeof(CallSlot) == 64, "CallSlot must stay within a single cache line");

template <size_t Capacity = 65536>
class SlotTable {
 public:
  static constexpr uint32_t INVALID_SLOT = 0xFFFFFFFF;

  SlotTable() {
    slots_ = std::make_unique<CallSlot[]>(Capacity);
    // Initialize free list chain
    for (size_t i = 0; i < Capacity - 1; ++i) {
      slots_[i].next_free = static_cast<uint32_t>(i + 1);
    }
    slots_[Capacity - 1].next_free = INVALID_SLOT;
    free_head_.store(PackTaggedIndex(0, 0), std::memory_order_relaxed);
  }

  ~SlotTable() = default;

  SlotTable(const SlotTable&) = delete;
  SlotTable& operator=(const SlotTable&) = delete;

  // Allocate a slot in ARMING state and generate a 64-bit correlation_id.
  // The slot is not resolvable until PublishSlot() moves it to IN_FLIGHT.
  uint64_t AllocateSlot(TaskNode* task, google::protobuf::Message* response_msg, RpcController* controller,
                        ContinuationTarget target, butil::IOBuf* raw_resp_body = nullptr,
                        std::size_t max_in_flight = Capacity) {
    if (max_in_flight == 0 || max_in_flight > Capacity || !TryAcquireLiveSlot(max_in_flight)) {
      return 0;
    }
    uint32_t slot_id = PopFreeSlot();
    if (slot_id == INVALID_SLOT) {
      ReleaseLiveSlot();
      return 0;  // Capacity exhausted
    }

    CallSlot& slot = slots_[slot_id];
    // Increment version for ABA protection
    uint32_t version = slot.version.fetch_add(1, std::memory_order_relaxed) + 1;

    slot.task = task;
    slot.response_msg = response_msg;
    slot.controller = controller;
    slot.raw_resp_body = raw_resp_body;
    slot.executor = target.executor;
    slot.origin_thread = target.thread_id;
    slot.state.store(static_cast<uint32_t>(SlotState::ARMING), std::memory_order_release);

    return MakeCorrelationId(version, slot_id);
  }

  // Publish an ARMING slot. A cancel/timeout that arrived while the sender was
  // preparing the frame is retained as *_PENDING and completed here, after the
  // sender's final read of request/controller.
  PublishOutcome PublishSlot(uint64_t correlation_id) {
    CallSlot* slot = LookupSlot(correlation_id);
    if (slot == nullptr) {
      return PublishOutcome::kInvalid;
    }

    uint32_t expected = static_cast<uint32_t>(SlotState::ARMING);
    if (slot->state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::IN_FLIGHT),
                                            std::memory_order_acq_rel)) {
      return PublishOutcome::kInFlight;
    }

    if (expected == static_cast<uint32_t>(SlotState::CANCEL_PENDING)) {
      expected = static_cast<uint32_t>(SlotState::CANCEL_PENDING);
      if (slot->state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::CANCELED),
                                              std::memory_order_acq_rel)) {
        FinishFailure(*slot, GetSlotId(correlation_id), RPC_ECANCELED, "RPC call canceled via stop_token");
        return PublishOutcome::kCanceled;
      }
    } else if (expected == static_cast<uint32_t>(SlotState::TIMEOUT_PENDING)) {
      expected = static_cast<uint32_t>(SlotState::TIMEOUT_PENDING);
      if (slot->state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::TIMED_OUT),
                                              std::memory_order_acq_rel)) {
        FinishFailure(*slot, GetSlotId(correlation_id), RPC_ETIMEOUT, "RPC call timed out");
        return PublishOutcome::kTimedOut;
      }
    }
    return PublishOutcome::kInvalid;
  }

  // Abandon a slot that has not been published. This path never dispatches a
  // continuation: StartUnaryCall's caller owns that decision for inline
  // failures.
  bool DiscardArmingSlot(uint64_t correlation_id) {
    CallSlot* slot = LookupSlot(correlation_id);
    if (slot == nullptr) {
      return false;
    }
    uint32_t expected = static_cast<uint32_t>(SlotState::ARMING);
    if (!slot->state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::TIMED_OUT),
                                             std::memory_order_acq_rel)) {
      return false;
    }
    PushFreeSlot(GetSlotId(correlation_id));
    return true;
  }

  // Build a cancel functor for caller-owned std::stop_callback storage.
  SlotCancelFn MakeCancelFn(uint64_t correlation_id) noexcept {
    return SlotCancelFn {[](void* table, uint64_t cid) noexcept { static_cast<SlotTable*>(table)->CancelSlot(cid); },
                         this, correlation_id};
  }

  // Complete slot on receiving network response (Zero-Copy parse Protobuf + dispatch continuation)
  bool CompleteSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf, RpcMeta&& meta,
                    const butil::IOBuf& attachment_iobuf) {
    CallSlot* slot_ptr = LookupSlot(correlation_id);
    if (slot_ptr == nullptr) {
      return false;
    }
    CallSlot& slot = *slot_ptr;

    // Atomic CAS transition: IN_FLIGHT -> COMPLETED
    uint32_t expected = static_cast<uint32_t>(SlotState::IN_FLIGHT);
    if (!slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::COMPLETED),
                                            std::memory_order_acq_rel)) {
      return false;  // Not published yet, or lost race against timeout/cancellation
    }

    if (slot.controller) {
      if (meta.error_code() != RPC_SUCCESS) {
        slot.controller->SetFailed(meta.error_code(), meta.error_text());
      }
      if (!meta.headers().empty()) {
        slot.controller->MutableResponseHeaders().swap(*meta.mutable_headers());
      }
      if (!attachment_iobuf.empty()) {
        slot.controller->ResponseAttachment() = attachment_iobuf;
      }
      slot.controller->set_latency_us(slot.controller->CalculateElapsedUs());
    }

    // Parse Protobuf zero-copy from response IOBuf if applicable
    if (slot.response_msg && !body_iobuf.empty() && (!slot.controller || !slot.controller->Failed())) {
      butil::IOBufAsZeroCopyInputStream zc_in(body_iobuf);
      if (!slot.response_msg->ParseFromZeroCopyStream(&zc_in)) {
        if (slot.controller) {
          slot.controller->SetFailed(RPC_EINVALID_DATA, "Failed to deserialize response protobuf");
        }
      }
    }

    if (slot.raw_resp_body) {
      *slot.raw_resp_body = body_iobuf;
    }

    ReleaseAndDispatch(slot, GetSlotId(correlation_id));
    return true;
  }

  bool CompleteSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf, const RpcMeta& meta,
                    const butil::IOBuf& attachment_iobuf) {
    RpcMeta meta_copy = meta;
    return CompleteSlot(correlation_id, body_iobuf, std::move(meta_copy), attachment_iobuf);
  }

  // Simplified complete slot overload
  bool CompleteSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf) {
    RpcMeta meta;
    meta.set_correlation_id(correlation_id);
    butil::IOBuf dummy_attachment;
    return CompleteSlot(correlation_id, body_iobuf, meta, dummy_attachment);
  }

  // Timeout slot
  bool TimeoutSlot(uint64_t correlation_id, const std::string& err_msg = "RPC call timed out") {
    return ResolveFailure(correlation_id, SlotState::TIMED_OUT, RPC_ETIMEOUT, err_msg);
  }

  // Cancel slot on stop_token trigger
  bool CancelSlot(uint64_t correlation_id, const std::string& err_msg = "RPC call canceled via stop_token") {
    return ResolveFailure(correlation_id, SlotState::CANCELED, RPC_ECANCELED, err_msg);
  }

  // Fails an already-published call for transport and local resource errors.
  // Unlike TimeoutSlot this preserves the supplied error code.
  bool FailSlot(uint64_t correlation_id, int error_code, const std::string& err_msg) {
    CallSlot* slot_ptr = LookupSlot(correlation_id);
    if (slot_ptr == nullptr) {
      return false;
    }
    CallSlot& slot = *slot_ptr;
    uint32_t expected = static_cast<uint32_t>(SlotState::IN_FLIGHT);
    if (!slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::FAILED),
                                            std::memory_order_acq_rel)) {
      return false;
    }
    FinishFailure(slot, GetSlotId(correlation_id), error_code, err_msg);
    return true;
  }

  static constexpr uint64_t MakeCorrelationId(uint32_t version, uint32_t slot_id) {
    return (static_cast<uint64_t>(version) << 32) | static_cast<uint64_t>(slot_id);
  }

  static constexpr uint32_t GetSlotId(uint64_t correlation_id) {
    return static_cast<uint32_t>(correlation_id & 0xFFFFFFFF);
  }

  static constexpr uint32_t GetVersion(uint64_t correlation_id) { return static_cast<uint32_t>(correlation_id >> 32); }

  [[nodiscard]] std::size_t active_slot_count() const noexcept { return active_slots_.load(std::memory_order_acquire); }

 private:
  static constexpr uint64_t PackTaggedIndex(uint32_t index, uint32_t tag) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(index);
  }

  static constexpr uint32_t ExtractIndex(uint64_t tagged) { return static_cast<uint32_t>(tagged & 0xFFFFFFFF); }

  static constexpr uint32_t ExtractTag(uint64_t tagged) { return static_cast<uint32_t>(tagged >> 32); }

  // Bounds + ABA check. Returns nullptr if the correlation_id is stale or out of range.
  CallSlot* LookupSlot(uint64_t correlation_id) {
    uint32_t slot_id = GetSlotId(correlation_id);
    if (slot_id >= Capacity) {
      return nullptr;
    }
    CallSlot& slot = slots_[slot_id];
    if (slot.version.load(std::memory_order_acquire) != GetVersion(correlation_id)) {
      return nullptr;
    }
    return &slot;
  }

  // Shared body of TimeoutSlot / CancelSlot. A failure during ARMING is
  // retained as a pending state; a failure during IN_FLIGHT dispatches now.
  bool ResolveFailure(uint64_t correlation_id, SlotState target_state, int error_code, const std::string& err_msg) {
    CallSlot* slot_ptr = LookupSlot(correlation_id);
    if (slot_ptr == nullptr) {
      return false;
    }
    CallSlot& slot = *slot_ptr;

    uint32_t expected = static_cast<uint32_t>(SlotState::ARMING);
    const SlotState pending_state =
        target_state == SlotState::CANCELED ? SlotState::CANCEL_PENDING : SlotState::TIMEOUT_PENDING;
    if (slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(pending_state), std::memory_order_acq_rel)) {
      return true;  // Accepted; PublishSlot() will dispatch after the safe handoff point.
    }

    expected = static_cast<uint32_t>(SlotState::IN_FLIGHT);
    if (!slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(target_state), std::memory_order_acq_rel)) {
      return false;  // Another resolver won, or this slot has already been recycled.
    }

    FinishFailure(slot, GetSlotId(correlation_id), error_code, err_msg);
    return true;
  }

  void FinishFailure(CallSlot& slot, uint32_t slot_id, int error_code, std::string_view err_msg) {
    if (slot.controller) {
      slot.controller->SetFailed(error_code, err_msg);
      slot.controller->set_latency_us(slot.controller->CalculateElapsedUs());
    }
    ReleaseAndDispatch(slot, slot_id);
  }

  // Recycle the slot, then hand the continuation to its origin executor.
  // The slot MUST be released before dispatching: once the continuation runs, the coroutine frame
  // owning the TaskNode may already be destroyed, so nothing in `slot` may be read afterwards.
  void ReleaseAndDispatch(CallSlot& slot, uint32_t slot_id) {
    TaskNode* task = slot.task;
    Executor* executor = slot.executor;
    std::size_t thread_id = slot.origin_thread;

    PushFreeSlot(slot_id);

    if (task == nullptr) {
      return;
    }
    if (executor != nullptr) {
      // Resume on the thread the call was issued from: thread-local scheduler/io_uring context
      // must stay consistent across the suspension point.
      executor->schedule(task, thread_id);
    } else {
      // No executor affinity (blocking call paths, unit tests): run inline on this thread.
      task->run();
    }
  }

  bool TryAcquireLiveSlot(std::size_t limit) {
    std::size_t current = active_slots_.load(std::memory_order_acquire);
    while (current < limit) {
      if (active_slots_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
        return true;
      }
    }
    return false;
  }

  void ReleaseLiveSlot() { active_slots_.fetch_sub(1, std::memory_order_acq_rel); }

  uint32_t PopFreeSlot() {
    uint64_t current = free_head_.load(std::memory_order_acquire);
    while (true) {
      uint32_t index = ExtractIndex(current);
      if (index == INVALID_SLOT) {
        return INVALID_SLOT;
      }
      uint32_t tag = ExtractTag(current);
      uint32_t next = slots_[index].next_free;
      uint64_t desired = PackTaggedIndex(next, tag + 1);

      if (free_head_.compare_exchange_weak(current, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return index;
      }
    }
  }

  void PushFreeSlot(uint32_t slot_id) {
    slot_id = slot_id % Capacity;
    slots_[slot_id].task = nullptr;
    slots_[slot_id].response_msg = nullptr;
    slots_[slot_id].controller = nullptr;
    slots_[slot_id].raw_resp_body = nullptr;
    slots_[slot_id].executor = nullptr;
    slots_[slot_id].origin_thread = 0;
    slots_[slot_id].state.store(static_cast<uint32_t>(SlotState::FREE), std::memory_order_release);
    ReleaseLiveSlot();

    uint64_t current = free_head_.load(std::memory_order_acquire);
    while (true) {
      uint32_t tag = ExtractTag(current);
      slots_[slot_id].next_free = ExtractIndex(current);
      uint64_t desired = PackTaggedIndex(slot_id, tag + 1);

      if (free_head_.compare_exchange_weak(current, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
      }
    }
  }

  std::unique_ptr<CallSlot[]> slots_;
  std::atomic<uint64_t> free_head_ {0};
  std::atomic<std::size_t> active_slots_ {0};
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
