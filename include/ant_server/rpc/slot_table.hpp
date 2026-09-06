#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <google/protobuf/message.h>

#include "absl/synchronization/mutex.h"
#include "ant_server/constants.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/core/client_metrics.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
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

// Version and state must be observed and changed as one indivisible value.
// Checking version first and CASing state later leaves a reuse window in
// which a stale resolver can change the state of a newer call in the same
// slot.
constexpr uint64_t PackSlotLifecycle(uint32_t version, SlotState state) noexcept {
  return (static_cast<uint64_t>(version) << 32) | static_cast<uint32_t>(state);
}

constexpr uint32_t SlotLifecycleVersion(uint64_t lifecycle) noexcept { return static_cast<uint32_t>(lifecycle >> 32); }

constexpr SlotState SlotLifecycleState(uint64_t lifecycle) noexcept {
  return static_cast<SlotState>(static_cast<uint32_t>(lifecycle));
}

// Result of handing an ARMING slot to the resolver side. A pending failure is
// delivered by PublishSlot itself, never by the thread that requested it.
enum class PublishOutcome : uint8_t { kInFlight, kCanceled, kTimedOut, kFailed, kInvalid };

// Where a slot's continuation should run once the slot resolves. Every live
// RPC slot owns a worker executor; IO threads never run a completion inline.
struct ContinuationTarget {
  Executor* executor {nullptr};
  static ContinuationTarget Worker(Executor& executor) noexcept { return ContinuationTarget {&executor}; }
};

// All RPC completions are load-balanced by the channel worker executor. A
// network round-trip makes preserving the issuing worker's CPU-cache locality
// ineffective, and business coroutines are intentionally migratable.
inline ContinuationTarget CurrentContinuationTarget(Executor& fallback_executor) noexcept {
  return ContinuationTarget::Worker(fallback_executor);
}

enum class ResponseCompletionOutcome : uint8_t { kCompleted, kUnknownCorrelationId, kLateResponse };

struct ResponseCompletionStats {
  uint64_t completed {0};
  uint64_t unknown_correlation_id {0};
  uint64_t late_response {0};
};

// Non-response terminal events share one state transition and one cleanup
// path. Response remains separate because it carries protobuf/IOBuf payload,
// but it joins the same finalizer after applying that payload.
enum class SlotResolutionKind : uint8_t { kCanceled, kTimedOut, kFailed };

struct SlotResolution {
  SlotResolutionKind kind;
  int error_code;
  std::string_view error_message;

  static SlotResolution Canceled(std::string_view message = "RPC call canceled") {
    return {SlotResolutionKind::kCanceled, RPC_ECANCELED, message};
  }
  static SlotResolution TimedOut(std::string_view message = "RPC call timed out") {
    return {SlotResolutionKind::kTimedOut, RPC_ETIMEOUT, message};
  }
  static SlotResolution Failed(int error_code, std::string_view message) {
    return {SlotResolutionKind::kFailed, error_code, message};
  }
};

// Align to 64 bytes (1 CPU Cache Line) to prevent false sharing under high concurrency
struct alignas(ant_server::constants::kCacheLineSize) CallSlot {
  std::atomic<uint64_t> lifecycle {PackSlotLifecycle(0, SlotState::FREE)};
  TaskNode* task {nullptr};
  google::protobuf::Message* response_msg {nullptr};
  RpcController* controller {nullptr};
  butil::IOBuf* raw_resp_body {nullptr};
  Executor* executor {nullptr};
  uint32_t next_free {0};
  std::atomic<uint64_t> deadline_timer_id {0};
};

static_assert(sizeof(CallSlot) == 64, "CallSlot must stay within a single cache line");

// NotifyOnCancel is user code, so it follows the same worker-executor rule as
// the RPC continuation. The wrapper owns itself and never touches Controller
// after invoking the closure: user code may destroy that controller.
struct NotifyThenContinueTask final : TaskNode {
  NotifyThenContinueTask(google::protobuf::Closure* notify, TaskNode* continuation)
      : notify_(notify), continuation_(continuation) {
    execute = [](TaskNode* self) noexcept {
      auto* task = static_cast<NotifyThenContinueTask*>(self);
      google::protobuf::Closure* notify = task->notify_;
      TaskNode* continuation = task->continuation_;
      delete task;
      if (notify != nullptr) {
        notify->Run();
      }
      if (continuation != nullptr) {
        continuation->run();
      }
    };
  }

 private:
  google::protobuf::Closure* notify_;
  TaskNode* continuation_;
};

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
    free_head_ = 0;
  }

  ~SlotTable() = default;

  SlotTable(const SlotTable&) = delete;
  SlotTable& operator=(const SlotTable&) = delete;

  // Must be configured before this table accepts calls with deadlines. The
  // keeper itself is thread-safe; terminal resolution may cancel a timer from
  // an IO, worker, or timer thread.
  void SetDeadlineTimerKeeper(TimerKeeper& timer_keeper) noexcept { deadline_timer_keeper_ = &timer_keeper; }
  void SetClientMetrics(ClientMetrics& metrics) noexcept { client_metrics_ = &metrics; }

  // Allocate a slot in ARMING state and generate a 64-bit correlation_id.
  // The slot is not resolvable until PublishSlot() moves it to IN_FLIGHT.
  uint64_t AllocateSlot(TaskNode* task, google::protobuf::Message* response_msg, RpcController* controller,
                        ContinuationTarget target, butil::IOBuf* raw_resp_body = nullptr,
                        std::size_t max_in_flight = Capacity) {
    if (target.executor == nullptr || max_in_flight == 0 || max_in_flight > Capacity ||
        !TryAcquireLiveSlot(max_in_flight)) {
      return 0;
    }
    uint32_t slot_id = PopFreeSlot();
    if (slot_id == INVALID_SLOT) {
      active_slots_.fetch_sub(1, std::memory_order_acq_rel);
      return 0;  // Capacity exhausted
    }

    CallSlot& slot = slots_[slot_id];
    // PopFreeSlot gives this thread exclusive ownership of the cell. Advance
    // the generation before publishing ARMING so stale terminal events cannot
    // act on this reuse.
    const uint32_t version = SlotLifecycleVersion(slot.lifecycle.load(std::memory_order_relaxed)) + 1;

    slot.task = task;
    slot.response_msg = response_msg;
    slot.controller = controller;
    slot.raw_resp_body = raw_resp_body;
    slot.executor = target.executor;
    slot.deadline_timer_id.store(0, std::memory_order_relaxed);
    slot.lifecycle.store(PackSlotLifecycle(version, SlotState::ARMING), std::memory_order_release);
    if (client_metrics_ != nullptr) {
      client_metrics_->active_in_flight.Add(1);
    }

    return MakeCorrelationId(version, slot_id);
  }

  // Publish an ARMING slot. A cancel/timeout that arrived while the sender was
  // preparing the frame is retained as *_PENDING and completed here, after the
  // sender's final read of request/controller.
  PublishOutcome PublishSlot(uint64_t correlation_id) {
    CallSlot* slot = SlotAt(correlation_id);
    if (slot == nullptr) {
      return PublishOutcome::kInvalid;
    }

    int failure_code = RPC_ECONN_FAILED;
    std::string failure_message;
    if (!RegisterPublishedSlot(correlation_id, &failure_code, &failure_message)) {
      uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::ARMING);
      if (slot->lifecycle.compare_exchange_strong(
              expected, PackSlotLifecycle(GetVersion(correlation_id), SlotState::FAILED), std::memory_order_acq_rel)) {
        ApplyFailureAndFinalize(*slot, correlation_id, SlotResolution::Failed(failure_code, failure_message));
        return PublishOutcome::kFailed;
      }
      if (SlotLifecycleVersion(expected) == GetVersion(correlation_id)) {
        if (const auto pending = PendingResolution(SlotLifecycleState(expected));
            pending.has_value() &&
            TransitionAndFinalize(*slot, correlation_id, SlotLifecycleState(expected), *pending)) {
          return pending->kind == SlotResolutionKind::kCanceled ? PublishOutcome::kCanceled : PublishOutcome::kTimedOut;
        }
      }
      return PublishOutcome::kInvalid;
    }

    uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::ARMING);
    if (slot->lifecycle.compare_exchange_strong(
            expected, PackSlotLifecycle(GetVersion(correlation_id), SlotState::IN_FLIGHT), std::memory_order_acq_rel)) {
      if (TakeConnectionFailure(correlation_id, &failure_code, &failure_message)) {
        expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::IN_FLIGHT);
        if (slot->lifecycle.compare_exchange_strong(expected,
                                                    PackSlotLifecycle(GetVersion(correlation_id), SlotState::FAILED),
                                                    std::memory_order_acq_rel)) {
          ApplyFailureAndFinalize(*slot, correlation_id, SlotResolution::Failed(failure_code, failure_message));
          return PublishOutcome::kFailed;
        }
        return PublishOutcome::kInvalid;
      }
      return PublishOutcome::kInFlight;
    }

    UnregisterPublishedSlot(correlation_id);
    if (SlotLifecycleVersion(expected) == GetVersion(correlation_id)) {
      const SlotState observed_state = SlotLifecycleState(expected);
      if (const auto pending = PendingResolution(observed_state);
          pending.has_value() && TransitionAndFinalize(*slot, correlation_id, observed_state, *pending)) {
        return pending->kind == SlotResolutionKind::kCanceled ? PublishOutcome::kCanceled : PublishOutcome::kTimedOut;
      }
    }
    return PublishOutcome::kInvalid;
  }

  // Abandon a slot that has not been published. This path never dispatches a
  // continuation: StartUnaryCall's caller owns that decision for inline
  // failures.
  bool DiscardArmingSlot(uint64_t correlation_id) {
    CallSlot* slot = SlotAt(correlation_id);
    if (slot == nullptr) {
      return false;
    }
    uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::ARMING);
    if (!slot->lifecycle.compare_exchange_strong(
            expected, PackSlotLifecycle(GetVersion(correlation_id), SlotState::TIMED_OUT), std::memory_order_acq_rel)) {
      return false;
    }
    CancelDeadlineTimer(*slot);
    if (slot->controller) {
      slot->controller->ClearActiveSlot(correlation_id);
    }
    PushFreeSlot(correlation_id);
    return true;
  }

  // A deadline is installed while the slot is ARMING. A timer that fires
  // before PublishSlot() can only move the slot to TIMEOUT_PENDING; the
  // sender remains the sole owner of caller data until PublishSlot().
  bool AttachDeadlineTimer(uint64_t correlation_id, uint64_t timer_id) {
    if (timer_id == 0) {
      return false;
    }
    CallSlot* slot = SlotAt(correlation_id);
    if (slot == nullptr) {
      return false;
    }
    const uint64_t lifecycle = slot->lifecycle.load(std::memory_order_acquire);
    if (SlotLifecycleVersion(lifecycle) != GetVersion(correlation_id)) {
      return false;
    }
    const SlotState state = SlotLifecycleState(lifecycle);
    if (state != SlotState::ARMING && state != SlotState::CANCEL_PENDING && state != SlotState::TIMEOUT_PENDING) {
      return false;
    }
    slot->deadline_timer_id.store(timer_id, std::memory_order_release);
    return true;
  }

  // Complete a slot on receiving a network response (Zero-Copy protobuf parse
  // plus continuation dispatch). The result preserves observability for
  // unknown and stale correlation IDs without ever completing a recycled slot.
  ResponseCompletionOutcome CompleteResponseSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf,
                                                 RpcMeta&& meta, const butil::IOBuf& attachment_iobuf) {
    const uint32_t slot_id = GetSlotId(correlation_id);
    if (slot_id >= Capacity) {
      unknown_correlation_id_.fetch_add(1, std::memory_order_relaxed);
      return ResponseCompletionOutcome::kUnknownCorrelationId;
    }
    CallSlot& slot = slots_[slot_id];
    // Version and state participate in the same CAS. A delayed response that
    // observed an older generation can never complete a reused slot.
    uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::IN_FLIGHT);
    if (!slot.lifecycle.compare_exchange_strong(
            expected, PackSlotLifecycle(GetVersion(correlation_id), SlotState::COMPLETED), std::memory_order_acq_rel)) {
      late_response_.fetch_add(1, std::memory_order_relaxed);
      return ResponseCompletionOutcome::kLateResponse;
    }

    int completion_error_code = meta.error_code();
    if (slot.controller) {
      slot.controller->ClearActiveSlot(correlation_id);
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
        completion_error_code = RPC_EINVALID_DATA;
        if (slot.controller) {
          slot.controller->SetFailed(RPC_EINVALID_DATA, "Failed to deserialize response protobuf");
        }
      }
    }

    if (slot.raw_resp_body) {
      *slot.raw_resp_body = body_iobuf;
    }
    if (slot.controller && slot.controller->Failed()) {
      completion_error_code = slot.controller->ErrorCode();
    }

    completed_responses_.fetch_add(1, std::memory_order_relaxed);
    FinalizeSlotAndDispatch(slot, correlation_id, completion_error_code);
    return ResponseCompletionOutcome::kCompleted;
  }

  bool CompleteSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf, RpcMeta&& meta,
                    const butil::IOBuf& attachment_iobuf) {
    return CompleteResponseSlot(correlation_id, body_iobuf, std::move(meta), attachment_iobuf) ==
           ResponseCompletionOutcome::kCompleted;
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
    return ResolveTerminal(correlation_id, SlotResolution::TimedOut(err_msg), /*defer_while_arming=*/true);
  }

  // Cancellation is requested through RpcController::StartCancel().
  bool CancelSlot(uint64_t correlation_id, const std::string& err_msg = "RPC call canceled") {
    return ResolveTerminal(correlation_id, SlotResolution::Canceled(err_msg), /*defer_while_arming=*/true);
  }

  // Fails an already-published call for transport and local resource errors.
  // Unlike TimeoutSlot this preserves the supplied error code.
  bool FailSlot(uint64_t correlation_id, int error_code, const std::string& err_msg) {
    return ResolveTerminal(correlation_id, SlotResolution::Failed(error_code, err_msg), /*defer_while_arming=*/false);
  }

  // Fail every published call after a channel-level terminal event. The active
  // set is protected by a mutex rather than made lock-free deliberately: this
  // rare control-plane operation must be O(active calls), and safe concurrent
  // removal is more valuable here than a lock-free list on the hot path.
  //
  // Marking the table failed while taking the set closes the PublishSlot race:
  // an ARMING call that was not in the snapshot fails itself when it publishes.
  std::size_t FailAllActiveSlots(int error_code, std::string_view err_msg) {
    std::unordered_set<uint64_t> active_calls;
    {
      absl::MutexLock lock(&active_calls_mu_);
      if (connection_failed_) {
        return 0;
      }
      connection_failed_ = true;
      connection_failure_code_ = error_code;
      connection_failure_message_ = std::string(err_msg);
      active_calls.swap(active_calls_);
    }
    for (uint64_t correlation_id : active_calls) {
      FailSlot(correlation_id, error_code, std::string(err_msg));
    }
    return active_calls.size();
  }

  static constexpr uint64_t MakeCorrelationId(uint32_t version, uint32_t slot_id) {
    return (static_cast<uint64_t>(version) << 32) | static_cast<uint64_t>(slot_id);
  }

  static constexpr uint32_t GetSlotId(uint64_t correlation_id) {
    return static_cast<uint32_t>(correlation_id & 0xFFFFFFFF);
  }

  static constexpr uint32_t GetVersion(uint64_t correlation_id) { return static_cast<uint32_t>(correlation_id >> 32); }

  [[nodiscard]] std::size_t active_slot_count() const noexcept { return active_slots_.load(std::memory_order_acquire); }
  [[nodiscard]] ResponseCompletionStats response_completion_stats() const noexcept {
    return {completed_responses_.load(std::memory_order_relaxed),
            unknown_correlation_id_.load(std::memory_order_relaxed), late_response_.load(std::memory_order_relaxed)};
  }

 private:
  CallSlot* SlotAt(uint64_t correlation_id) {
    uint32_t slot_id = GetSlotId(correlation_id);
    if (slot_id >= Capacity) {
      return nullptr;
    }
    return &slots_[slot_id];
  }

  static SlotState TerminalState(const SlotResolution& resolution) {
    switch (resolution.kind) {
      case SlotResolutionKind::kCanceled:
        return SlotState::CANCELED;
      case SlotResolutionKind::kTimedOut:
        return SlotState::TIMED_OUT;
      case SlotResolutionKind::kFailed:
        return SlotState::FAILED;
    }
    std::terminate();
  }

  static std::optional<SlotState> PendingState(const SlotResolution& resolution) {
    switch (resolution.kind) {
      case SlotResolutionKind::kCanceled:
        return SlotState::CANCEL_PENDING;
      case SlotResolutionKind::kTimedOut:
        return SlotState::TIMEOUT_PENDING;
      case SlotResolutionKind::kFailed:
        return std::nullopt;
    }
    std::terminate();
  }

  static std::optional<SlotResolution> PendingResolution(SlotState state) {
    switch (state) {
      case SlotState::CANCEL_PENDING:
        return SlotResolution::Canceled();
      case SlotState::TIMEOUT_PENDING:
        return SlotResolution::TimedOut();
      default:
        return std::nullopt;
    }
  }

  // The single terminal resolver for cancel, timeout, and local/transport
  // failures. A cancel or timeout that arrives in ARMING records a pending
  // event; only PublishSlot() may turn that into a dispatched terminal state.
  bool ResolveTerminal(uint64_t correlation_id, const SlotResolution& resolution, bool defer_while_arming) {
    CallSlot* slot_ptr = SlotAt(correlation_id);
    if (slot_ptr == nullptr) {
      return false;
    }
    CallSlot& slot = *slot_ptr;

    if (defer_while_arming) {
      const auto pending_state = PendingState(resolution);
      uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), SlotState::ARMING);
      if (pending_state.has_value() &&
          slot.lifecycle.compare_exchange_strong(
              expected, PackSlotLifecycle(GetVersion(correlation_id), *pending_state), std::memory_order_acq_rel)) {
        return true;
      }
    }

    return TransitionAndFinalize(slot, correlation_id, SlotState::IN_FLIGHT, resolution);
  }

  bool TransitionAndFinalize(CallSlot& slot, uint64_t correlation_id, SlotState expected_state,
                             const SlotResolution& resolution) {
    uint64_t expected = PackSlotLifecycle(GetVersion(correlation_id), expected_state);
    if (!slot.lifecycle.compare_exchange_strong(
            expected, PackSlotLifecycle(GetVersion(correlation_id), TerminalState(resolution)),
            std::memory_order_acq_rel)) {
      return false;
    }
    ApplyFailureAndFinalize(slot, correlation_id, resolution);
    return true;
  }

  bool RegisterPublishedSlot(uint64_t correlation_id, int* failure_code, std::string* failure_message) {
    absl::MutexLock lock(&active_calls_mu_);
    if (!connection_failed_) {
      active_calls_.insert(correlation_id);
      return true;
    }
    *failure_code = connection_failure_code_;
    *failure_message = connection_failure_message_;
    return false;
  }

  bool TakeConnectionFailure(uint64_t correlation_id, int* failure_code, std::string* failure_message) {
    absl::MutexLock lock(&active_calls_mu_);
    if (!connection_failed_) {
      return false;
    }
    active_calls_.erase(correlation_id);
    *failure_code = connection_failure_code_;
    *failure_message = connection_failure_message_;
    return true;
  }

  void UnregisterPublishedSlot(uint64_t correlation_id) {
    absl::MutexLock lock(&active_calls_mu_);
    active_calls_.erase(correlation_id);
  }

  void ApplyFailureAndFinalize(CallSlot& slot, uint64_t correlation_id, const SlotResolution& resolution) {
    if (slot.controller) {
      slot.controller->ClearActiveSlot(correlation_id);
      slot.controller->SetFailed(resolution.error_code, resolution.error_message);
      slot.controller->set_latency_us(slot.controller->CalculateElapsedUs());
    }
    FinalizeSlotAndDispatch(slot, correlation_id, resolution.error_code);
  }

  // Recycle the slot, then hand the continuation to its origin executor.
  // The slot MUST be released before dispatching: once the continuation runs, the coroutine frame
  // owning the TaskNode may already be destroyed, so nothing in `slot` may be read afterwards.
  void FinalizeSlotAndDispatch(CallSlot& slot, uint64_t correlation_id, int error_code) {
    TaskNode* task = slot.task;
    Executor* executor = slot.executor;
    google::protobuf::Closure* notify = slot.controller ? slot.controller->TakeNotifyOnCompletion() : nullptr;
    const uint64_t latency_us = slot.controller ? static_cast<uint64_t>(slot.controller->latency_us()) : 0;

    if (client_metrics_ != nullptr) {
      client_metrics_->RecordCompletion(error_code, latency_us);
    }

    UnregisterPublishedSlot(correlation_id);
    CancelDeadlineTimer(slot);

    PushFreeSlot(correlation_id);

    if (task == nullptr && notify == nullptr) {
      return;
    }
    // AllocateSlot rejects a null executor. Keep this guard defensive because
    // the slot may be inspected while being recycled in debug tooling.
    if (executor == nullptr) {
      std::terminate();
    }
    if (notify != nullptr) {
      executor->schedule(new NotifyThenContinueTask(notify, task));
    } else {
      executor->schedule(task);
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

  void ReleaseLiveSlot() {
    active_slots_.fetch_sub(1, std::memory_order_acq_rel);
    if (client_metrics_ != nullptr) {
      client_metrics_->active_in_flight.Add(-1);
    }
  }

  uint32_t PopFreeSlot() {
    absl::MutexLock lock(&free_list_mu_);
    if (free_head_ == INVALID_SLOT) {
      return INVALID_SLOT;
    }
    const uint32_t index = free_head_;
    free_head_ = slots_[index].next_free;
    return index;
  }

  void PushFreeSlot(uint64_t correlation_id) {
    const uint32_t slot_id = GetSlotId(correlation_id);
    slots_[slot_id].task = nullptr;
    slots_[slot_id].response_msg = nullptr;
    slots_[slot_id].controller = nullptr;
    slots_[slot_id].raw_resp_body = nullptr;
    slots_[slot_id].executor = nullptr;
    slots_[slot_id].deadline_timer_id.store(0, std::memory_order_relaxed);
    slots_[slot_id].lifecycle.store(PackSlotLifecycle(GetVersion(correlation_id), SlotState::FREE),
                                    std::memory_order_release);
    ReleaseLiveSlot();

    absl::MutexLock lock(&free_list_mu_);
    slots_[slot_id].next_free = free_head_;
    free_head_ = slot_id;
  }

  void CancelDeadlineTimer(CallSlot& slot) {
    const uint64_t timer_id = slot.deadline_timer_id.exchange(0, std::memory_order_acq_rel);
    if (timer_id != 0 && deadline_timer_keeper_ != nullptr) {
      deadline_timer_keeper_->CancelTimer(timer_id);
    }
  }

  std::unique_ptr<CallSlot[]> slots_;
  absl::Mutex free_list_mu_;
  uint32_t free_head_ {0};
  std::atomic<std::size_t> active_slots_ {0};
  TimerKeeper* deadline_timer_keeper_ {nullptr};
  ClientMetrics* client_metrics_ {nullptr};
  std::atomic<uint64_t> completed_responses_ {0};
  std::atomic<uint64_t> unknown_correlation_id_ {0};
  std::atomic<uint64_t> late_response_ {0};
  absl::Mutex active_calls_mu_;
  std::unordered_set<uint64_t> active_calls_;
  bool connection_failed_ {false};
  int connection_failure_code_ {RPC_ECONN_FAILED};
  std::string connection_failure_message_;
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
