#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "ant_server/constants.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc {

// Slot State definitions
enum class SlotState : uint32_t { FREE = 0, IN_FLIGHT = 1, COMPLETED = 2, TIMED_OUT = 3 };

// Align to 64 bytes (1 CPU Cache Line) to prevent false sharing under high concurrency
struct alignas(ant_server::constants::kCacheLineSize) CallSlot {
  std::atomic<uint32_t> version {0};
  std::atomic<uint32_t> state {static_cast<uint32_t>(SlotState::FREE)};
  std::coroutine_handle<> handle {nullptr};
  google::protobuf::Message* response_msg {nullptr};
  RpcController* controller {nullptr};
  google::protobuf::Closure* done {nullptr};
  butil::IOBuf* raw_resp_body {nullptr};
  uint32_t next_free {0};
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
    free_head_.store(PackTaggedIndex(0, 0), std::memory_order_relaxed);
  }

  ~SlotTable() = default;

  SlotTable(const SlotTable&) = delete;
  SlotTable& operator=(const SlotTable&) = delete;

  // Allocate an in-flight slot and generate a 64-bit correlation_id
  uint64_t AllocateSlot(std::coroutine_handle<> handle, google::protobuf::Message* response_msg,
                        RpcController* controller, google::protobuf::Closure* done,
                        butil::IOBuf* raw_resp_body = nullptr) {
    uint32_t slot_id = PopFreeSlot();
    if (slot_id == INVALID_SLOT) {
      return 0;  // Capacity exhausted
    }

    CallSlot& slot = slots_[slot_id];
    // Increment version for ABA protection
    uint32_t version = slot.version.fetch_add(1, std::memory_order_relaxed) + 1;

    slot.handle = handle;
    slot.response_msg = response_msg;
    slot.controller = controller;
    slot.done = done;
    slot.raw_resp_body = raw_resp_body;
    slot.state.store(static_cast<uint32_t>(SlotState::IN_FLIGHT), std::memory_order_release);

    return MakeCorrelationId(version, slot_id);
  }

  // Complete slot on receiving network response (Zero-Copy parse Protobuf + resume coroutine/done)
  bool CompleteSlot(uint64_t correlation_id, const butil::IOBuf& body_iobuf, RpcMeta&& meta,
                    const butil::IOBuf& attachment_iobuf) {
    uint32_t slot_id = GetSlotId(correlation_id);
    uint32_t version = GetVersion(correlation_id);

    if (slot_id >= Capacity) {
      return false;
    }

    CallSlot& slot = slots_[slot_id];

    // ABA check: if version doesn't match, this response is late/abandoned
    if (slot.version.load(std::memory_order_acquire) != version) {
      return false;
    }

    // Atomic CAS transition: IN_FLIGHT -> COMPLETED
    uint32_t expected = static_cast<uint32_t>(SlotState::IN_FLIGHT);
    if (!slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::COMPLETED),
                                            std::memory_order_acq_rel)) {
      return false;  // Lost race against timeout or cancellation
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

    auto handle = slot.handle;
    auto* done = slot.done;

    // Return slot to free pool
    PushFreeSlot(slot_id);

    // Resume coroutine or run callback
    if (handle) {
      handle.resume();
    } else if (done) {
      done->Run();
    }

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

  // Timeout or cancel slot
  bool TimeoutSlot(uint64_t correlation_id, const std::string& err_msg = "RPC call timed out") {
    uint32_t slot_id = GetSlotId(correlation_id);
    uint32_t version = GetVersion(correlation_id);

    if (slot_id >= Capacity) {
      return false;
    }

    CallSlot& slot = slots_[slot_id];

    if (slot.version.load(std::memory_order_acquire) != version) {
      return false;
    }

    uint32_t expected = static_cast<uint32_t>(SlotState::IN_FLIGHT);
    if (!slot.state.compare_exchange_strong(expected, static_cast<uint32_t>(SlotState::TIMED_OUT),
                                            std::memory_order_acq_rel)) {
      return false;  // Response already arrived
    }

    if (slot.controller) {
      slot.controller->SetFailed(RPC_ETIMEOUT, err_msg);
      slot.controller->set_latency_us(slot.controller->CalculateElapsedUs());
    }

    auto handle = slot.handle;
    auto* done = slot.done;

    PushFreeSlot(slot_id);

    if (handle) {
      handle.resume();
    } else if (done) {
      done->Run();
    }

    return true;
  }

  static constexpr uint64_t MakeCorrelationId(uint32_t version, uint32_t slot_id) {
    return (static_cast<uint64_t>(version) << 32) | static_cast<uint64_t>(slot_id);
  }

  static constexpr uint32_t GetSlotId(uint64_t correlation_id) {
    return static_cast<uint32_t>(correlation_id & 0xFFFFFFFF);
  }

  static constexpr uint32_t GetVersion(uint64_t correlation_id) { return static_cast<uint32_t>(correlation_id >> 32); }

 private:
  static constexpr uint64_t PackTaggedIndex(uint32_t index, uint32_t tag) {
    return (static_cast<uint64_t>(tag) << 32) | static_cast<uint64_t>(index);
  }

  static constexpr uint32_t ExtractIndex(uint64_t tagged) { return static_cast<uint32_t>(tagged & 0xFFFFFFFF); }

  static constexpr uint32_t ExtractTag(uint64_t tagged) { return static_cast<uint32_t>(tagged >> 32); }

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
    slots_[slot_id].state.store(static_cast<uint32_t>(SlotState::FREE), std::memory_order_release);
    slots_[slot_id].handle = nullptr;
    slots_[slot_id].response_msg = nullptr;
    slots_[slot_id].controller = nullptr;
    slots_[slot_id].done = nullptr;
    slots_[slot_id].raw_resp_body = nullptr;

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
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
