#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include <google/protobuf/service.h>

#include "ant_server/rpc/error_code.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "rpc_meta.pb.h"

namespace ant_server::rpc {

template <size_t Capacity>
class SlotTable;

namespace detail {

// The channel-side endpoint of a controller's active RPC registration.  It
// deliberately exposes only cancellation: RpcController must not know about
// a channel's IO driver, slot storage, or connection lifecycle.
class RpcCallCancellationTarget {
 public:
  virtual ~RpcCallCancellationTarget() = default;

  virtual bool CancelRpcSlot(uint64_t correlation_id) = 0;
};

}  // namespace detail

// ============================================================================
// RpcController: The central invocation context for RPC requests & responses.
// Production-ready implementation of google::protobuf::RpcController.
// Inspired by Apache bRPC Controller, optimized for modern C++20 coroutines.
// ============================================================================
class RpcController : public google::protobuf::RpcController {
 public:
  // Thread-safety: while a call is active, StartCancel() may be invoked from
  // any thread. All other mutation, Reset(), and destruction are owner-side
  // operations and must not race with an active call or its completion.
  RpcController() = default;
  ~RpcController() override = default;

  RpcController(const RpcController&) = delete;
  RpcController& operator=(const RpcController&) = delete;
  RpcController(RpcController&&) = delete;
  RpcController& operator=(RpcController&&) = delete;

  // --------------------------------------------------------------------------
  // 1. Standard google::protobuf::RpcController Interface
  // --------------------------------------------------------------------------
  void Reset() override {
    failed_ = false;
    cancel_requested_.store(false, std::memory_order_release);
    error_code_ = RPC_SUCCESS;
    error_text_.clear();
    correlation_id_ = 0;
    log_id_ = 0;
    timeout_ms_ = -1;
    remote_side_ = butil::EndPoint();
    local_side_ = butil::EndPoint();
    request_headers_.clear();
    response_headers_.clear();
    request_attachment_.clear();
    response_attachment_.clear();
    stop_token_ = std::stop_token();
    start_time_ = std::chrono::steady_clock::now();
    latency_us_ = 0;
  }

  [[nodiscard]] bool Failed() const override { return failed_; }

  [[nodiscard]] std::string ErrorText() const override { return error_text_; }

  // Thread-safe cancellation request. For an active client call this enters
  // SlotTable::CancelSlot(correlation_id); SlotTable, rather than this method,
  // decides whether cancellation wins against response, timeout, or close.
  void StartCancel() override {
    cancel_requested_.store(true, std::memory_order_release);
    std::optional<ActiveSlot> active_slot;
    {
      std::lock_guard<std::mutex> lock(active_slot_mu_);
      active_slot = active_slot_;
    }
    RequestCancel(active_slot);
  }

  void SetFailed(const std::string& reason) override { SetFailed(RPC_EINTERNAL, reason); }

  [[nodiscard]] bool IsCanceled() const override {
    return cancel_requested_.load(std::memory_order_acquire) || stop_token_.stop_requested() ||
           (failed_ && error_code_ == RPC_ECANCELED);
  }

  void NotifyOnCancel(google::protobuf::Closure* callback) override { (void)callback; }

  // --------------------------------------------------------------------------
  // 2. Enhanced Error Management & Codes
  // --------------------------------------------------------------------------
  void SetFailed(int error_code, std::string_view reason) {
    failed_ = true;
    error_code_ = error_code;
    error_text_ = std::string(reason);
  }

  [[nodiscard]] int ErrorCode() const noexcept { return error_code_; }

  // --------------------------------------------------------------------------
  // 3. Distributed Tracing & RPC IDs
  // --------------------------------------------------------------------------
  void SetCorrelationId(uint64_t id) noexcept { correlation_id_ = id; }
  [[nodiscard]] uint64_t CorrelationId() const noexcept { return correlation_id_; }

  void SetLogId(uint64_t id) noexcept { log_id_ = id; }
  [[nodiscard]] uint64_t LogId() const noexcept { return log_id_; }

  void set_log_id(uint64_t id) noexcept { log_id_ = id; }
  [[nodiscard]] uint64_t log_id() const noexcept { return log_id_; }

  // --------------------------------------------------------------------------
  // 4. Network Endpoints
  // --------------------------------------------------------------------------
  void SetRemoteSide(const butil::EndPoint& ep) noexcept { remote_side_ = ep; }
  [[nodiscard]] const butil::EndPoint& RemoteSide() const noexcept { return remote_side_; }

  void SetLocalSide(const butil::EndPoint& ep) noexcept { local_side_ = ep; }
  [[nodiscard]] const butil::EndPoint& LocalSide() const noexcept { return local_side_; }

  // --------------------------------------------------------------------------
  // 5. Inbound Request Metadata / Headers (Client -> Server)
  // --------------------------------------------------------------------------
  void SetRequestHeader(std::string_view key, std::string_view value) {
    request_headers_[std::string(key)] = std::string(value);
  }

  void SetHeader(std::string_view key, std::string_view value) { SetRequestHeader(key, value); }

  [[nodiscard]] std::optional<std::string_view> GetRequestHeader(std::string_view key) const {
    auto map_it = request_headers_.find(std::string(key));
    if (map_it != request_headers_.end()) {
      return map_it->second;
    }
    return std::nullopt;
  }

  [[nodiscard]] const google::protobuf::Map<std::string, std::string>& RequestHeaders() const noexcept {
    return request_headers_;
  }

  google::protobuf::Map<std::string, std::string>& MutableRequestHeaders() noexcept { return request_headers_; }

  // --------------------------------------------------------------------------
  // 6. Outbound Response Metadata / Headers (Server -> Client)
  // --------------------------------------------------------------------------
  void SetResponseHeader(std::string_view key, std::string_view value) {
    response_headers_[std::string(key)] = std::string(value);
  }

  [[nodiscard]] std::optional<std::string_view> GetResponseHeader(std::string_view key) const {
    auto map_it = response_headers_.find(std::string(key));
    if (map_it != response_headers_.end()) {
      return map_it->second;
    }
    return std::nullopt;
  }

  [[nodiscard]] const google::protobuf::Map<std::string, std::string>& ResponseHeaders() const noexcept {
    return response_headers_;
  }

  google::protobuf::Map<std::string, std::string>& MutableResponseHeaders() noexcept { return response_headers_; }

  // --------------------------------------------------------------------------
  // Unified / Backwards-compatible Header Interface
  // --------------------------------------------------------------------------
  [[nodiscard]] std::optional<std::string_view> GetHeader(std::string_view key) const {
    auto resp_hdr = GetResponseHeader(key);
    if (resp_hdr.has_value()) {
      return resp_hdr;
    }
    return GetRequestHeader(key);
  }

  [[nodiscard]] const google::protobuf::Map<std::string, std::string>& Headers() const noexcept {
    return RequestHeaders();
  }

  google::protobuf::Map<std::string, std::string>& MutableHeaders() noexcept { return MutableRequestHeaders(); }

  // --------------------------------------------------------------------------
  // 7. Zero-Copy Attachments (butil::IOBuf)
  // --------------------------------------------------------------------------
  butil::IOBuf& RequestAttachment() noexcept { return request_attachment_; }
  [[nodiscard]] const butil::IOBuf& RequestAttachment() const noexcept { return request_attachment_; }

  butil::IOBuf& ResponseAttachment() noexcept { return response_attachment_; }
  [[nodiscard]] const butil::IOBuf& ResponseAttachment() const noexcept { return response_attachment_; }

  // --------------------------------------------------------------------------
  // 8. Timeout & Cancellation
  // --------------------------------------------------------------------------
  void SetTimeoutMs(int64_t timeout_ms) noexcept { timeout_ms_ = timeout_ms; }
  [[nodiscard]] int64_t TimeoutMs() const noexcept { return timeout_ms_; }

  void set_timeout_ms(int64_t timeout_ms) noexcept { timeout_ms_ = timeout_ms; }
  [[nodiscard]] int64_t timeout_ms() const noexcept { return timeout_ms_; }

  void SetStopToken(std::stop_token token) noexcept { stop_token_ = std::move(token); }
  [[nodiscard]] std::stop_token GetStopToken() const noexcept { return stop_token_; }

  // --------------------------------------------------------------------------
  // 9. Performance & Latency Metrics
  // --------------------------------------------------------------------------
  void RecordStart() { start_time_ = std::chrono::steady_clock::now(); }
  void set_latency_us(int64_t us) noexcept { latency_us_ = us; }
  [[nodiscard]] int64_t latency_us() const noexcept { return latency_us_; }

  [[nodiscard]] int64_t CalculateElapsedUs() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(now - start_time_).count();
  }

 private:
  template <size_t Capacity>
  friend class SlotTable;
  friend class RpcChannel;

  // A controller has at most one active RPC. correlation_id is versioned by
  // SlotTable, so a stale cancellation snapshot cannot affect a recycled
  // slot. The weak target avoids keeping an already-closed channel alive.
  struct ActiveSlot {
    std::weak_ptr<detail::RpcCallCancellationTarget> target;
    uint64_t correlation_id {0};
  };

  static void RequestCancel(const std::optional<ActiveSlot>& active_slot) {
    if (!active_slot.has_value()) {
      return;
    }
    if (auto target = active_slot->target.lock()) {
      (void)target->CancelRpcSlot(active_slot->correlation_id);
    }
  }

  // Core-only: the channel binds the one slot currently owned by this
  // controller. A cancellation that arrived before the bind is delivered
  // immediately after it, while the slot is still in ARMING.
  bool BindActiveSlot(const std::shared_ptr<detail::RpcCallCancellationTarget>& target, uint64_t correlation_id) {
    std::optional<ActiveSlot> active_slot;
    bool cancel_requested = false;
    {
      std::lock_guard<std::mutex> lock(active_slot_mu_);
      if (active_slot_.has_value()) {
        return false;
      }
      active_slot_ = ActiveSlot {target, correlation_id};
      active_slot = active_slot_;
      cancel_requested = cancel_requested_.load(std::memory_order_acquire);
    }
    if (cancel_requested) {
      RequestCancel(active_slot);
    }
    return true;
  }

  // Core-only: only the slot with this exact versioned correlation id may
  // detach the active slot. This prevents completion of an old call from unbinding
  // a controller that has subsequently been reused.
  void ClearActiveSlot(uint64_t correlation_id) {
    std::lock_guard<std::mutex> lock(active_slot_mu_);
    if (active_slot_.has_value() && active_slot_->correlation_id == correlation_id) {
      active_slot_.reset();
    }
  }

  bool failed_ {false};
  int error_code_ {RPC_SUCCESS};
  std::string error_text_;

  uint64_t correlation_id_ {0};
  uint64_t log_id_ {0};

  butil::EndPoint remote_side_;
  butil::EndPoint local_side_;

  google::protobuf::Map<std::string, std::string> request_headers_;
  google::protobuf::Map<std::string, std::string> response_headers_;

  butil::IOBuf request_attachment_;
  butil::IOBuf response_attachment_;

  int64_t timeout_ms_ {-1};
  std::stop_token stop_token_;

  std::atomic<bool> cancel_requested_ {false};
  std::mutex active_slot_mu_;
  std::optional<ActiveSlot> active_slot_;

  int64_t latency_us_ {0};
  std::chrono::steady_clock::time_point start_time_ {std::chrono::steady_clock::now()};
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
