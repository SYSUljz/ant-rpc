#pragma once

#include <cstdint>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ant_server/rpc/error_code.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc {

// ============================================================================
// RpcController: The central invocation context for RPC requests & responses.
// Inspired by Apache bRPC Controller, optimized for C++20 coroutines.
//
// Key Responsibilities:
// 1. Error Status Management (ErrorCode, ErrorText, SetFailed).
// 2. Network Metadata (Local & Remote butil::EndPoint).
// 3. Distributed Tracing & RPC IDs (CorrelationId, LogId).
// 4. Custom Key-Value Headers/Metadata (Auth tokens, trace context).
// 5. Zero-Copy Binary Attachments (butil::IOBuf).
// 6. Timeout & Structured Cancellation (std::stop_token).
// 7. Full State Reset for Object Reuse / Connection Pooling.
// ============================================================================
class RpcController {
 public:
  RpcController() = default;
  ~RpcController() = default;

  RpcController(const RpcController&) = delete;
  RpcController& operator=(const RpcController&) = delete;
  RpcController(RpcController&&) noexcept = default;
  RpcController& operator=(RpcController&&) noexcept = default;

  // --------------------------------------------------------------------------
  // 1. Error & Status Management
  // --------------------------------------------------------------------------
  [[nodiscard]] bool Failed() const noexcept { return failed_; }
  [[nodiscard]] int ErrorCode() const noexcept { return error_code_; }
  [[nodiscard]] const std::string& ErrorText() const noexcept { return error_text_; }

  void SetFailed(int error_code, std::string_view reason) {
    failed_ = true;
    error_code_ = error_code;
    error_text_ = std::string(reason);
  }

  void SetFailed(std::string_view reason) { SetFailed(RPC_EINTERNAL, reason); }

  // --------------------------------------------------------------------------
  // 2. Distributed Tracing & IDs
  // --------------------------------------------------------------------------
  void SetCorrelationId(uint64_t id) noexcept { correlation_id_ = id; }
  [[nodiscard]] uint64_t CorrelationId() const noexcept { return correlation_id_; }

  void SetLogId(uint64_t id) noexcept { log_id_ = id; }
  [[nodiscard]] uint64_t LogId() const noexcept { return log_id_; }

  // --------------------------------------------------------------------------
  // 3. Network Endpoints
  // --------------------------------------------------------------------------
  void SetRemoteSide(const butil::EndPoint& ep) noexcept { remote_side_ = ep; }
  [[nodiscard]] const butil::EndPoint& RemoteSide() const noexcept { return remote_side_; }

  void SetLocalSide(const butil::EndPoint& ep) noexcept { local_side_ = ep; }
  [[nodiscard]] const butil::EndPoint& LocalSide() const noexcept { return local_side_; }

  // --------------------------------------------------------------------------
  // 4. Custom Metadata / Headers (Key-Value pairs)
  // --------------------------------------------------------------------------
  // 4. Custom Metadata / Headers (Zero-Copy Key-Value pairs)
  // --------------------------------------------------------------------------
  void SetHeader(std::string_view key, std::string_view value) { headers_[key] = value; }

  [[nodiscard]] std::optional<std::string_view> GetHeader(std::string_view key) const {
    auto it = headers_.find(key);
    if (it != headers_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  [[nodiscard]] const absl::flat_hash_map<std::string_view, std::string_view>& Headers() const noexcept {
    return headers_;
  }
  absl::flat_hash_map<std::string_view, std::string_view>& MutableHeaders() noexcept { return headers_; }

  // --------------------------------------------------------------------------
  // 5. Zero-Copy Attachments (butil::IOBuf)
  // --------------------------------------------------------------------------
  butil::IOBuf& RequestAttachment() noexcept { return request_attachment_; }
  [[nodiscard]] const butil::IOBuf& RequestAttachment() const noexcept { return request_attachment_; }

  butil::IOBuf& ResponseAttachment() noexcept { return response_attachment_; }
  [[nodiscard]] const butil::IOBuf& ResponseAttachment() const noexcept { return response_attachment_; }

  // --------------------------------------------------------------------------
  // 6. Timeout & Cancellation
  // --------------------------------------------------------------------------
  void SetTimeoutMs(int64_t timeout_ms) noexcept { timeout_ms_ = timeout_ms; }
  [[nodiscard]] int64_t TimeoutMs() const noexcept { return timeout_ms_; }

  void SetStopToken(std::stop_token token) noexcept { stop_token_ = std::move(token); }
  [[nodiscard]] std::stop_token GetStopToken() const noexcept { return stop_token_; }

  [[nodiscard]] bool IsCanceled() const noexcept {
    return stop_token_.stop_requested() || (failed_ && error_code_ == RPC_ECANCELED);
  }

  // --------------------------------------------------------------------------
  // 7. Reset: Reclaims state for reuse in connection/buffer pools
  // --------------------------------------------------------------------------
  void Reset() {
    failed_ = false;
    error_code_ = RPC_SUCCESS;
    error_text_.clear();
    correlation_id_ = 0;
    log_id_ = 0;
    remote_side_ = butil::EndPoint();
    local_side_ = butil::EndPoint();
    headers_.clear();
    request_attachment_.clear();
    response_attachment_.clear();
    timeout_ms_ = -1;
    stop_token_ = std::stop_token();
  }

 private:
  bool failed_ {false};
  int error_code_ {RPC_SUCCESS};
  std::string error_text_;

  uint64_t correlation_id_ {0};
  uint64_t log_id_ {0};

  butil::EndPoint remote_side_;
  butil::EndPoint local_side_;

  absl::flat_hash_map<std::string_view, std::string_view> headers_;

  butil::IOBuf request_attachment_;
  butil::IOBuf response_attachment_;

  int64_t timeout_ms_ {-1};
  std::stop_token stop_token_;
};

}  // namespace ant_server::rpc
