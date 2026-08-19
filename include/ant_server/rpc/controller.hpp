#pragma once

#include <chrono>
#include <cstdint>
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

// ============================================================================
// RpcController: The central invocation context for RPC requests & responses.
// Production-ready implementation of google::protobuf::RpcController.
// Inspired by Apache bRPC Controller, optimized for modern C++20 coroutines.
// ============================================================================
class RpcController : public google::protobuf::RpcController {
 public:
  RpcController() = default;
  ~RpcController() override = default;

  RpcController(const RpcController&) = delete;
  RpcController& operator=(const RpcController&) = delete;
  RpcController(RpcController&&) noexcept = default;
  RpcController& operator=(RpcController&&) noexcept = default;

  // --------------------------------------------------------------------------
  // 1. Standard google::protobuf::RpcController Interface
  // --------------------------------------------------------------------------
  void Reset() override {
    failed_ = false;
    canceled_ = false;
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

  void StartCancel() override {
    canceled_ = true;
    SetFailed(RPC_ECANCELED, "RPC call was canceled");
  }

  void SetFailed(const std::string& reason) override {
    SetFailed(RPC_EINTERNAL, reason);
  }

  [[nodiscard]] bool IsCanceled() const override {
    return canceled_ || stop_token_.stop_requested() || (failed_ && error_code_ == RPC_ECANCELED);
  }

  void NotifyOnCancel(google::protobuf::Closure* callback) override {
    (void)callback;
  }

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

  void SetHeader(std::string_view key, std::string_view value) {
    SetRequestHeader(key, value);
  }

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

  google::protobuf::Map<std::string, std::string>& MutableRequestHeaders() noexcept {
    return request_headers_;
  }

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

  google::protobuf::Map<std::string, std::string>& MutableResponseHeaders() noexcept {
    return response_headers_;
  }

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

  google::protobuf::Map<std::string, std::string>& MutableHeaders() noexcept {
    return MutableRequestHeaders();
  }

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
  bool failed_ {false};
  bool canceled_ {false};
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

  int64_t latency_us_ {0};
  std::chrono::steady_clock::time_point start_time_ {std::chrono::steady_clock::now()};
};

}  // namespace ant_server::rpc

namespace ant_rpc {
  using namespace ant_server::rpc;
}
