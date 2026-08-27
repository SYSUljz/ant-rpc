#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include "ant_server/awaiter/resume_on.hpp"
#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/server/server_options.hpp"
#include "ant_server/rpc/service_registry.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc::detail {

class ServerConnection;

class ServerRuntime final : public std::enable_shared_from_this<ServerRuntime> {
 public:
  ServerRuntime(Context& context, RpcServerOptions options)
      : timer_keeper_(context.GetTimerKeeper()), options_(std::move(options)) {}
  bool TryAcquireConnection() noexcept {
    if (!accepting_.load(std::memory_order_acquire)) return false;
    std::size_t current = active_connections_.load(std::memory_order_acquire);
    while (current < options_.max_connections) {
      if (active_connections_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) return true;
    }
    return false;
  }
  void TrackConnection(const std::shared_ptr<ServerConnection>& connection);
  void ReleaseConnection(const std::shared_ptr<ServerConnection>& connection);
  bool TryAcquireInFlight() noexcept {
    std::size_t current = in_flight_.load(std::memory_order_acquire);
    while (current < options_.max_in_flight) {
      if (in_flight_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                           std::memory_order_acquire)) return true;
    }
    return false;
  }
  void ReleaseInFlight() noexcept { in_flight_.fetch_sub(1, std::memory_order_acq_rel); }
  void BeginGracefulStop();
  const RpcServerOptions& options() const noexcept { return options_; }

 private:
  void ForceCloseLiveConnections();
  void CancelGracefulStopTimer();
  TimerKeeper& timer_keeper_;
  const RpcServerOptions options_;
  std::atomic<bool> accepting_ {true};
  std::atomic<std::size_t> active_connections_ {0};
  std::atomic<std::size_t> in_flight_ {0};
  std::atomic<uint64_t> graceful_stop_timer_id_ {0};
  std::mutex connections_mu_;
  std::vector<std::shared_ptr<ServerConnection>> connections_;
};

inline void PackServerErrorResponse(const FrameParseResult& request, int error_code, const char* error_text,
                                    butil::IOBuf& response) {
  RpcMeta meta;
  meta.set_msg_type(RPC_RESPONSE);
  meta.set_correlation_id(request.meta.correlation_id());
  meta.set_service_name(request.meta.service_name());
  meta.set_method_name(request.meta.method_name());
  meta.set_error_code(error_code);
  meta.set_error_text(error_text);
  PackRpcFrame(meta, nullptr, nullptr, response);
}

// Per-accepted-fd state machine. Only its Context IO thread mutates the fd,
// receive buffer, outbound queue and write state. Workers use kSendResponse.
class ServerConnection final : public IoCommandMailbox, public std::enable_shared_from_this<ServerConnection> {
 public:
  struct OutboundFrame { std::shared_ptr<butil::IOBuf> buffer; };
  struct ServerCommand {
    enum class Type : uint8_t { kStart, kSendResponse, kClose };
    Type type;
    OutboundFrame frame {};
    static ServerCommand Start() { return {Type::kStart, {}}; }
    static ServerCommand SendResponse(OutboundFrame frame) { return {Type::kSendResponse, std::move(frame)}; }
    static ServerCommand Close() { return {Type::kClose, {}}; }
  };

  ServerConnection(Context& context, int fd, ServiceRegistry& registry, std::shared_ptr<ServerRuntime> runtime)
      : context_(context), fd_(fd), registry_(registry), runtime_(std::move(runtime)),
        idle_timeout_(runtime_->options().idle_timeout) {}
  void Start() { PostCommand(ServerCommand::Start()); }
  void EnqueueResponse(std::shared_ptr<butil::IOBuf> response) {
    if (response) PostCommand(ServerCommand::SendResponse({std::move(response)}));
  }
  void RequestClose() {
    if (running_.exchange(false, std::memory_order_acq_rel)) PostCommand(ServerCommand::Close());
  }
  int fd() const noexcept { return fd_.load(std::memory_order_acquire); }

  void DrainCommandsOnIoThread() override {
    commands_.Drain([this](ServerCommand&& command) {
      switch (command.type) {
        case ServerCommand::Type::kStart: StartOnIoThread(); break;
        case ServerCommand::Type::kSendResponse:
          if (running_.load(std::memory_order_acquire)) {
            outbound_.push_back(std::move(command.frame));
            StartNextWriteOnIoThread();
          }
          break;
        case ServerCommand::Type::kClose: BeginCloseOnIoThread(); break;
      }
    });
  }

 private:
  struct InFlightGuard { std::shared_ptr<ServerRuntime> runtime; ~InFlightGuard() { runtime->ReleaseInFlight(); } };
  void PostCommand(ServerCommand command) { commands_.Push(std::move(command)); context_.Notify(shared_from_this()); }
  void StartOnIoThread() {
    if (!running_.load(std::memory_order_acquire)) { receiver_exited_ = true; TryFinishCloseOnIoThread(); return; }
    ArmIdleTimerOnIoThread();
    ReceiveLoop(shared_from_this());
  }
  void BeginCloseOnIoThread() {
    CancelIdleTimer();
    if (const int socket = fd(); socket >= 0) shutdown(socket, SHUT_RDWR);
    outbound_.clear();
    if (!receiver_started_) receiver_exited_ = true;
    TryFinishCloseOnIoThread();
  }
  void TryFinishCloseOnIoThread() {
    if (running_.load(std::memory_order_acquire) || !receiver_exited_ || writing_) return;
    CancelIdleTimer();
    if (const int socket = fd_.exchange(-1, std::memory_order_acq_rel); socket >= 0) close(socket);
    if (!released_.exchange(true, std::memory_order_acq_rel)) runtime_->ReleaseConnection(shared_from_this());
  }
  void StartNextWriteOnIoThread() {
    if (writing_ || outbound_.empty() || !running_.load(std::memory_order_acquire)) return;
    writing_ = true;
    WriteFrame(shared_from_this(), outbound_.front());
  }
  void ArmIdleTimerOnIoThread() {
    if (idle_timeout_.count() <= 0 || !running_.load(std::memory_order_acquire)) return;
    const uint64_t generation = idle_generation_.fetch_add(1, std::memory_order_acq_rel) + 1;
    CancelIdleTimer();
    auto self = shared_from_this();
    const uint64_t timer = context_.GetTimerKeeper().AddTimer(idle_timeout_, [self, generation] {
      if (self->idle_generation_.load(std::memory_order_acquire) == generation) self->RequestClose();
    });
    idle_timer_id_.store(timer, std::memory_order_release);
  }
  void CancelIdleTimer() {
    if (const uint64_t timer = idle_timer_id_.exchange(0, std::memory_order_acq_rel); timer != 0) {
      context_.GetTimerKeeper().CancelTimer(timer);
    }
  }
  DetachedTask ReceiveLoop(std::shared_ptr<ServerConnection> self) {
    self->receiver_started_ = true;
    while (self->running_.load(std::memory_order_acquire)) {
      while (true) {
        FrameParseResult frame = TryParseRpcFrame(self->recv_buffer_, self->runtime_->options().max_frame_bytes);
        if (frame.status == FrameParseStatus::NEED_MORE_DATA) break;
        if (frame.status != FrameParseStatus::SUCCESS) { self->running_.store(false, std::memory_order_release); break; }
        self->recv_buffer_.pop_front(frame.total_frame_bytes);
        self->DispatchRequest(self, std::move(frame));
      }
      if (!self->running_.load(std::memory_order_acquire)) break;
      const int bytes = co_await ReadAwaiter(self->context_, self->fd(), self->recv_buffer_, false);
      if (bytes <= 0) break;
      self->ArmIdleTimerOnIoThread();
    }
    self->running_.store(false, std::memory_order_release);
    self->receiver_exited_ = true;
    self->outbound_.clear();
    self->TryFinishCloseOnIoThread();
  }
  DetachedTask DispatchRequest(std::shared_ptr<ServerConnection> self, FrameParseResult frame) {
    if (!self->runtime_->TryAcquireInFlight()) {
      auto response = std::make_shared<butil::IOBuf>();
      PackServerErrorResponse(frame, RPC_EOVERLOAD, "RPC server max_in_flight limit reached", *response);
      self->EnqueueResponse(std::move(response));
      co_return;
    }
    InFlightGuard in_flight {self->runtime_};
    if (auto* executor = self->context_.GetExecutor()) co_await resume_on(*executor);
    auto response = std::make_shared<butil::IOBuf>();
    if (!self->registry_.Dispatch(frame, *response)) { self->RequestClose(); co_return; }
    self->EnqueueResponse(std::move(response));
  }
  DetachedTask WriteFrame(std::shared_ptr<ServerConnection> self, OutboundFrame frame) {
    while (frame.buffer && !frame.buffer->empty() && self->running_.load(std::memory_order_acquire)) {
      const int written = co_await IOBufWriteAwaiter(self->context_, self->fd(), *frame.buffer, false);
      if (!self->running_.load(std::memory_order_acquire) || written <= 0) {
        self->running_.store(false, std::memory_order_release);
        if (const int socket = self->fd(); socket >= 0) shutdown(socket, SHUT_RDWR);
        break;
      }
      frame.buffer->pop_front(static_cast<std::size_t>(written));
    }
    self->writing_ = false;
    if (!self->outbound_.empty()) self->outbound_.pop_front();
    self->StartNextWriteOnIoThread();
    self->TryFinishCloseOnIoThread();
  }
  Context& context_;
  std::atomic<int> fd_ {-1};
  ServiceRegistry& registry_;
  std::shared_ptr<ServerRuntime> runtime_;
  const std::chrono::milliseconds idle_timeout_;
  MpscQueue<ServerCommand> commands_;
  butil::IOBuf recv_buffer_;
  std::deque<OutboundFrame> outbound_;
  std::atomic<bool> running_ {true};
  std::atomic<bool> released_ {false};
  std::atomic<uint64_t> idle_generation_ {0};
  std::atomic<uint64_t> idle_timer_id_ {0};
  bool receiver_started_ {false};
  bool receiver_exited_ {false};
  bool writing_ {false};
};

inline void ServerRuntime::TrackConnection(const std::shared_ptr<ServerConnection>& connection) {
  std::lock_guard<std::mutex> lock(connections_mu_); connections_.push_back(connection);
}
inline void ServerRuntime::ReleaseConnection(const std::shared_ptr<ServerConnection>& connection) {
  { std::lock_guard<std::mutex> lock(connections_mu_); std::erase(connections_, connection); }
  active_connections_.fetch_sub(1, std::memory_order_acq_rel);
  if (active_connections_.load(std::memory_order_acquire) == 0) CancelGracefulStopTimer();
}
inline void ServerRuntime::BeginGracefulStop() {
  accepting_.store(false, std::memory_order_release);
  if (active_connections_.load(std::memory_order_acquire) == 0) return;
  if (options_.graceful_stop_timeout.count() == 0) { ForceCloseLiveConnections(); return; }
  auto self = shared_from_this();
  const uint64_t timer = timer_keeper_.AddTimer(options_.graceful_stop_timeout, [self] { self->ForceCloseLiveConnections(); });
  const uint64_t old = graceful_stop_timer_id_.exchange(timer, std::memory_order_acq_rel);
  if (old != 0) timer_keeper_.CancelTimer(old);
}
inline void ServerRuntime::ForceCloseLiveConnections() {
  std::vector<std::shared_ptr<ServerConnection>> snapshot;
  { std::lock_guard<std::mutex> lock(connections_mu_); snapshot = connections_; }
  for (const auto& connection : snapshot) connection->RequestClose();
}
inline void ServerRuntime::CancelGracefulStopTimer() {
  if (const uint64_t timer = graceful_stop_timer_id_.exchange(0, std::memory_order_acq_rel); timer != 0) timer_keeper_.CancelTimer(timer);
}
inline DetachedTask handle_rpc_client(Context& ctx, int client_fd, ServiceRegistry& registry) {
  auto runtime = std::make_shared<ServerRuntime>(ctx, RpcServerOptions {});
  if (!runtime->TryAcquireConnection()) { close(client_fd); co_return; }
  auto connection = std::make_shared<ServerConnection>(ctx, client_fd, registry, runtime);
  runtime->TrackConnection(connection);
  connection->Start();
}
}  // namespace ant_server::rpc::detail
