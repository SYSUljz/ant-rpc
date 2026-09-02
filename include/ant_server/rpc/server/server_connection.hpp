#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include "absl/synchronization/mutex.h"
#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/server/server_metrics.hpp"
#include "ant_server/rpc/server/server_options.hpp"
#include "ant_server/rpc/service_registry.hpp"
#include "ant_server/type.hpp"
#include "butil/iobuf.h"

namespace ant_server::rpc::detail {

class ServerConnection;

class ServerRuntime final : public std::enable_shared_from_this<ServerRuntime> {
 public:
  ServerRuntime(Context& context, RpcServerOptions options)
      : timer_keeper_(context.GetTimerKeeper()),
        options_(std::move(options)),
        metrics_(std::make_shared<ServerMetrics>()) {}
  bool TryAcquireConnection() noexcept {
    if (!accepting_.load(std::memory_order_acquire)) {
      metrics_->rejected_connections.Add();
      return false;
    }
    std::size_t current = active_connections_.load(std::memory_order_acquire);
    while (current < options_.max_connections) {
      if (active_connections_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        metrics_->accepted_connections.Add();
        metrics_->active_connections.Add(1);
        return true;
      }
    }
    metrics_->rejected_connections.Add();
    return false;
  }
  void TrackConnection(const std::shared_ptr<ServerConnection>& connection);
  void ReleaseConnection(const std::shared_ptr<ServerConnection>& connection);
  bool TryAcquireInFlight() noexcept {
    std::size_t current = in_flight_.load(std::memory_order_acquire);
    while (current < options_.max_in_flight) {
      if (in_flight_.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                           std::memory_order_acquire)) {
        metrics_->active_in_flight.Add(1);
        return true;
      }
    }
    return false;
  }
  void ReleaseInFlight() noexcept {
    in_flight_.fetch_sub(1, std::memory_order_acq_rel);
    metrics_->active_in_flight.Add(-1);
    NotifyDrained();
  }
  [[nodiscard]] bool IsAccepting() const noexcept { return accepting_.load(std::memory_order_acquire); }
  void BeginGracefulStop();
  void WaitForDrained() {
    absl::MutexLock lock(&drain_mu_);
    drain_mu_.Await(absl::Condition(this, &ServerRuntime::IsDrained));
  }
  const RpcServerOptions& options() const noexcept { return options_; }
  [[nodiscard]] ServerMetrics& metrics() noexcept { return *metrics_; }
  [[nodiscard]] const ServerMetrics& metrics() const noexcept { return *metrics_; }
  [[nodiscard]] std::shared_ptr<const ServerMetrics> metrics_handle() const noexcept { return metrics_; }

 private:
  struct InboundDoneClosure;

  void ForceCloseLiveConnections();
  void CancelGracefulStopTimer();
  bool IsDrained() const {
    return active_connections_.load(std::memory_order_acquire) == 0 && in_flight_.load(std::memory_order_acquire) == 0;
  }
  // Abseil's Await waiters are re-evaluated on unlock. The counters are
  // atomic hot-path state, so take this control lock only to publish a change.
  void NotifyDrained() { absl::MutexLock lock(&drain_mu_); }
  TimerKeeper& timer_keeper_;
  const RpcServerOptions options_;
  std::atomic<bool> accepting_ {true};
  std::atomic<std::size_t> active_connections_ {0};
  std::atomic<std::size_t> in_flight_ {0};
  std::atomic<uint64_t> graceful_stop_timer_id_ {0};
  std::shared_ptr<ServerMetrics> metrics_;
  absl::Mutex connections_mu_;
  std::vector<std::shared_ptr<ServerConnection>> connections_;
  absl::Mutex drain_mu_;
};

inline bool PackServerErrorResponse(const FrameParseResult& request, int error_code, const char* error_text,
                                    butil::IOBuf& response, std::size_t max_frame_bytes) {
  RpcMeta meta;
  meta.set_msg_type(RPC_RESPONSE);
  meta.set_correlation_id(request.meta.correlation_id());
  meta.set_service_name(request.meta.service_name());
  meta.set_method_name(request.meta.method_name());
  meta.set_error_code(error_code);
  meta.set_error_text(error_text);
  return PackRpcFrame(meta, nullptr, nullptr, response, max_frame_bytes);
}

// Per-accepted-fd state machine. Only its Context IO thread mutates the fd,
// receive buffer, inbound-call list, outbound queue and write state. Workers
// return completion through a typed ServerCommand.
class ServerConnection final : public IoCommandMailbox, public std::enable_shared_from_this<ServerConnection> {
 private:
  struct InboundCallState;

 public:
  struct OutboundFrame {
    butil::IOBuf buffer;
  };
  struct ServerCommand {
    enum class Type : uint8_t { kStart, kSendResponse, kCompleteInbound, kClose };
    Type type;
    OutboundFrame frame {};
    std::shared_ptr<InboundCallState> inbound_call {};
    bool close_after_completion {false};

    static ServerCommand Start() { return {Type::kStart, {}}; }
    static ServerCommand SendResponse(OutboundFrame frame) { return {Type::kSendResponse, std::move(frame)}; }
    static ServerCommand CompleteInbound(std::shared_ptr<InboundCallState> call, OutboundFrame frame,
                                         bool close_after_completion) {
      return {Type::kCompleteInbound, std::move(frame), std::move(call), close_after_completion};
    }
    static ServerCommand Close() { return {Type::kClose, {}}; }
  };

  ServerConnection(Context& context, int fd, ServiceRegistry& registry, std::shared_ptr<ServerRuntime> runtime)
      : context_(context),
        fd_(fd),
        registry_(registry),
        runtime_(std::move(runtime)),
        idle_timeout_(runtime_->options().idle_timeout) {}
  void Start() { PostCommand(ServerCommand::Start()); }
  void EnqueueResponse(butil::IOBuf response) {
    if (!response.empty()) {
      PostCommand(ServerCommand::SendResponse({std::move(response)}));
    }
  }
  void RequestClose() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
      PostCommand(ServerCommand::Close());
    }
  }
  int fd() const noexcept { return fd_.load(std::memory_order_acquire); }

  void DrainCommandsOnIoThread() override {
    commands_.Drain([this](ServerCommand&& command) {
      switch (command.type) {
        case ServerCommand::Type::kStart:
          StartOnIoThread();
          break;
        case ServerCommand::Type::kSendResponse:
          if (running_.load(std::memory_order_acquire) && !command.frame.buffer.empty()) {
            outbound_.push_back(std::move(command.frame));
            runtime_->metrics().responses_enqueued.Add();
            StartNextWriteOnIoThread();
          }
          break;
        case ServerCommand::Type::kCompleteInbound:
          CompleteInboundOnIoThread(std::move(command.inbound_call), std::move(command.frame),
                                    command.close_after_completion);
          break;
        case ServerCommand::Type::kClose:
          BeginCloseOnIoThread();
          break;
      }
    });
  }

 private:
  struct InboundDoneClosure;

  // Ownership hand-off from the IO state machine to one worker execution.
  // The IO thread never accesses request-owned state after moving it here.
  struct InboundCallState final : std::enable_shared_from_this<InboundCallState> {
    enum class CompletionState : uint8_t { kPending, kFinalizationScheduled, kFinalized };

    InboundCallState(std::weak_ptr<ServerConnection> connection, std::shared_ptr<ServerRuntime> runtime,
                     FrameParseResult frame, Executor& executor, std::size_t max_frame_bytes)
        : connection(std::move(connection)),
          runtime(std::move(runtime)),
          frame(std::move(frame)),
          executor(&executor),
          max_frame_bytes(max_frame_bytes) {}

    std::weak_ptr<ServerConnection> connection;
    std::shared_ptr<ServerRuntime> runtime;
    FrameParseResult frame;
    RpcMeta response_meta;
    google::protobuf::Service* service {nullptr};
    const google::protobuf::MethodDescriptor* method {nullptr};
    std::unique_ptr<google::protobuf::Message> request;
    std::unique_ptr<google::protobuf::Message> response;
    std::unique_ptr<RpcController> controller;
    std::unique_ptr<InboundDoneClosure> done;
    Executor* executor;
    const std::size_t max_frame_bytes;
    std::atomic<CompletionState> completion {CompletionState::kPending};
    const std::chrono::steady_clock::time_point received_at {std::chrono::steady_clock::now()};
  };

  // The callback is valid until its first successful completion has been
  // observed by the connection. A conforming protobuf service calls Run()
  // once; duplicate calls before retirement are reduced to a no-op by the
  // InboundCallState CAS.
  struct InboundDoneClosure final : google::protobuf::Closure {
    explicit InboundDoneClosure(std::weak_ptr<InboundCallState> call) : call_(std::move(call)) {}
    void Run() override;

   private:
    std::weak_ptr<InboundCallState> call_;
  };

  static void FinalizeInboundCall(std::shared_ptr<InboundCallState> call) noexcept;

  struct ServerFinalizeTask final : TaskNode {
    explicit ServerFinalizeTask(std::shared_ptr<InboundCallState> call) : call(std::move(call)) {
      execute = [](TaskNode* task) noexcept {
        auto* self = static_cast<ServerFinalizeTask*>(task);
        ServerConnection::FinalizeInboundCall(std::move(self->call));
        delete self;
      };
    }

    std::shared_ptr<InboundCallState> call;
  };

  static void ScheduleInboundFinalization(std::shared_ptr<InboundCallState> call) {
    call->executor->schedule(new ServerFinalizeTask(std::move(call)));
  }

  // A concrete task rather than an erased closure makes the server's IO to
  // worker transition visible in the type system and in profiles.
  struct ServerDispatchTask final : TaskNode {
    ServerDispatchTask(std::shared_ptr<InboundCallState> call, ServiceRegistry& registry)
        : call(std::move(call)), registry(&registry) {
      execute = [](TaskNode* task) noexcept {
        auto* self = static_cast<ServerDispatchTask*>(task);
        ServerConnection::RunInboundCall(std::move(self->call), *self->registry);
        delete self;
      };
    }

    std::shared_ptr<InboundCallState> call;
    ServiceRegistry* registry;
  };
  void PostCommand(ServerCommand command) {
    commands_.Push(std::move(command));
    context_.Notify(shared_from_this());
  }
  void CompleteInbound(std::shared_ptr<InboundCallState> call, butil::IOBuf response, bool close_after_completion) {
    PostCommand(ServerCommand::CompleteInbound(std::move(call), {std::move(response)}, close_after_completion));
  }
  void CompleteInboundOnIoThread(std::shared_ptr<InboundCallState> call, OutboundFrame response,
                                 bool close_after_completion) {
    if (call) {
      std::erase(inbound_calls_, call);
    }
    if (close_after_completion) {
      running_.store(false, std::memory_order_release);
      BeginCloseOnIoThread();
      return;
    }
    if (running_.load(std::memory_order_acquire) && !response.buffer.empty()) {
      outbound_.push_back(std::move(response));
      runtime_->metrics().responses_enqueued.Add();
      StartNextWriteOnIoThread();
    }
    TryFinishCloseOnIoThread();
  }
  void StartOnIoThread() {
    if (!running_.load(std::memory_order_acquire)) {
      receiver_exited_ = true;
      TryFinishCloseOnIoThread();
      return;
    }
    ArmIdleTimerOnIoThread();
    ReceiveLoop(shared_from_this());
  }
  void BeginCloseOnIoThread() {
    CancelIdleTimer();
    if (const int socket = fd(); socket >= 0) {
      context_.UseService<IOuringSocketService>().SubmitShutdown(socket, SHUT_RDWR, /*is_fixed=*/true);
    }
    outbound_.clear();
    if (!receiver_started_) {
      receiver_exited_ = true;
    }
    TryFinishCloseOnIoThread();
  }
  void TryFinishCloseOnIoThread() {
    if (running_.load(std::memory_order_acquire) || !receiver_exited_ || writing_) {
      return;
    }
    CancelIdleTimer();
    if (const int socket = fd_.exchange(-1, std::memory_order_acq_rel); socket >= 0) {
      context_.UseService<IOuringSocketService>().SubmitClose(socket, nullptr, /*is_fixed=*/true);
    }
    if (!inbound_calls_.empty()) {
      return;
    }
    if (!released_.exchange(true, std::memory_order_acq_rel)) {
      runtime_->ReleaseConnection(shared_from_this());
    }
  }
  void StartNextWriteOnIoThread() {
    if (writing_ || outbound_.empty() || !running_.load(std::memory_order_acquire)) {
      return;
    }
    writing_ = true;
    OutboundFrame frame = std::move(outbound_.front());
    outbound_.pop_front();
    WriteFrame(shared_from_this(), std::move(frame));
  }
  void ArmIdleTimerOnIoThread() {
    if (idle_timeout_.count() <= 0 || !running_.load(std::memory_order_acquire)) {
      return;
    }
    const uint64_t generation = idle_generation_.fetch_add(1, std::memory_order_acq_rel) + 1;
    CancelIdleTimer();
    auto self = shared_from_this();
    const uint64_t timer = context_.GetTimerKeeper().AddTimer(idle_timeout_, [self, generation] {
      if (self->idle_generation_.load(std::memory_order_acquire) == generation) {
        self->RequestClose();
      }
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
        if (frame.status == FrameParseStatus::NEED_MORE_DATA) {
          break;
        }
        if (frame.status != FrameParseStatus::SUCCESS) {
          self->runtime_->metrics().protocol_errors.Add();
          // A protocol violation is terminal for this TCP connection. Do not
          // wait for another read CQE to make that visible to the peer.
          self->running_.store(false, std::memory_order_release);
          if (const int socket = self->fd(); socket >= 0) {
            self->context_.UseService<IOuringSocketService>().SubmitShutdown(socket, SHUT_RDWR, /*is_fixed=*/true);
          }
          break;
        }
        if (frame.meta.msg_type() != RPC_REQUEST) {
          self->runtime_->metrics().protocol_errors.Add();
          // A peer may not send a response or an unspecified message type to
          // a server connection. Treat it as a terminal protocol violation.
          self->running_.store(false, std::memory_order_release);
          if (const int socket = self->fd(); socket >= 0) {
            self->context_.UseService<IOuringSocketService>().SubmitShutdown(socket, SHUT_RDWR, /*is_fixed=*/true);
          }
          break;
        }
        self->recv_buffer_.pop_front(frame.total_frame_bytes);
        self->HandleRequest(std::move(frame));
      }
      if (!self->running_.load(std::memory_order_acquire)) {
        break;
      }
      const int bytes = co_await ReadAwaiter(self->context_, self->fd(), self->recv_buffer_, /*is_fixed=*/true);
      if (bytes <= 0) {
        break;
      }
      self->ArmIdleTimerOnIoThread();
    }
    self->running_.store(false, std::memory_order_release);
    self->receiver_exited_ = true;
    self->outbound_.clear();
    self->TryFinishCloseOnIoThread();
  }
  // The framework's request entry point. It is called only after a complete,
  // valid RPC_REQUEST frame has been decoded on the connection's IO owner.
  // All generic server request accounting begins here; individual protobuf
  // services do not need to add their own transport-level instrumentation.
  void HandleRequest(FrameParseResult frame) {
    runtime_->metrics().requests_received.Add();
    if (!runtime_->IsAccepting()) {
      runtime_->metrics().requests_rejected_overload.Add();
      butil::IOBuf response;
      if (PackServerErrorResponse(frame, RPC_EOVERLOAD, "RPC server is stopping", response,
                                  runtime_->options().max_frame_bytes)) {
        EnqueueResponse(std::move(response));
      } else {
        RequestClose();
      }
      return;
    }
    if (!runtime_->TryAcquireInFlight()) {
      runtime_->metrics().requests_rejected_overload.Add();
      butil::IOBuf response;
      if (PackServerErrorResponse(frame, RPC_EOVERLOAD, "RPC server max_in_flight limit reached", response,
                                  runtime_->options().max_frame_bytes)) {
        EnqueueResponse(std::move(response));
      } else {
        RequestClose();
      }
      return;
    }

    auto* executor = context_.GetExecutor();
    if (executor == nullptr) {
      runtime_->ReleaseInFlight();
      RequestClose();
      return;
    }
    auto call = std::make_shared<InboundCallState>(weak_from_this(), runtime_, std::move(frame), *executor,
                                                   runtime_->options().max_frame_bytes);
    runtime_->metrics().calls_started.Add();
    inbound_calls_.push_back(call);
    executor->schedule(new ServerDispatchTask(std::move(call), registry_));
  }

  static void RunInboundCall(std::shared_ptr<InboundCallState> call, ServiceRegistry& registry) noexcept {
    call->response_meta.set_msg_type(RPC_RESPONSE);
    call->response_meta.set_correlation_id(call->frame.meta.correlation_id());
    call->response_meta.set_service_name(call->frame.meta.service_name());
    call->response_meta.set_method_name(call->frame.meta.method_name());

    const auto lookup = registry.FindMethod(call->frame.meta.service_name(), call->frame.meta.method_name());
    if (!lookup.service) {
      call->response_meta.set_error_code(RPC_ENOSERVICE);
      call->response_meta.set_error_text("Service not found: " + call->frame.meta.service_name());
    } else if (!lookup.method) {
      call->response_meta.set_error_code(RPC_ENOMETHOD);
      call->response_meta.set_error_text("Method not found: " + call->frame.meta.method_name());
    } else {
      call->service = lookup.service;
      call->method = lookup.method;
      call->request.reset(call->service->GetRequestPrototype(call->method).New());
      call->response.reset(call->service->GetResponsePrototype(call->method).New());
      if (!call->frame.body_iobuf.empty()) {
        butil::IOBufAsZeroCopyInputStream input(call->frame.body_iobuf);
        if (!call->request->ParseFromZeroCopyStream(&input)) {
          call->response_meta.set_error_code(RPC_EINVALID_DATA);
          call->response_meta.set_error_text("Failed to parse request protobuf");
        }
      }
      if (call->response_meta.error_code() == RPC_SUCCESS) {
        call->controller = std::make_unique<RpcController>();
        call->controller->SetCorrelationId(call->frame.meta.correlation_id());
        call->controller->SetLogId(call->frame.meta.log_id());
        call->controller->SetTimeoutMs(call->frame.meta.timeout_ms());
        if (!call->frame.meta.headers().empty()) {
          call->controller->MutableRequestHeaders().swap(*call->frame.meta.mutable_headers());
        }
        if (!call->frame.attachment_iobuf.empty()) {
          call->controller->RequestAttachment() = std::move(call->frame.attachment_iobuf);
        }
        call->done = std::make_unique<InboundDoneClosure>(call);
        call->service->CallMethod(call->method, call->controller.get(), call->request.get(), call->response.get(),
                                  call->done.get());
        return;
      }
    }

    call->completion.store(InboundCallState::CompletionState::kFinalizationScheduled, std::memory_order_release);
    ScheduleInboundFinalization(std::move(call));
  }
  DetachedTask WriteFrame(std::shared_ptr<ServerConnection> self, OutboundFrame frame) {
    while (!frame.buffer.empty() && self->running_.load(std::memory_order_acquire)) {
      const int written = co_await IOBufWriteAwaiter(self->context_, self->fd(), frame.buffer, /*is_fixed=*/true);
      if (!self->running_.load(std::memory_order_acquire) || written <= 0) {
        self->runtime_->metrics().write_errors.Add();
        self->running_.store(false, std::memory_order_release);
        if (const int socket = self->fd(); socket >= 0) {
          self->context_.UseService<IOuringSocketService>().SubmitShutdown(socket, SHUT_RDWR, /*is_fixed=*/true);
        }
        break;
      }
      frame.buffer.pop_front(static_cast<std::size_t>(written));
    }
    self->writing_ = false;
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
  std::vector<std::shared_ptr<InboundCallState>> inbound_calls_;
  std::deque<OutboundFrame> outbound_;
  std::atomic<bool> running_ {true};
  std::atomic<bool> released_ {false};
  std::atomic<uint64_t> idle_generation_ {0};
  std::atomic<uint64_t> idle_timer_id_ {0};
  bool receiver_started_ {false};
  bool receiver_exited_ {false};
  bool writing_ {false};
};

inline void ServerConnection::InboundDoneClosure::Run() {
  auto call = call_.lock();
  if (!call) {
    return;
  }
  auto expected = InboundCallState::CompletionState::kPending;
  if (!call->completion.compare_exchange_strong(expected, InboundCallState::CompletionState::kFinalizationScheduled,
                                                std::memory_order_acq_rel, std::memory_order_acquire)) {
    return;
  }
  ServerConnection::ScheduleInboundFinalization(std::move(call));
}

inline void ServerConnection::FinalizeInboundCall(std::shared_ptr<InboundCallState> call) noexcept {
  butil::IOBuf wire_response;
  bool close_after_completion = false;

  if (call->controller) {
    call->response_meta.set_error_code(call->controller->ErrorCode());
    call->response_meta.set_error_text(call->controller->ErrorText());
    if (!call->controller->ResponseHeaders().empty()) {
      call->response_meta.mutable_headers()->swap(call->controller->MutableResponseHeaders());
    }
    const butil::IOBuf* attachment =
        !call->controller->ResponseAttachment().empty() ? &call->controller->ResponseAttachment() : nullptr;
    if (!PackRpcFrame(call->response_meta, call->response.get(), attachment, wire_response, call->max_frame_bytes)) {
      close_after_completion = true;
      wire_response.clear();
    }
  } else if (!PackRpcFrame(call->response_meta, nullptr, nullptr, wire_response, call->max_frame_bytes)) {
    close_after_completion = true;
    wire_response.clear();
  }

  auto& metrics = call->runtime->metrics();
  metrics.calls_completed.Add();
  if (call->response_meta.error_code() != RPC_SUCCESS || close_after_completion) {
    metrics.call_errors.Add();
  }
  metrics.request_latency.Record(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - call->received_at));

  call->completion.store(InboundCallState::CompletionState::kFinalized, std::memory_order_release);
  auto runtime = call->runtime;
  if (auto connection = call->connection.lock()) {
    connection->CompleteInbound(std::move(call), std::move(wire_response), close_after_completion);
  }
  runtime->ReleaseInFlight();
}

inline void ServerRuntime::TrackConnection(const std::shared_ptr<ServerConnection>& connection) {
  absl::MutexLock lock(&connections_mu_);
  connections_.push_back(connection);
}
inline void ServerRuntime::ReleaseConnection(const std::shared_ptr<ServerConnection>& connection) {
  {
    absl::MutexLock lock(&connections_mu_);
    std::erase(connections_, connection);
  }
  active_connections_.fetch_sub(1, std::memory_order_acq_rel);
  metrics_->active_connections.Add(-1);
  metrics_->closed_connections.Add();
  if (active_connections_.load(std::memory_order_acquire) == 0) {
    CancelGracefulStopTimer();
  }
  NotifyDrained();
}
inline void ServerRuntime::BeginGracefulStop() {
  accepting_.store(false, std::memory_order_release);
  if (active_connections_.load(std::memory_order_acquire) == 0) {
    return;
  }
  if (options_.graceful_stop_timeout.count() == 0) {
    ForceCloseLiveConnections();
    return;
  }
  auto self = shared_from_this();
  const uint64_t timer =
      timer_keeper_.AddTimer(options_.graceful_stop_timeout, [self] { self->ForceCloseLiveConnections(); });
  const uint64_t old = graceful_stop_timer_id_.exchange(timer, std::memory_order_acq_rel);
  if (old != 0) {
    timer_keeper_.CancelTimer(old);
  }
}
inline void ServerRuntime::ForceCloseLiveConnections() {
  std::vector<std::shared_ptr<ServerConnection>> snapshot;
  {
    absl::MutexLock lock(&connections_mu_);
    snapshot = connections_;
  }
  for (const auto& connection : snapshot) {
    connection->RequestClose();
  }
}
inline void ServerRuntime::CancelGracefulStopTimer() {
  if (const uint64_t timer = graceful_stop_timer_id_.exchange(0, std::memory_order_acq_rel); timer != 0) {
    timer_keeper_.CancelTimer(timer);
  }
}
inline DetachedTask handle_rpc_client(Context& ctx, int client_fd, ServiceRegistry& registry) {
  auto runtime = std::make_shared<ServerRuntime>(ctx, RpcServerOptions {});
  if (!runtime->TryAcquireConnection()) {
    close(client_fd);
    co_return;
  }
  auto connection = std::make_shared<ServerConnection>(ctx, client_fd, registry, runtime);
  runtime->TrackConnection(connection);
  connection->Start();
}
}  // namespace ant_server::rpc::detail
