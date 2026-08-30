#pragma once

#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include <arpa/inet.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "absl/synchronization/notification.h"
#include "ant_server/context/context.hpp"
#include "ant_server/coroutine/task.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/io_driver/channel_io_driver.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/rpc/runtime.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "ant_server/scheduler/timer_keeper.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc {

struct RpcChannelOptions {
  std::chrono::milliseconds connect_timeout {1000};
  std::chrono::milliseconds default_rpc_timeout {5000};
  std::size_t max_frame_bytes {kDefaultMaxRpcFrameBytes};
  std::size_t max_in_flight {65536};
  std::size_t max_outbound_bytes {16U * 1024U * 1024U};
  bool tcp_no_delay {true};

  [[nodiscard]] bool IsValid() const noexcept {
    return connect_timeout.count() > 0 && connect_timeout.count() <= std::numeric_limits<int>::max() &&
           default_rpc_timeout.count() > 0 && max_frame_bytes >= kRpcHeaderBytes && max_in_flight > 0 &&
           max_in_flight <= 65536 && max_outbound_bytes >= kRpcHeaderBytes;
  }
};
using ChannelOptions = RpcChannelOptions;

struct RpcCallAwaiter;

namespace detail {

// Adapts protobuf's callback-shaped public API to the scheduler's TaskNode.
// It belongs to the channel facade, rather than SlotTable: the table only
// manages generic continuations and must not depend on protobuf Closure.
struct ClosureTask final : TaskNode {
  google::protobuf::Closure* done {nullptr};
  bool heap_allocated {false};
  SlotStopCallback stop_callback;

  ClosureTask() noexcept {
    execute = [](TaskNode* self) noexcept {
      auto* node = static_cast<ClosureTask*>(self);
      auto* closure = node->done;
      const bool owns_self = node->heap_allocated;
      if (closure) {
        closure->Run();
      }
      if (owns_self) {
        delete node;
      }
    };
  }

  explicit ClosureTask(google::protobuf::Closure* closure, bool heap = false) noexcept : ClosureTask() {
    done = closure;
    heap_allocated = heap;
  }
};

}  // namespace detail

// StartUnaryCall never directly resumes a coroutine. SlotTable owns all
// continuation dispatch, including a pending resolution delivered by PublishSlot.
// The caller uses this result to decide whether it may continue inline or must remain suspended.
enum class StartOutcome : uint8_t { kInFlight, kFailedInline, kResolvedBySlot };

struct StartResult {
  uint64_t correlation_id {0};
  StartOutcome outcome {StartOutcome::kFailedInline};
};

// Thread-safe protobuf-compatible facade. IO-thread-only socket state lives
// in io_driver/channel_io_driver.hpp.
class RpcChannel : public google::protobuf::RpcChannel {
 public:
  // The normal application-facing API. Init() binds this channel to one IO
  // Context selected by the process Runtime; Context is never exposed to the
  // caller.
  RpcChannel() : state_(std::make_shared<detail::ChannelState>()) {}

  // Advanced/testing API. The caller owns the Context and must keep its
  // Scheduler alive until this channel has been closed or destroyed.
  explicit RpcChannel(Context& context)
      : ctx_(&context), timer_keeper_(&context.GetTimerKeeper()), state_(std::make_shared<detail::ChannelState>()) {}
  ~RpcChannel() override { Close(); }
  RpcChannel(const RpcChannel&) = delete;
  RpcChannel& operator=(const RpcChannel&) = delete;

  // bRPC-style endpoint API for normal application code.
  int Init(const std::string& server_addr_and_port, const RpcChannelOptions* options = nullptr) {
    butil::EndPoint endpoint;
    return butil::str2endpoint(server_addr_and_port.c_str(), &endpoint) == 0 ? Init(endpoint, options) : -1;
  }
  int Init(const char* server_addr_and_port, const RpcChannelOptions* options = nullptr) {
    return server_addr_and_port ? Init(std::string(server_addr_and_port), options) : -1;
  }
  int Init(const std::string& server_ip, int port, const RpcChannelOptions* options = nullptr) {
    butil::ip_t ip_value;
    return butil::str2ip(server_ip.c_str(), &ip_value) == 0 ? Init(butil::EndPoint(ip_value, port), options) : -1;
  }
  int Init(const char* ip, int port, const RpcChannelOptions* options = nullptr) {
    return Init(std::string(ip), port, options);
  }
  int Init(butil::EndPoint endpoint, const RpcChannelOptions* options = nullptr) {
    const RpcChannelOptions requested_options = options ? *options : RpcChannelOptions {};
    if (!requested_options.IsValid()) {
      return -1;
    }
    Close();
    if (!BindDefaultRuntime()) {
      return -1;
    }
    if (!ctx_->GetScheduler().IsRunning()) {
      ReleaseDefaultRuntimeBinding();
      return -1;
    }
    options_ = requested_options;
    endpoint_ = endpoint;
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      ReleaseDefaultRuntimeBinding();
      return -1;
    }
    if (options_.tcp_no_delay) {
      int enabled = 1;
      setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
    }
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_port = htons(endpoint_.port);
    address.sin_addr = endpoint_.ip;
    auto state = std::make_shared<detail::ChannelState>();
    {
      std::lock_guard<std::mutex> lock(state->close_mu);
      state->closed = false;
    }
    state->running.store(true, std::memory_order_release);
    auto driver = std::make_shared<detail::RpcChannelIoDriver>(
        *ctx_, state, fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address), *timer_keeper_,
        options_.connect_timeout, options_.max_frame_bytes, options_.max_outbound_bytes);
    {
      std::lock_guard<std::mutex> lock(channel_mu_);
      // Init still waits on this local shared_ptr for the asynchronous connect
      // result, so Channel and this compatibility bridge must co-own it.
      state_ = state;
      driver_ = driver;
    }
    driver->Start();
    std::unique_lock<std::mutex> lock(state->connect_mu);
    state->connect_cv.wait(lock, [&state] { return state->connect_finished; });
    if (state->connect_result == 0) {
      return 0;
    }
    lock.unlock();
    Close();
    return -1;
  }

  void Close() {
    std::shared_ptr<detail::RpcChannelIoDriver> driver;
    {
      std::lock_guard<std::mutex> lock(channel_mu_);
      driver = driver_;
    }
    if (driver) {
      driver->RequestClose();
      driver->WaitClosed();
      std::lock_guard<std::mutex> lock(channel_mu_);
      if (driver_ == driver) {
        driver_.reset();
      }
    }
    ReleaseDefaultRuntimeBinding();
  }

  int fd() const noexcept {
    std::lock_guard<std::mutex> lock(channel_mu_);
    return driver_ ? driver_->fd() : -1;
  }
  SlotTable<65536>& slot_table() { return state_->slots; }

  void CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                  const google::protobuf::Message* request, google::protobuf::Message* response,
                  google::protobuf::Closure* done) override {
    auto* rpc_controller = dynamic_cast<RpcController*>(controller);
    if (controller && !rpc_controller) {
      controller->SetFailed("RpcChannel requires ant_server::rpc::RpcController");
      if (done) {
        done->Run();
      }
      return;
    }
    if (done) {
      auto* task = new detail::ClosureTask(done, true);
      const StartResult result = StartUnaryCall(method->service()->full_name(), method->name(), rpc_controller, request,
                                                response, task, CurrentCallContinuationTarget(), &task->stop_callback);
      if (result.outcome == StartOutcome::kFailedInline) {
        task->run();
      }
      return;
    }
    absl::Notification notification;
    struct SyncClosure final : google::protobuf::Closure {
      explicit SyncClosure(absl::Notification& value) : notification(value) {}
      void Run() override { notification.Notify(); }
      absl::Notification& notification;
    } closure(notification);
    detail::ClosureTask task(&closure);
    SlotStopCallback stop_callback;
    const StartResult result = StartUnaryCall(method->service()->full_name(), method->name(), rpc_controller, request,
                                              response, &task, CurrentCallContinuationTarget(), &stop_callback);
    if (result.outcome == StartOutcome::kFailedInline) {
      task.run();
      return;
    }
    const int64_t timeout = rpc_controller && rpc_controller->TimeoutMs() > 0 ? rpc_controller->TimeoutMs()
                                                                              : options_.default_rpc_timeout.count();
    AwaitBlockingCall(state_, result.correlation_id, notification, timeout);
  }

  void CallMethod(std::string_view service_name, std::string_view method_name, RpcController& controller,
                  const butil::IOBuf& request_body, butil::IOBuf& response_body) {
    absl::Notification notification;
    struct SyncClosure final : google::protobuf::Closure {
      explicit SyncClosure(absl::Notification& value) : notification(value) {}
      void Run() override { notification.Notify(); }
      absl::Notification& notification;
    } closure(notification);
    detail::ClosureTask task(&closure);
    SlotStopCallback stop_callback;
    const StartResult result =
        StartUnaryCall(service_name, method_name, &controller, nullptr, nullptr, &task, CurrentCallContinuationTarget(),
                       &stop_callback, &request_body, &response_body);
    if (result.outcome == StartOutcome::kFailedInline) {
      task.run();
      return;
    }
    const int64_t timeout = controller.TimeoutMs() > 0 ? controller.TimeoutMs() : options_.default_rpc_timeout.count();
    AwaitBlockingCall(state_, result.correlation_id, notification, timeout);
  }

  RpcCallAwaiter CallAsync(const google::protobuf::MethodDescriptor* method, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response);
  RpcCallAwaiter CallAsync(std::string_view service_name, std::string_view method_name, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response);
  template <typename ResponseType>
  Task<ResponseType> Call(std::string_view service_name, std::string_view method_name,
                          const google::protobuf::Message& request, RpcController* controller = nullptr);

 private:
  friend struct RpcCallAwaiter;

  // Before Init() has bound a Context, StartUnaryCall fails before it allocates
  // a slot, so this empty target can never reach completion dispatch.
  ContinuationTarget CurrentCallContinuationTarget() const noexcept {
    if (ctx_ && ctx_->GetExecutor()) {
      return CurrentContinuationTarget(*ctx_->GetExecutor());
    }
    return {};
  }

  StartResult StartUnaryCall(std::string_view service_name, std::string_view method_name, RpcController* controller,
                             const google::protobuf::Message* request, google::protobuf::Message* response,
                             TaskNode* task, ContinuationTarget target, SlotStopCallback* stop_callback,
                             const butil::IOBuf* raw_request_body = nullptr,
                             butil::IOBuf* raw_response_body = nullptr) {
    std::shared_ptr<detail::ChannelState> state;
    std::shared_ptr<detail::RpcChannelIoDriver> driver;
    {
      std::lock_guard<std::mutex> lock(channel_mu_);
      state = state_;
      driver = driver_;
    }
    if (controller) {
      controller->RecordStart();
    }
    const std::stop_token stop_token = controller ? controller->GetStopToken() : std::stop_token {};
    if (stop_token.stop_requested() || (controller && controller->IsCanceled())) {
      SetInlineFailure(controller, "RPC call canceled before send", RPC_ECANCELED);
      return {};
    }
    if (!driver || !state->running.load(std::memory_order_acquire)) {
      SetInlineFailure(controller, "Connection closed");
      return {};
    }
    const uint64_t correlation_id =
        state->slots.AllocateSlot(task, response, controller, target, raw_response_body, options_.max_in_flight);
    if (correlation_id == 0) {
      SetInlineFailure(controller, "RPC max_in_flight limit reached", RPC_EOVERLOAD);
      return {};
    }
    if (controller) {
      controller->SetCorrelationId(correlation_id);
      if (!controller->BindActiveSlot(state, correlation_id)) {
        state->slots.DiscardArmingSlot(correlation_id);
        SetInlineFailure(controller, "RpcController is already bound to an active RPC", RPC_EINTERNAL);
        return {};
      }
    }
    if (stop_callback && stop_token.stop_possible()) {
      // A stop request may invoke this synchronously. SlotTable records it as
      // CANCEL_PENDING while ARMING; PublishSlot() performs the completion.
      stop_callback->emplace(stop_token, state->slots.MakeCancelFn(correlation_id));
    }
    RpcMeta meta;
    meta.set_msg_type(RPC_REQUEST);
    meta.set_correlation_id(correlation_id);
    meta.set_service_name(std::string(service_name));
    meta.set_method_name(std::string(method_name));
    if (controller) {
      meta.set_log_id(controller->LogId());
      meta.set_timeout_ms(controller->TimeoutMs() > 0 ? controller->TimeoutMs() : options_.default_rpc_timeout.count());
      if (!controller->Headers().empty()) {
        *meta.mutable_headers() = controller->Headers();
      }
    }
    auto frame = std::make_shared<butil::IOBuf>();
    const butil::IOBuf* attachment = controller ? &controller->RequestAttachment() : nullptr;
    bool packed = false;
    if (request) {
      packed = PackRpcFrame(meta, request, attachment, *frame, options_.max_frame_bytes);
    } else if (raw_request_body) {
      packed = PackRpcFrame(meta, *raw_request_body, attachment, *frame, options_.max_frame_bytes);
    } else {
      butil::IOBuf empty;
      packed = PackRpcFrame(meta, empty, attachment, *frame, options_.max_frame_bytes);
    }
    if (!packed) {
      return ResolveArmingFailure(state, correlation_id, RPC_EINVALID_DATA,
                                  "RPC request frame exceeds max_frame_bytes");
    }
    const std::size_t frame_bytes = frame->size();
    if (!driver->TryReserveOutboundBytes(frame_bytes)) {
      return ResolveArmingFailure(state, correlation_id, RPC_EOVERLOAD, "RPC max_outbound_bytes limit reached");
    }
    const PublishOutcome publish = state->slots.PublishSlot(correlation_id);
    if (publish == PublishOutcome::kInvalid) {
      driver->ReleaseReservedOutboundBytes(frame_bytes);
      state->slots.DiscardArmingSlot(correlation_id);
      SetInlineFailure(controller, "Failed to publish RPC slot", RPC_EINTERNAL);
      return {};
    }
    // From PublishSlot onwards SlotTable may dispatch task on another thread;
    // do not read request, response, controller, or task below this line.
    if (publish != PublishOutcome::kInFlight) {
      driver->ReleaseReservedOutboundBytes(frame_bytes);
      return {correlation_id, StartOutcome::kResolvedBySlot};
    }
    driver->EnqueueReserved({correlation_id, std::move(frame)});
    return {correlation_id, StartOutcome::kInFlight};
  }

  // Local failures can arrive after a stop callback has changed ARMING into a
  // pending terminal state. Publishing first lets SlotTable choose that winner
  // safely; otherwise this local error becomes an ordinary slot failure.
  static StartResult ResolveArmingFailure(const std::shared_ptr<detail::ChannelState>& state, uint64_t correlation_id,
                                          int error_code, const std::string& message) {
    const PublishOutcome publish = state->slots.PublishSlot(correlation_id);
    if (publish == PublishOutcome::kInFlight) {
      state->slots.FailSlot(correlation_id, error_code, message);
    }
    return {correlation_id, StartOutcome::kResolvedBySlot};
  }

  static void SetInlineFailure(RpcController* controller, const std::string& message,
                               int error_code = RPC_ECONN_FAILED) {
    if (controller) {
      controller->SetFailed(error_code, message);
    }
  }

  static void AwaitBlockingCall(const std::shared_ptr<detail::ChannelState>& state, uint64_t correlation_id,
                                absl::Notification& notification, int64_t timeout_ms) {
    if (notification.WaitForNotificationWithTimeout(absl::Milliseconds(timeout_ms))) {
      return;
    }
    // If timeout loses to response/cancellation, wait until that winner has
    // dispatched the stack-owned notification task before returning.
    if (!state->slots.TimeoutSlot(correlation_id)) {
      notification.WaitForNotification();
    }
  }

  bool BindDefaultRuntime() {
    if (ctx_ != nullptr) {
      return true;
    }
    runtime_ = detail::AcquireDefaultRuntime();
    if (!runtime_ || !runtime_->IsRunning()) {
      runtime_.reset();
      return false;
    }
    ctx_ = &runtime_->AcquireChannelContext();
    timer_keeper_ = &ctx_->GetTimerKeeper();
    return true;
  }

  void ReleaseDefaultRuntimeBinding() {
    if (runtime_) {
      runtime_.reset();
      ctx_ = nullptr;
      timer_keeper_ = nullptr;
    }
  }

  Context* ctx_ {nullptr};
  butil::EndPoint endpoint_;
  RpcChannelOptions options_;
  mutable std::mutex channel_mu_;
  std::shared_ptr<detail::ChannelState> state_;
  std::shared_ptr<detail::RpcChannelIoDriver> driver_;
  TimerKeeper* timer_keeper_ {nullptr};
  std::shared_ptr<Runtime> runtime_;
};

}  // namespace ant_server::rpc
