#pragma once

#include <unistd.h>

#include <coroutine>
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
#include "ant_server/rpc/core/channel_io_driver.hpp"
#include "ant_server/rpc/error_code.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc {

struct RpcChannelOptions {
  int connect_timeout_ms {1000};
  int64_t timeout_ms {5000};
  bool tcp_no_delay {true};
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
// in core/channel_io_driver.hpp.
class RpcChannel : public google::protobuf::RpcChannel {
 public:
  RpcChannel() : state_(std::make_shared<detail::ChannelState>()) {}
  explicit RpcChannel(Context& context) : ctx_(&context), state_(std::make_shared<detail::ChannelState>()) {}
  ~RpcChannel() override { Close(); }
  RpcChannel(const RpcChannel&) = delete;
  RpcChannel& operator=(const RpcChannel&) = delete;

  int Init(const std::string& server_ip, int port, const RpcChannelOptions* options = nullptr) {
    butil::ip_t ip_value;
    return butil::str2ip(server_ip.c_str(), &ip_value) == 0 ? Init(butil::EndPoint(ip_value, port), options) : -1;
  }
  int Init(const char* ip, int port, const RpcChannelOptions* options = nullptr) {
    return Init(std::string(ip), port, options);
  }
  int Init(butil::EndPoint endpoint, const RpcChannelOptions* options = nullptr) {
    Close();
    if (options) {
      options_ = *options;
    }
    endpoint_ = endpoint;
    EnsureContext();
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      StopOwnedContext();
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
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
      close(fd);
      StopOwnedContext();
      return -1;
    }
    auto state = std::make_shared<detail::ChannelState>();
    {
      std::lock_guard<std::mutex> lock(state->close_mu);
      state->closed = false;
    }
    state->running.store(true, std::memory_order_release);
    auto driver = std::make_shared<detail::RpcChannelIoDriver>(*ctx_, state, fd);
    {
      std::lock_guard<std::mutex> lock(channel_mu_);
      state_ = std::move(state);
      driver_ = driver;
    }
    driver->Start();
    return 0;
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
    StopOwnedContext();
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
                                                response, task, CurrentContinuationTarget(), &task->stop_callback);
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
                                              response, &task, ContinuationTarget {}, &stop_callback);
    if (result.outcome == StartOutcome::kFailedInline) {
      task.run();
      return;
    }
    const int64_t timeout =
        rpc_controller && rpc_controller->TimeoutMs() > 0 ? rpc_controller->TimeoutMs() : options_.timeout_ms;
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
    const StartResult result = StartUnaryCall(service_name, method_name, &controller, nullptr, nullptr, &task,
                                              ContinuationTarget {}, &stop_callback, &request_body, &response_body);
    if (result.outcome == StartOutcome::kFailedInline) {
      task.run();
      return;
    }
    const int64_t timeout = controller.TimeoutMs() > 0 ? controller.TimeoutMs() : options_.timeout_ms;
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
    if (stop_token.stop_requested()) {
      SetInlineFailure(controller, "RPC call canceled before send", RPC_ECANCELED);
      return {};
    }
    if (!driver || !state->running.load(std::memory_order_acquire)) {
      SetInlineFailure(controller, "Connection closed");
      return {};
    }
    const uint64_t correlation_id = state->slots.AllocateSlot(task, response, controller, target, raw_response_body);
    if (correlation_id == 0) {
      SetInlineFailure(controller, "RPC slot table capacity exhausted", RPC_EOVERLOAD);
      return {};
    }
    if (controller) {
      controller->SetCorrelationId(correlation_id);
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
      meta.set_timeout_ms(controller->TimeoutMs());
      if (!controller->Headers().empty()) {
        *meta.mutable_headers() = controller->Headers();
      }
    }
    auto frame = std::make_shared<butil::IOBuf>();
    const butil::IOBuf* attachment = controller ? &controller->RequestAttachment() : nullptr;
    if (request) {
      PackRpcFrame(meta, request, attachment, *frame);
    } else if (raw_request_body) {
      PackRpcFrame(meta, *raw_request_body, attachment, *frame);
    } else {
      butil::IOBuf empty;
      PackRpcFrame(meta, empty, attachment, *frame);
    }
    const PublishOutcome publish = state->slots.PublishSlot(correlation_id);
    if (publish == PublishOutcome::kInvalid) {
      state->slots.DiscardArmingSlot(correlation_id);
      SetInlineFailure(controller, "Failed to publish RPC slot", RPC_EINTERNAL);
      return {};
    }
    // From PublishSlot onwards SlotTable may dispatch task on another thread;
    // do not read request, response, controller, or task below this line.
    if (publish != PublishOutcome::kInFlight) {
      return {correlation_id, StartOutcome::kResolvedBySlot};
    }
    driver->Enqueue({correlation_id, std::move(frame)});
    return {correlation_id, StartOutcome::kInFlight};
  }

  void EnsureContext() {
    if (ctx_) {
      return;
    }
    owned_ctx_ = std::make_unique<Context>();
    ctx_ = owned_ctx_.get();
    owned_io_thread_ = std::thread([context = ctx_] { context->Start(); });
  }
  void StopOwnedContext() {
    if (!owned_ctx_) {
      return;
    }
    owned_ctx_->Stop();
    if (owned_io_thread_.joinable()) {
      owned_io_thread_.join();
    }
    owned_ctx_.reset();
    ctx_ = nullptr;
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

  Context* ctx_ {nullptr};
  butil::EndPoint endpoint_;
  RpcChannelOptions options_;
  mutable std::mutex channel_mu_;
  std::shared_ptr<detail::ChannelState> state_;
  std::shared_ptr<detail::RpcChannelIoDriver> driver_;
  std::unique_ptr<Context> owned_ctx_;
  std::thread owned_io_thread_;
};

}  // namespace ant_server::rpc
