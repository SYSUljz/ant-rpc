#pragma once

#include <unistd.h>

#include <coroutine>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

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
#include "ant_server/rpc/channel_io_driver.hpp"
#include "ant_server/rpc/controller.hpp"
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

class RpcChannel;
struct RpcCallAwaiter {
  RpcChannel& channel;
  std::string_view service_name;
  std::string_view method_name;
  RpcController* controller;
  const google::protobuf::Message* request;
  google::protobuf::Message* response;
  uint64_t correlation_id {0};
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) noexcept;
  void await_resume() noexcept {}
};

// Thread-safe API facade. Socket state and all IO-thread-only code live in
// RpcChannelIoDriver, making the ownership boundary explicit.
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
      SendRpc(method->service()->full_name(), method->name(), rpc_controller, request, response, nullptr, done);
      return;
    }
    absl::Notification notification;
    struct SyncClosure final : google::protobuf::Closure {
      explicit SyncClosure(absl::Notification& value) : notification(value) {}
      void Run() override { notification.Notify(); }
      absl::Notification& notification;
    } closure(notification);
    SendRpc(method->service()->full_name(), method->name(), rpc_controller, request, response, nullptr, &closure);
    const int64_t timeout =
        rpc_controller && rpc_controller->TimeoutMs() > 0 ? rpc_controller->TimeoutMs() : options_.timeout_ms;
    if (!notification.WaitForNotificationWithTimeout(absl::Milliseconds(timeout)) && rpc_controller) {
      rpc_controller->SetFailed(RPC_ETIMEOUT, "RPC call timed out");
    }
  }

  void CallMethod(std::string_view service_name, std::string_view method_name, RpcController& controller,
                  const butil::IOBuf& request_body, butil::IOBuf& response_body) {
    absl::Notification notification;
    struct SyncClosure final : google::protobuf::Closure {
      explicit SyncClosure(absl::Notification& value) : notification(value) {}
      void Run() override { notification.Notify(); }
      absl::Notification& notification;
    } closure(notification);
    SendRpc(service_name, method_name, &controller, nullptr, nullptr, nullptr, &closure, &request_body, &response_body);
    const int64_t timeout = controller.TimeoutMs() > 0 ? controller.TimeoutMs() : options_.timeout_ms;
    if (!notification.WaitForNotificationWithTimeout(absl::Milliseconds(timeout))) {
      controller.SetFailed(RPC_ETIMEOUT, "RPC call timed out");
    }
  }

  RpcCallAwaiter CallAsync(const google::protobuf::MethodDescriptor* method, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response) {
    return {*this, method->service()->full_name(), method->name(), controller, request, response};
  }
  RpcCallAwaiter CallAsync(std::string_view service_name, std::string_view method_name, RpcController* controller,
                           const google::protobuf::Message* request, google::protobuf::Message* response) {
    return {*this, service_name, method_name, controller, request, response};
  }
  template <typename ResponseType>
  Task<ResponseType> Call(std::string_view service_name, std::string_view method_name,
                          const google::protobuf::Message& request, RpcController* controller = nullptr) {
    ResponseType response;
    co_await CallAsync(service_name, method_name, controller, &request, &response);
    co_return response;
  }

  uint64_t SendRpc(std::string_view service_name, std::string_view method_name, RpcController* controller,
                   const google::protobuf::Message* request, google::protobuf::Message* response,
                   std::coroutine_handle<> handle, google::protobuf::Closure* done,
                   const butil::IOBuf* raw_request_body = nullptr, butil::IOBuf* raw_response_body = nullptr) {
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
    if (!driver || !state->running.load(std::memory_order_acquire)) {
      FailImmediately(controller, handle, done, "Connection closed");
      return 0;
    }
    const uint64_t correlation_id = state->slots.AllocateSlot(handle, response, controller, done, raw_response_body);
    if (correlation_id == 0) {
      FailImmediately(controller, handle, done, "RPC slot table capacity exhausted", RPC_EOVERLOAD);
      return 0;
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
    driver->Enqueue({correlation_id, std::move(frame)});
    return correlation_id;
  }

 private:
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
  static void FailImmediately(RpcController* controller, std::coroutine_handle<> handle,
                              google::protobuf::Closure* done, const std::string& message,
                              int error_code = RPC_ECONN_FAILED) {
    if (controller) {
      controller->SetFailed(error_code, message);
    }
    if (handle) {
      handle.resume();
    } else if (done) {
      done->Run();
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

inline void RpcCallAwaiter::await_suspend(std::coroutine_handle<> handle) noexcept {
  correlation_id = channel.SendRpc(service_name, method_name, controller, request, response, handle, nullptr);
}

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
