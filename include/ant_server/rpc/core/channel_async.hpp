#pragma once

#include <coroutine>
#include <string_view>

#include "ant_server/rpc/core/channel_core.hpp"

namespace ant_server::rpc {

// Coroutine-facing unary RPC adapter. It is intentionally separate from the
// protobuf-compatible channel core and connection lifecycle.
struct RpcCallAwaiter {
  RpcChannel& channel;
  std::string_view service_name;
  std::string_view method_name;
  RpcController* controller;
  const google::protobuf::Message* request;
  google::protobuf::Message* response;
  uint64_t correlation_id {0};

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) noexcept {
    correlation_id = channel.StartUnaryCall(service_name, method_name, controller, request, response, handle, nullptr);
  }
  void await_resume() noexcept {}
};

inline RpcCallAwaiter RpcChannel::CallAsync(const google::protobuf::MethodDescriptor* method, RpcController* controller,
                                            const google::protobuf::Message* request,
                                            google::protobuf::Message* response) {
  return {*this, method->service()->full_name(), method->name(), controller, request, response};
}

inline RpcCallAwaiter RpcChannel::CallAsync(std::string_view service_name, std::string_view method_name,
                                            RpcController* controller, const google::protobuf::Message* request,
                                            google::protobuf::Message* response) {
  return {*this, service_name, method_name, controller, request, response};
}

template <typename ResponseType>
Task<ResponseType> RpcChannel::Call(std::string_view service_name, std::string_view method_name,
                                    const google::protobuf::Message& request, RpcController* controller) {
  ResponseType response;
  co_await CallAsync(service_name, method_name, controller, &request, &response);
  co_return response;
}

}  // namespace ant_server::rpc
