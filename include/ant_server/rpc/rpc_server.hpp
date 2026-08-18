#pragma once

#include <liburing.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include <google/protobuf/service.h>

#include "ant_server/awaiter/resume_on.hpp"
#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/rpc/protocol.hpp"
#include "ant_server/rpc/service_registry.hpp"
#include "ant_server/type.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc {

inline DetachedTask handle_rpc_client(Context& ctx, int client_fd, ServiceRegistry& registry) {
  butil::IOBuf read_buf;

  while (true) {
    FrameParseResult result = TryParseRpcFrame(read_buf);

    if (result.status == FrameParseStatus::NEED_MORE_DATA) {
      int read_bytes = co_await ReadAwaiter {ctx, client_fd, read_buf};
      if (read_bytes <= 0) {
        co_await CloseAwaiter {ctx, client_fd};
        co_return;
      }
      continue;
    } else if (result.status == FrameParseStatus::SUCCESS) {
      read_buf.pop_front(result.total_frame_bytes);

      // If Context has an attached Executor (WorkStealingExecutor worker pool),
      // hop the coroutine to a Worker Thread so business logic runs on the multi-core pool
      if (auto* exec = ctx.GetExecutor()) {
        co_await resume_on(*exec);
      }

      butil::IOBuf response_buf;
      if (!registry.Dispatch(result, response_buf)) {
        co_await CloseAwaiter {ctx, client_fd};
        co_return;
      }

      co_await IOBufWriteAwaiter {ctx, client_fd, response_buf};
      continue;
    } else {
      co_await CloseAwaiter {ctx, client_fd};
      co_return;
    }
  }
}

// ============================================================================
// RpcServer: High-Performance C++20 Asynchronous io_uring RPC Server (bRPC Style)
// ============================================================================
class RpcServer {
 public:
  explicit RpcServer(Context& ctx, butil::EndPoint endpoint) : ctx_(ctx), endpoint_(endpoint) { Init(); }

  explicit RpcServer(Context& ctx, int port, butil::ip_t ip = butil::IP_ANY)
      : RpcServer(ctx, butil::EndPoint(ip, port)) {}

  ~RpcServer() {
    if (server_socket_ >= 0) {
      close(server_socket_);
    }
  }

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  // bRPC-style AddService
  bool AddService(google::protobuf::Service* service) { return registry_.RegisterService(service); }

  bool AddService(std::shared_ptr<google::protobuf::Service> service) {
    if (!service) {
      return false;
    }
    owned_services_.push_back(service);
    return registry_.RegisterService(service.get());
  }

  ServiceRegistry& GetRegistry() noexcept { return registry_; }
  [[nodiscard]] int GetSocketFd() const noexcept { return server_socket_; }
  [[nodiscard]] const butil::EndPoint& GetEndPoint() const noexcept { return endpoint_; }

 private:
  void Init() {
    server_socket_ = butil::tcp_listen(endpoint_);
    if (server_socket_ < 0) {
      perror("Failed to start listening with butil::tcp_listen...");
      return;
    }

    if (endpoint_.port == 0) {
      butil::get_local_side(server_socket_, &endpoint_);
    }

    acceptor_ = std::make_unique<Acceptor>(ctx_, server_socket_,
                                           [this](int client_fd) { handle_rpc_client(ctx_, client_fd, registry_); });
    acceptor_->Start();
  }

  Context& ctx_;
  butil::EndPoint endpoint_;
  int server_socket_ {-1};
  ServiceRegistry registry_;
  std::vector<std::shared_ptr<google::protobuf::Service>> owned_services_;
  std::unique_ptr<Acceptor> acceptor_;
};

}  // namespace ant_server::rpc

namespace ant_rpc {
using namespace ant_server::rpc;
}
