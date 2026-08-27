#pragma once

#include <unistd.h>

#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include <google/protobuf/service.h>
#include <sys/socket.h>

#include "ant_server/context/context.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/rpc/server/server_connection.hpp"
#include "butil/endpoint.h"

namespace ant_server::rpc {

// Public listen/accept/service facade. Per-connection IO lives in
// server_connection.hpp and is not mixed into this lifecycle API.
class RpcServer {
 public:
  explicit RpcServer(Context& ctx, butil::EndPoint endpoint, RpcServerOptions options = {})
      : ctx_(ctx), endpoint_(endpoint), options_(std::move(options)),
        runtime_(std::make_shared<detail::ServerRuntime>(ctx, options_)) { Init(); }
  explicit RpcServer(Context& ctx, int port, butil::ip_t ip = butil::IP_ANY, RpcServerOptions options = {})
      : RpcServer(ctx, butil::EndPoint(ip, port), std::move(options)) {}
  ~RpcServer() { Stop(); }
  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  bool AddService(google::protobuf::Service* service) { return registry_.RegisterService(service); }
  bool AddService(std::shared_ptr<google::protobuf::Service> service) {
    if (!service) return false;
    owned_services_.push_back(service);
    return registry_.RegisterService(service.get());
  }
  void Stop() {
    if (stopped_.exchange(true, std::memory_order_acq_rel)) return;
    if (server_socket_ >= 0) { shutdown(server_socket_, SHUT_RDWR); close(server_socket_); server_socket_ = -1; }
    runtime_->BeginGracefulStop();
  }
  ServiceRegistry& GetRegistry() noexcept { return registry_; }
  [[nodiscard]] int GetSocketFd() const noexcept { return server_socket_; }
  [[nodiscard]] const butil::EndPoint& GetEndPoint() const noexcept { return endpoint_; }
  [[nodiscard]] const RpcServerOptions& options() const noexcept { return options_; }

 private:
  static bool ValidateOptions(const RpcServerOptions& options) noexcept {
    return options.max_connections > 0 && options.max_in_flight > 0 && options.max_frame_bytes >= sizeof(RpcHeader) &&
           options.graceful_stop_timeout.count() >= 0 && options.idle_timeout.count() >= 0;
  }
  void Init() {
    if (!ValidateOptions(options_)) return;
    server_socket_ = butil::tcp_listen(endpoint_);
    if (server_socket_ < 0) { perror("Failed to start listening with butil::tcp_listen..."); return; }
    if (endpoint_.port == 0) butil::get_local_side(server_socket_, &endpoint_);
    acceptor_ = std::make_unique<Acceptor>(ctx_, server_socket_, [this](int client_fd) {
      if (!runtime_->TryAcquireConnection()) { close(client_fd); return; }
      auto connection = std::make_shared<detail::ServerConnection>(ctx_, client_fd, registry_, runtime_);
      runtime_->TrackConnection(connection);
      connection->Start();
    });
    acceptor_->Start();
  }
  Context& ctx_;
  butil::EndPoint endpoint_;
  const RpcServerOptions options_;
  std::shared_ptr<detail::ServerRuntime> runtime_;
  std::atomic<bool> stopped_ {false};
  int server_socket_ {-1};
  ServiceRegistry registry_;
  std::vector<std::shared_ptr<google::protobuf::Service>> owned_services_;
  std::unique_ptr<Acceptor> acceptor_;
};

inline DetachedTask handle_rpc_client(Context& ctx, int client_fd, ServiceRegistry& registry) {
  detail::handle_rpc_client(ctx, client_fd, registry);
  co_return;
}

}  // namespace ant_server::rpc

namespace ant_rpc { using namespace ant_server::rpc; }
