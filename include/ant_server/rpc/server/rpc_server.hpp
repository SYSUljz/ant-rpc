#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <google/protobuf/service.h>
#include <sys/socket.h>

#include "absl/synchronization/mutex.h"
#include "ant_server/context/context.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/logging/logging.hpp"
#include "ant_server/rpc/server/server_connection.hpp"
#include "ant_server/rpc/server/server_metrics.hpp"
#include "butil/endpoint.h"

namespace ant_server::rpc {

class RpcServer;
namespace detail {
// Removal must wait for ongoing queries before returning. Observers are weakly
// held so an admin endpoint may be destroyed before its registered servers.
class ServerObserver {
 public:
  virtual ~ServerObserver() = default;
  virtual void RemoveServer(const RpcServer* server) = 0;
};
}  // namespace detail

// Public listen/accept/service facade. Per-connection IO lives in
// server_connection.hpp and is not mixed into this lifecycle API.
class RpcServer {
 public:
  enum class Status : uint8_t { kCreated, kRunning, kStopping, kStopped, kFailed };

  explicit RpcServer(Context& ctx, butil::EndPoint endpoint, RpcServerOptions options = {})
      : ctx_(ctx),
        endpoint_(endpoint),
        options_(std::move(options)),
        runtime_(std::make_shared<detail::ServerRuntime>(ctx, options_)) {}
  explicit RpcServer(Context& ctx, int port, butil::ip_t ip = butil::IP_ANY, RpcServerOptions options = {})
      : RpcServer(ctx, butil::EndPoint(ip, port), std::move(options)) {}
  ~RpcServer() {
    std::vector<std::weak_ptr<detail::ServerObserver>> observers;
    {
      absl::MutexLock lock(&observers_mu_);
      observers.swap(observers_);
    }
    // No lifecycle lock is held: an ongoing admin query may need status().
    for (const auto& observer : observers) {
      if (auto live = observer.lock()) {
        live->RemoveServer(this);
      }
    }
    Stop();
    Join();
  }
  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  bool AddService(google::protobuf::Service* service) {
    absl::MutexLock lock(&lifecycle_mu_);
    if (status_ != Status::kCreated || !registry_.RegisterService(service)) {
      return false;
    }
    runtime_->RegisterServiceMetrics(*service->GetDescriptor());
    return true;
  }
  bool AddService(std::shared_ptr<google::protobuf::Service> service) {
    absl::MutexLock lock(&lifecycle_mu_);
    if (status_ != Status::kCreated || !service) {
      return false;
    }
    if (!registry_.RegisterService(service.get())) {
      return false;
    }
    runtime_->RegisterServiceMetrics(*service->GetDescriptor());
    owned_services_.push_back(std::move(service));
    return true;
  }

  // Bind/listen and arm accept only after services and options are final.
  bool Start() {
    absl::MutexLock lock(&lifecycle_mu_);
    if (status_ != Status::kCreated || !options_.IsValid()) {
      return false;
    }
    server_socket_ = butil::tcp_listen(endpoint_);
    if (server_socket_ < 0) {
      const int error = errno;
      ant_server::logging::Write(ant_server::logging::Event::kServerStartFailed,
                                 [&](auto& out) { out << " port=" << endpoint_.port << " errno=" << error; });
      status_ = Status::kFailed;
      return false;
    }
    if (endpoint_.port == 0) {
      butil::get_local_side(server_socket_, &endpoint_);
    }
    acceptor_ = std::make_unique<Acceptor>(ctx_, server_socket_, [this](int client_fd) {
      if (!runtime_->TryAcquireConnection()) {
        ctx_.UseService<IOuringSocketService>().SubmitClose(client_fd, nullptr, /*is_fixed=*/true);
        return;
      }
      auto connection = std::make_shared<detail::ServerConnection>(ctx_, client_fd, registry_, runtime_);
      runtime_->TrackConnection(connection);
      connection->Start();
    });
    acceptor_->Start();
    status_ = Status::kRunning;
    ant_server::logging::Write(ant_server::logging::Event::kServerStarted,
                               [&](auto& out) { out << " port=" << endpoint_.port; });
    return true;
  }

  // Stop accepting immediately. Existing connections get the configured
  // graceful deadline, then ServerRuntime requests their IO-owner close.
  void Stop() {
    {
      absl::MutexLock lock(&lifecycle_mu_);
      if (status_ == Status::kStopped || status_ == Status::kStopping) {
        return;
      }
      status_ = status_ == Status::kRunning ? Status::kStopping : Status::kStopped;
    }
    if (server_socket_ >= 0) {
      shutdown(server_socket_, SHUT_RDWR);
      close(server_socket_);
      server_socket_ = -1;
    }
    runtime_->BeginGracefulStop();
  }

  // Separate from Stop so multiple servers may begin shutdown together.
  // It also waits for running service code: closing an fd cannot safely
  // destroy ServiceRegistry while a worker is still executing a service.
  bool Join() {
    {
      absl::MutexLock lock(&lifecycle_mu_);
      if (status_ == Status::kCreated || status_ == Status::kRunning || status_ == Status::kFailed) {
        return false;
      }
      if (status_ == Status::kStopped) {
        return true;
      }
    }
    runtime_->WaitForDrained();
    absl::MutexLock lock(&lifecycle_mu_);
    status_ = Status::kStopped;
    ant_server::logging::Write(ant_server::logging::Event::kServerStopped,
                               [&](auto& out) { out << " port=" << endpoint_.port; });
    return true;
  }

  ServiceRegistry& GetRegistry() noexcept { return registry_; }
  [[nodiscard]] int GetSocketFd() const noexcept { return server_socket_; }
  [[nodiscard]] const butil::EndPoint& GetEndPoint() const noexcept { return endpoint_; }
  [[nodiscard]] const RpcServerOptions& options() const noexcept { return options_; }
  [[nodiscard]] const ServerMetrics& metrics() const noexcept { return runtime_->metrics(); }
  // Lets an AdminServer retain metrics safely even if this facade is later
  // destroyed. The metrics remain a snapshot source, not a control channel.
  [[nodiscard]] std::shared_ptr<const ServerMetrics> metrics_handle() const noexcept {
    return runtime_->metrics_handle();
  }
  [[nodiscard]] Status status() const noexcept {
    absl::MutexLock lock(&lifecycle_mu_);
    return status_;
  }

  // Registration, like other facade API calls, must not race destruction.
  void RegisterObserver(const std::shared_ptr<detail::ServerObserver>& observer) const {
    absl::MutexLock lock(&observers_mu_);
    std::erase_if(observers_, [](const auto& entry) { return entry.expired(); });
    for (const auto& entry : observers_) {
      if (entry.lock() == observer) {
        return;
      }
    }
    observers_.push_back(observer);
  }

 private:
  Context& ctx_;
  butil::EndPoint endpoint_;
  const RpcServerOptions options_;
  std::shared_ptr<detail::ServerRuntime> runtime_;
  mutable absl::Mutex lifecycle_mu_;
  Status status_ ABSL_GUARDED_BY(lifecycle_mu_) {Status::kCreated};
  mutable absl::Mutex observers_mu_;
  mutable std::vector<std::weak_ptr<detail::ServerObserver>> observers_ ABSL_GUARDED_BY(observers_mu_);
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

namespace ant_rpc {
using namespace ant_server::rpc;
}
