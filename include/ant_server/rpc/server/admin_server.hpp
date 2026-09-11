#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>

#include "absl/synchronization/mutex.h"
#include "ant_server/awaiter/socket_awaiter.hpp"
#include "ant_server/context/context.hpp"
#include "ant_server/handler/acceptor.hpp"
#include "ant_server/http/parser.hpp"
#include "ant_server/metrics/metric_registry.hpp"
#include "ant_server/rpc/server/rpc_server.hpp"
#include "ant_server/rpc/server/server_metrics.hpp"
#include "butil/endpoint.h"
#include "butil/iobuf.h"

namespace ant_server::rpc::detail {

class AdminConnection;

struct AdminMetricSource {
  std::string name;
  std::shared_ptr<const ServerMetrics> metrics;
  const RpcServer* server;
};

inline std::string_view ServerStatusName(RpcServer::Status status) {
  switch (status) {
    case RpcServer::Status::kCreated:
      return "created";
    case RpcServer::Status::kRunning:
      return "running";
    case RpcServer::Status::kStopping:
      return "stopping";
    case RpcServer::Status::kStopped:
      return "stopped";
    case RpcServer::Status::kFailed:
      return "failed";
  }
  return "unknown";
}

inline std::string EscapeJson(std::string_view value) {
  std::string result;
  constexpr char kHex[] = "0123456789abcdef";
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') {
      result += '\\';
      result += static_cast<char>(c);
    } else if (c < 0x20) {
      result += "\\u00";
      result += kHex[c >> 4];
      result += kHex[c & 15];
    } else {
      result += static_cast<char>(c);
    }
  }
  return result;
}

inline std::string EscapePrometheusLabel(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    switch (character) {
      case '\\':
        result += "\\\\";
        break;
      case '"':
        result += "\\\"";
        break;
      case '\n':
        result += "\\n";
        break;
      default:
        result += character;
    }
  }
  return result;
}

class AdminServerState final : public ServerObserver, public std::enable_shared_from_this<AdminServerState> {
 public:
  explicit AdminServerState(Context& context) : context(context) {}

  bool AddSource(std::string name, const RpcServer& server) {
    if (name.empty()) {
      return false;
    }
    absl::MutexLock lock(&sources_mu);
    for (const auto& source : sources) {
      if (source.name == name) {
        return false;
      }
    }
    server.RegisterObserver(shared_from_this());
    sources.push_back({std::move(name), server.metrics_handle(), &server});
    return true;
  }

  void RemoveServer(const RpcServer* server) override {
    absl::MutexLock lock(&sources_mu);
    std::erase_if(sources, [server](const auto& source) { return source.server == server; });
  }

  // Readiness, not a proof that workers are making progress or dependencies
  // are healthy. Metrics/status are independently sampled observations.
  bool Healthy() const {
    absl::MutexLock lock(&sources_mu);
    return accepting.load(std::memory_order_acquire) && !sources.empty() &&
           std::all_of(sources.begin(), sources.end(),
                       [](const auto& source) { return source.server->status() == RpcServer::Status::kRunning; });
  }

  std::string StatusText() const {
    // Keep the registration lock until all accesses to server/metrics finish.
    // RpcServer destruction removes its entries under the same lock.
    absl::MutexLock lock(&sources_mu);
    const auto& snapshot = sources;
    bool ready = accepting.load(std::memory_order_acquire) && !snapshot.empty();
    std::ostringstream servers;
    bool first = true;
    for (const auto& source : snapshot) {
      const auto status = source.server->status();
      ready = ready && status == RpcServer::Status::kRunning;
      if (!std::exchange(first, false)) {
        servers << ',';
      }
      servers << "{\"name\":\"" << EscapeJson(source.name) << "\",\"state\":\"" << ServerStatusName(status)
              << "\",\"active_connections\":" << source.metrics->active_connections.Value()
              << ",\"active_in_flight\":" << source.metrics->active_in_flight.Value()
              << ",\"pending_worker_tasks\":" << source.metrics->pending_worker_tasks.Value()
              << ",\"io_command_backlog\":" << source.metrics->io_command_backlog.Value()
              << ",\"outbound_bytes\":" << source.metrics->outbound_bytes.Value() << '}';
    }
    return std::string("{\"ready\":") + (ready ? "true" : "false") + ",\"servers\":[" + servers.str() + "]}\n";
  }

  std::string MetricsText() const {
    absl::MutexLock lock(&sources_mu);
    const auto& snapshot = sources;

    std::ostringstream output;
    output << "# TYPE ant_rpc_server_requests_received_total counter\n";
    output << "# TYPE ant_rpc_server_calls_completed_total counter\n";
    output << "# TYPE ant_rpc_server_responses_enqueued_total counter\n";
    output << "# TYPE ant_rpc_server_partial_write_completions_total counter\n";
    output << "# TYPE ant_rpc_server_active_connections gauge\n";
    output << "# TYPE ant_rpc_server_pending_worker_tasks gauge\n";
    output << "# TYPE ant_rpc_server_outbound_bytes gauge\n";
    output << "# TYPE ant_rpc_server_io_command_backlog gauge\n";
    output << "# TYPE ant_rpc_server_connection_closes_total counter\n";
    output << "# TYPE ant_rpc_server_request_latency_microseconds summary\n";
    output << "# TYPE ant_rpc_server_method_calls_started_total counter\n";
    output << "# TYPE ant_rpc_server_method_calls_completed_total counter\n";
    output << "# TYPE ant_rpc_server_method_call_errors_total counter\n";
    output << "# TYPE ant_rpc_server_method_active_in_flight gauge\n";
    output << "# TYPE ant_rpc_server_method_call_latency_microseconds summary\n";
    for (const auto& source : snapshot) {
      const std::string label = EscapePrometheusLabel(source.name);
      const auto latency = source.metrics->request_latency.Snapshot();
      const auto write_counter = [&output, &label](std::string_view metric, uint64_t value) {
        output << metric << "{server=\"" << label << "\"} " << value << '\n';
      };
      const auto write_gauge = [&output, &label](std::string_view metric, int64_t value) {
        output << metric << "{server=\"" << label << "\"} " << value << '\n';
      };
      write_counter("ant_rpc_server_accepted_connections_total", source.metrics->accepted_connections.Value());
      write_counter("ant_rpc_server_rejected_connections_total", source.metrics->rejected_connections.Value());
      write_counter("ant_rpc_server_closed_connections_total", source.metrics->closed_connections.Value());
      write_counter("ant_rpc_server_requests_received_total", source.metrics->requests_received.Value());
      write_counter("ant_rpc_server_calls_started_total", source.metrics->calls_started.Value());
      write_counter("ant_rpc_server_calls_completed_total", source.metrics->calls_completed.Value());
      write_counter("ant_rpc_server_call_errors_total", source.metrics->call_errors.Value());
      write_counter("ant_rpc_server_requests_rejected_overload_total",
                    source.metrics->requests_rejected_overload.Value());
      write_counter("ant_rpc_server_responses_enqueued_total", source.metrics->responses_enqueued.Value());
      write_counter("ant_rpc_server_write_errors_total", source.metrics->write_errors.Value());
      write_counter("ant_rpc_server_partial_write_completions_total",
                    source.metrics->partial_write_completions.Value());
      write_counter("ant_rpc_server_protocol_errors_total", source.metrics->protocol_errors.Value());
      write_gauge("ant_rpc_server_active_connections", source.metrics->active_connections.Value());
      write_gauge("ant_rpc_server_active_in_flight", source.metrics->active_in_flight.Value());
      write_gauge("ant_rpc_server_pending_worker_tasks", source.metrics->pending_worker_tasks.Value());
      write_gauge("ant_rpc_server_outbound_bytes", source.metrics->outbound_bytes.Value());
      write_gauge("ant_rpc_server_io_command_backlog", source.metrics->io_command_backlog.Value());
      for (std::size_t reason_index = static_cast<std::size_t>(ConnectionCloseReason::kLocalRequest);
           reason_index < static_cast<std::size_t>(ConnectionCloseReason::kCount); ++reason_index) {
        const auto reason = static_cast<ConnectionCloseReason>(reason_index);
        output << "ant_rpc_server_connection_closes_total{server=\"" << label << "\",reason=\""
               << ConnectionCloseReasonName(reason) << "\"} " << source.metrics->ConnectionCloses(reason) << '\n';
      }
      write_counter("ant_rpc_server_request_latency_microseconds_count", latency.count);
      write_counter("ant_rpc_server_request_latency_microseconds_sum", latency.total_microseconds);
      write_counter("ant_rpc_server_request_latency_microseconds_min", latency.min_microseconds);
      write_counter("ant_rpc_server_request_latency_microseconds_max", latency.max_microseconds);
      for (const ServerMethodMetrics* method : source.metrics->MethodSnapshot()) {
        const std::string service_label = EscapePrometheusLabel(method->service_name);
        const std::string method_label = EscapePrometheusLabel(method->method_name);
        const std::string labels =
            "{server=\"" + label + "\",service=\"" + service_label + "\",method=\"" + method_label + "\"} ";
        const auto method_latency = method->call_latency.Snapshot();
        output << "ant_rpc_server_method_calls_started_total" << labels << method->calls_started.Value() << '\n';
        output << "ant_rpc_server_method_calls_completed_total" << labels << method->calls_completed.Value() << '\n';
        output << "ant_rpc_server_method_call_errors_total" << labels << method->call_errors.Value() << '\n';
        output << "ant_rpc_server_method_active_in_flight" << labels << method->active_in_flight.Value() << '\n';
        output << "ant_rpc_server_method_call_latency_microseconds_count" << labels << method_latency.count << '\n';
        output << "ant_rpc_server_method_call_latency_microseconds_sum" << labels << method_latency.total_microseconds
               << '\n';
        output << "ant_rpc_server_method_call_latency_microseconds_min" << labels << method_latency.min_microseconds
               << '\n';
        output << "ant_rpc_server_method_call_latency_microseconds_max" << labels << method_latency.max_microseconds
               << '\n';
      }
    }
    output << registry.PrometheusText();
    return output.str();
  }

  void TrackConnection(const std::shared_ptr<AdminConnection>& connection);
  void ReleaseConnection(const std::shared_ptr<AdminConnection>& connection);
  void CloseConnections();

  Context& context;
  std::atomic<bool> accepting {true};
  // Reserve a separate namespace so custom series cannot collide with the
  // framework's ant_rpc_* families. Callers register unprefixed names.
  ant_server::metrics::MetricRegistry registry {"ant_custom_"};

 private:
  mutable absl::Mutex sources_mu;
  std::vector<AdminMetricSource> sources ABSL_GUARDED_BY(sources_mu);
  absl::Mutex connections_mu;
  std::vector<std::shared_ptr<AdminConnection>> connections ABSL_GUARDED_BY(connections_mu);
};

class AdminConnection final : public IoCommandMailbox, public std::enable_shared_from_this<AdminConnection> {
 public:
  AdminConnection(std::shared_ptr<AdminServerState> state, int fd) : state_(std::move(state)), fd_(fd) {}

  void Start() { Run(shared_from_this()); }
  void RequestClose() {
    if (!close_requested_.exchange(true, std::memory_order_acq_rel)) {
      state_->context.Notify(shared_from_this());
    }
  }
  void DrainCommandsOnIoThread() override {
    if (close_requested_.load(std::memory_order_acquire) && fd_ >= 0) {
      ShutdownSocket(state_->context, SocketHandle::Native(fd_), SHUT_RDWR);
    }
  }

 private:
  static void AppendResponse(butil::IOBuf& output, int status, std::string_view reason, std::string_view body,
                             std::string_view content_type = "text/plain; charset=utf-8") {
    const std::string headers = "HTTP/1.1 " + std::to_string(status) + " " + std::string(reason) +
                                "\r\nContent-Type: " + std::string(content_type) +
                                "\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n";
    output.append(headers);
    output.append(body.data(), body.size());
  }

  DetachedTask Run(std::shared_ptr<AdminConnection> self) {
    HttpParser parser;
    HttpRequest request;
    butil::IOBuf input;
    butil::IOBuf output;
    while (true) {
      const ParseResult parsed = parser.parse(input, request);
      if (parsed.status == PARSE_SUCCESS) {
        if (request.method != "GET") {
          AppendResponse(output, 405, "Method Not Allowed", "only GET is supported\n");
        } else if (request.url == "/metrics") {
          AppendResponse(output, 200, "OK", self->state_->MetricsText(), "text/plain; version=0.0.4");
        } else if (request.url == "/health") {
          const bool healthy = self->state_->Healthy();
          AppendResponse(output, healthy ? 200 : 503, healthy ? "OK" : "Service Unavailable",
                         healthy ? "ok\n" : "not ready\n");
        } else if (request.url == "/status" || request.url == "/status/") {
          AppendResponse(output, 200, "OK", self->state_->StatusText(), "application/json; charset=utf-8");
        } else {
          AppendResponse(output, 404, "Not Found", "not found\n");
        }
        break;
      }
      if (parsed.status == PARSE_ERROR) {
        AppendResponse(output, 400, "Bad Request", "bad request\n");
        break;
      }
      const int bytes = co_await ReadAwaiter(self->state_->context, self->fd_, input, /*is_fixed=*/false);
      if (bytes <= 0) {
        self->CloseOnIoThread();
        co_return;
      }
    }

    while (!output.empty()) {
      const int bytes = co_await IOBufWriteAwaiter(self->state_->context, self->fd_, output, /*is_fixed=*/false);
      if (bytes <= 0) {
        break;
      }
      output.pop_front(static_cast<std::size_t>(bytes));
    }
    self->CloseOnIoThread();
  }

  void CloseOnIoThread() {
    if (const int fd = std::exchange(fd_, -1); fd >= 0) {
      CloseSocket(state_->context, SocketHandle::Native(fd));
    }
    state_->ReleaseConnection(shared_from_this());
  }

  std::shared_ptr<AdminServerState> state_;
  int fd_ {-1};
  std::atomic<bool> close_requested_ {false};
};

inline void AdminServerState::TrackConnection(const std::shared_ptr<AdminConnection>& connection) {
  absl::MutexLock lock(&connections_mu);
  connections.push_back(connection);
}

inline void AdminServerState::ReleaseConnection(const std::shared_ptr<AdminConnection>& connection) {
  absl::MutexLock lock(&connections_mu);
  std::erase(connections, connection);
}

inline void AdminServerState::CloseConnections() {
  std::vector<std::shared_ptr<AdminConnection>> snapshot;
  {
    absl::MutexLock lock(&connections_mu);
    snapshot = connections;
    // The snapshot keeps each connection alive until its close command has
    // been posted. Clearing ownership here also prevents a stopped admin
    // server from retaining a connection that is waiting for a peer forever.
    connections.clear();
  }
  for (const auto& connection : snapshot) {
    connection->RequestClose();
  }
}

}  // namespace ant_server::rpc::detail

namespace ant_server::rpc {

inline butil::ip_t AdminLoopbackIp() noexcept { return {htonl(INADDR_LOOPBACK)}; }

// Small, local-only HTTP observability endpoint. It may share a Context with
// RpcServer, but owns a different listen fd and independent HTTP connections.
// Each accepted connection serves one request then closes, keeping the V1
// admin path intentionally bounded and separate from RPC transport state.
class AdminServer {
 public:
  explicit AdminServer(Context& context, int port = 0, butil::ip_t ip = AdminLoopbackIp())
      : endpoint_(ip, port), state_(std::make_shared<detail::AdminServerState>(context)) {}
  ~AdminServer() { Stop(); }
  AdminServer(const AdminServer&) = delete;
  AdminServer& operator=(const AdminServer&) = delete;

  bool AddServer(std::string name, const RpcServer& server) { return state_->AddSource(std::move(name), server); }

  [[nodiscard]] ant_server::metrics::MetricRegistry& Registry() noexcept { return state_->registry; }

  bool Start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return false;
    }
    listen_fd_ = butil::tcp_listen(endpoint_);
    if (listen_fd_ < 0) {
      started_.store(false, std::memory_order_release);
      return false;
    }
    if (endpoint_.port == 0) {
      butil::get_local_side(listen_fd_, &endpoint_);
    }
    acceptor_ = std::make_unique<Acceptor>(state_->context, listen_fd_, [state = state_](int client_fd) {
      if (!state->accepting.load(std::memory_order_acquire)) {
        CloseSocket(state->context, SocketHandle::Native(client_fd));
        return;
      }
      auto connection = std::make_shared<detail::AdminConnection>(state, client_fd);
      state->TrackConnection(connection);
      connection->Start();
    });
    acceptor_->Start();
    return true;
  }

  void Stop() {
    if (!started_.exchange(false, std::memory_order_acq_rel)) {
      return;
    }
    state_->accepting.store(false, std::memory_order_release);
    state_->CloseConnections();
    if (listen_fd_ >= 0) {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
  }

  [[nodiscard]] const butil::EndPoint& GetEndPoint() const noexcept { return endpoint_; }
  [[nodiscard]] bool IsRunning() const noexcept { return started_.load(std::memory_order_acquire); }

 private:
  butil::EndPoint endpoint_;
  std::shared_ptr<detail::AdminServerState> state_;
  std::unique_ptr<Acceptor> acceptor_;
  std::atomic<bool> started_ {false};
  int listen_fd_ {-1};
};

}  // namespace ant_server::rpc
