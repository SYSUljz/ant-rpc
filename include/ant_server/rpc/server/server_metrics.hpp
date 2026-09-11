#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <google/protobuf/descriptor.h>

#include "ant_server/metrics/latency_recorder.hpp"
#include "ant_server/metrics/metric.hpp"

namespace ant_server::rpc {

// Fixed labels keep the Prometheus series bounded and make close paths
// reviewable. kNone is connection-local state and is exported as kUnknown if
// a future path reaches release without selecting a reason.
enum class ConnectionCloseReason : uint8_t {
  kNone = 0,
  kLocalRequest,
  kServerStop,
  kPeerEof,
  kReadError,
  kWriteError,
  kProtocolError,
  kIdleTimeout,
  kResourceLimit,
  kInternalError,
  kUnknown,
  kCount,
};

constexpr std::string_view ConnectionCloseReasonName(ConnectionCloseReason reason) noexcept {
  switch (reason) {
    case ConnectionCloseReason::kLocalRequest:
      return "local_request";
    case ConnectionCloseReason::kServerStop:
      return "server_stop";
    case ConnectionCloseReason::kPeerEof:
      return "peer_eof";
    case ConnectionCloseReason::kReadError:
      return "read_error";
    case ConnectionCloseReason::kWriteError:
      return "write_error";
    case ConnectionCloseReason::kProtocolError:
      return "protocol_error";
    case ConnectionCloseReason::kIdleTimeout:
      return "idle_timeout";
    case ConnectionCloseReason::kResourceLimit:
      return "resource_limit";
    case ConnectionCloseReason::kInternalError:
      return "internal_error";
    case ConnectionCloseReason::kNone:
    case ConnectionCloseReason::kUnknown:
    case ConnectionCloseReason::kCount:
      return "unknown";
  }
  return "unknown";
}

// Metrics for one registered protobuf method. Instances are allocated once
// and never moved or removed, so an InboundCallState may safely retain a raw
// pointer while ServerRuntime keeps the owning ServerMetrics alive.
struct ServerMethodMetrics {
  std::string service_name;
  std::string method_name;
  ant_server::metrics::Counter calls_started;
  ant_server::metrics::Counter calls_completed;
  ant_server::metrics::Counter call_errors;
  ant_server::metrics::Gauge active_in_flight;
  ant_server::metrics::LatencyRecorder call_latency;
};

// Framework-owned metrics for one RpcServer instance. Service implementations
// do not update these; ServerRuntime and ServerConnection update them at their
// lifecycle boundaries. No registry or global names are involved yet.
struct ServerMetrics {
  ant_server::metrics::Counter accepted_connections;
  ant_server::metrics::Counter rejected_connections;
  ant_server::metrics::Counter closed_connections;
  // A complete, valid RPC_REQUEST frame reached ServerConnection.
  ant_server::metrics::Counter requests_received;
  // An InboundCallState was admitted to the business-call lifecycle.
  ant_server::metrics::Counter calls_started;
  // An admitted call reached its unique terminal state. This is deliberately
  // independent of whether its response is later written to the peer.
  ant_server::metrics::Counter calls_completed;
  ant_server::metrics::Counter call_errors;
  // A valid request was rejected before it entered InboundCallState.
  ant_server::metrics::Counter requests_rejected_overload;
  // A response was accepted into ServerConnection's serialized write queue.
  ant_server::metrics::Counter responses_enqueued;
  // An asynchronous socket write failed after a response was queued.
  ant_server::metrics::Counter write_errors;
  // A write completion made forward progress but left bytes in the same frame.
  ant_server::metrics::Counter partial_write_completions;
  ant_server::metrics::Counter protocol_errors;

  ant_server::metrics::Gauge active_connections;
  ant_server::metrics::Gauge active_in_flight;
  // Accepted service tasks which have not begun worker execution yet.
  ant_server::metrics::Gauge pending_worker_tasks;
  // Bytes retained in connection command mailboxes, outbound queues, or an
  // active partial write. This is the aggregate of per-connection reserves.
  ant_server::metrics::Gauge outbound_bytes;
  // Commands posted by workers/timers but not yet claimed by their owning IO
  // thread. The command currently executing is not part of the backlog.
  ant_server::metrics::Gauge io_command_backlog;

  ant_server::metrics::LatencyRecorder request_latency;

  void RecordConnectionClose(ConnectionCloseReason reason) noexcept {
    if (reason == ConnectionCloseReason::kNone || reason == ConnectionCloseReason::kCount) {
      reason = ConnectionCloseReason::kUnknown;
    }
    connection_closes_by_reason_[static_cast<std::size_t>(reason)].Add();
  }

  [[nodiscard]] uint64_t ConnectionCloses(ConnectionCloseReason reason) const noexcept {
    if (reason == ConnectionCloseReason::kNone || reason == ConnectionCloseReason::kCount) {
      reason = ConnectionCloseReason::kUnknown;
    }
    return connection_closes_by_reason_[static_cast<std::size_t>(reason)].Value();
  }

  // Called only after ServiceRegistry has resolved a real descriptor. This
  // bounds metric cardinality to methods compiled into registered services,
  // rather than trusting arbitrary service/method strings from the wire.
  ServerMethodMetrics& GetOrCreateMethod(const google::protobuf::MethodDescriptor& method) {
    absl::MutexLock lock(&method_metrics_mu_);
    auto [iterator, inserted] = method_metrics_.try_emplace(&method);
    if (inserted) {
      auto entry = std::make_unique<ServerMethodMetrics>();
      entry->service_name = method.service()->full_name();
      entry->method_name = method.name();
      iterator->second = std::move(entry);
    }
    return *iterator->second;
  }

  // The returned pointers remain valid for the lifetime of ServerMetrics.
  // Counter values remain weakly consistent, as expected for observability.
  [[nodiscard]] std::vector<const ServerMethodMetrics*> MethodSnapshot() const {
    absl::MutexLock lock(&method_metrics_mu_);
    std::vector<const ServerMethodMetrics*> snapshot;
    snapshot.reserve(method_metrics_.size());
    for (const auto& [descriptor, metrics] : method_metrics_) {
      (void)descriptor;
      snapshot.push_back(metrics.get());
    }
    std::sort(snapshot.begin(), snapshot.end(), [](const ServerMethodMetrics* lhs, const ServerMethodMetrics* rhs) {
      return lhs->service_name < rhs->service_name ||
             (lhs->service_name == rhs->service_name && lhs->method_name < rhs->method_name);
    });
    return snapshot;
  }

 private:
  mutable absl::Mutex method_metrics_mu_;
  absl::flat_hash_map<const google::protobuf::MethodDescriptor*, std::unique_ptr<ServerMethodMetrics>> method_metrics_
      ABSL_GUARDED_BY(method_metrics_mu_);
  std::array<ant_server::metrics::Counter, static_cast<std::size_t>(ConnectionCloseReason::kCount)>
      connection_closes_by_reason_;
};

}  // namespace ant_server::rpc
