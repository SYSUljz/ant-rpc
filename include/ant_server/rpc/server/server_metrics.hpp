#pragma once

#include "ant_server/metrics/latency_recorder.hpp"
#include "ant_server/metrics/metric.hpp"

namespace ant_server::rpc {

// Framework-owned metrics for one RpcServer instance. Service implementations
// do not update these; ServerRuntime and ServerConnection update them at their
// lifecycle boundaries. No registry or global names are involved yet.
struct ServerMetrics {
  ant_server::metrics::Counter accepted_connections;
  ant_server::metrics::Counter rejected_connections;
  ant_server::metrics::Counter closed_connections;
  ant_server::metrics::Counter requests_total;
  ant_server::metrics::Counter requests_completed;
  ant_server::metrics::Counter request_errors;
  ant_server::metrics::Counter overloaded_requests;
  ant_server::metrics::Counter protocol_errors;

  ant_server::metrics::Gauge active_connections;
  ant_server::metrics::Gauge active_in_flight;

  ant_server::metrics::LatencyRecorder request_latency;
};

}  // namespace ant_server::rpc
