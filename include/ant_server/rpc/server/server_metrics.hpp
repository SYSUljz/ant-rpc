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
  ant_server::metrics::Counter protocol_errors;

  ant_server::metrics::Gauge active_connections;
  ant_server::metrics::Gauge active_in_flight;
  // Accepted service tasks which have not begun worker execution yet.
  ant_server::metrics::Gauge pending_worker_tasks;
  // Bytes retained in connection command mailboxes, outbound queues, or an
  // active partial write. This is the aggregate of per-connection reserves.
  ant_server::metrics::Gauge outbound_bytes;

  ant_server::metrics::LatencyRecorder request_latency;
};

}  // namespace ant_server::rpc
