#pragma once

#include <chrono>
#include <cstddef>
#include <limits>

#include "ant_server/rpc/protocol.hpp"

namespace ant_server::rpc {
struct RpcServerOptions {
  // Number of Scheduler IO Contexts eligible to own accepted connections.
  // One keeps the old single-owner behavior; RpcServer clamps larger values
  // to Scheduler::NumIOThreads().
  std::size_t io_contexts {1};
  std::size_t max_connections {65536};
  std::size_t max_in_flight {65536};
  std::size_t max_in_flight_per_connection {4096};
  // Bounds work accepted by the server but still waiting to begin on a worker.
  // Running service methods are accounted for by max_in_flight instead.
  std::size_t max_pending_worker_tasks {8192};
  std::size_t max_frame_bytes {kDefaultMaxRpcFrameBytes};
  std::size_t max_outbound_bytes_per_connection {16U * 1024U * 1024U};
  std::chrono::milliseconds graceful_stop_timeout {5000};
  std::chrono::milliseconds idle_timeout {0};

  [[nodiscard]] bool IsValid() const noexcept {
    return io_contexts > 0 && max_connections > 0 && max_in_flight > 0 && max_in_flight_per_connection > 0 &&
           max_pending_worker_tasks > 0 && max_in_flight_per_connection <= max_in_flight &&
           max_frame_bytes >= kRpcHeaderBytes && max_outbound_bytes_per_connection >= kRpcHeaderBytes &&
           max_outbound_bytes_per_connection <= static_cast<std::size_t>(std::numeric_limits<int64_t>::max()) &&
           graceful_stop_timeout.count() >= 0 && idle_timeout.count() >= 0;
  }
};
using ServerOptions = RpcServerOptions;
}  // namespace ant_server::rpc
