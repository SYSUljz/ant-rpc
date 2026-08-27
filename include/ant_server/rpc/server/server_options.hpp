#pragma once

#include <chrono>
#include <cstddef>

#include "ant_server/rpc/protocol.hpp"

namespace ant_server::rpc {
struct RpcServerOptions {
  std::size_t max_connections {65536};
  std::size_t max_in_flight {65536};
  std::size_t max_frame_bytes {kDefaultMaxRpcFrameBytes};
  std::chrono::milliseconds graceful_stop_timeout {5000};
  std::chrono::milliseconds idle_timeout {0};

  [[nodiscard]] bool IsValid() const noexcept {
    return max_connections > 0 && max_in_flight > 0 && max_frame_bytes >= kRpcHeaderBytes &&
           graceful_stop_timeout.count() >= 0 && idle_timeout.count() >= 0;
  }
};
using ServerOptions = RpcServerOptions;
}  // namespace ant_server::rpc
