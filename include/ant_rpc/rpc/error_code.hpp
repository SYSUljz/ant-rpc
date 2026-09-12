#pragma once

#include <string_view>

namespace ant_rpc::rpc {

enum RpcErrorCode : int {
  RPC_SUCCESS = 0,
  RPC_ENOSERVICE = 1001,     // Service not registered or not found
  RPC_ENOMETHOD = 1002,      // Method not found in target service
  RPC_EINVALID_DATA = 1003,  // Request/Response payload deserialization failed
  RPC_ETIMEOUT = 1004,       // RPC execution or network timeout
  RPC_ECANCELED = 1005,      // RPC canceled locally or due to client disconnect
  RPC_EINTERNAL = 1006,      // Internal server or business error
  RPC_EOVERLOAD = 1007,      // Server rejected request due to high load
  RPC_ECONN_FAILED = 1008,   // Failed to connect to remote server
  RPC_EUNKNOWN = 1099        // Unknown or unspecified RPC error
};

constexpr std::string_view RpcErrorToString(int error_code) noexcept {
  switch (error_code) {
    case RPC_SUCCESS:
      return "RPC_SUCCESS";
    case RPC_ENOSERVICE:
      return "RPC_ENOSERVICE";
    case RPC_ENOMETHOD:
      return "RPC_ENOMETHOD";
    case RPC_EINVALID_DATA:
      return "RPC_EINVALID_DATA";
    case RPC_ETIMEOUT:
      return "RPC_ETIMEOUT";
    case RPC_ECANCELED:
      return "RPC_ECANCELED";
    case RPC_EINTERNAL:
      return "RPC_EINTERNAL";
    case RPC_EOVERLOAD:
      return "RPC_EOVERLOAD";
    case RPC_ECONN_FAILED:
      return "RPC_ECONN_FAILED";
    default:
      return "RPC_EUNKNOWN";
  }
}

}  // namespace ant_rpc::rpc
