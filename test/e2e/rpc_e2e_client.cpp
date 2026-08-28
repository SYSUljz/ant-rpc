#include <cstdlib>
#include <iostream>
#include <string>

#include "ant_server/rpc/channel.hpp"
#include "ant_server/rpc/controller.hpp"
#include "echo.pb.h"

namespace {

int ParsePort(const char* value) {
  char* end = nullptr;
  const long port = std::strtol(value, &end, 10);
  return end && *end == '\0' && port > 0 && port <= 65535 ? static_cast<int>(port) : -1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: rpc_e2e_client HOST PORT\n";
    return 2;
  }
  const int port = ParsePort(argv[2]);
  if (port < 0) {
    std::cerr << "invalid port\n";
    return 2;
  }

  if (!ant_rpc::InitRuntime({.worker_threads = 1, .io_threads = 1})) {
    std::cerr << "runtime init failed\n";
    return 1;
  }
  ant_rpc::RpcChannel channel;
  const int init_result = channel.Init(std::string(argv[1]) + ":" + std::to_string(port));
  if (init_result != 0) {
    std::cerr << "channel init failed: " << init_result << '\n';
    ant_rpc::ShutdownRuntime();
    return 1;
  }

  ant_rpc::EchoService_Stub stub(&channel);
  ant_rpc::EchoRequest request;
  request.set_message("process-e2e");
  ant_rpc::EchoResponse response;
  ant_rpc::RpcController controller;
  stub.Echo(&controller, &request, &response, nullptr);

  const bool passed = !controller.Failed() && response.message() == "Echo: process-e2e";
  if (!passed) {
    std::cerr << "RPC failed: " << controller.ErrorCode() << " " << controller.ErrorText() << ", response='"
              << response.message() << "'\n";
  }
  channel.Close();
  if (!ant_rpc::ShutdownRuntime()) {
    std::cerr << "runtime shutdown failed\n";
    return 1;
  }
  if (!passed) {
    return 1;
  }
  std::cout << "OK process-e2e\n";
  return 0;
}
