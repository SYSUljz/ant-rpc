#include <cstdlib>
#include <iostream>
#include <string>

#include "ant_server/rpc/channel.hpp"
#include "ant_server/rpc/controller.hpp"
#include "ant_server/scheduler/scheduler.hpp"
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

  Scheduler scheduler(1, 1);
  scheduler.Start();
  ant_rpc::RpcChannel channel(scheduler.GetIOContext(0));
  const int init_result = channel.Init(argv[1], port);
  if (init_result != 0) {
    std::cerr << "channel init failed: " << init_result << '\n';
    scheduler.Stop();
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
  scheduler.Stop();
  if (!passed) {
    return 1;
  }
  std::cout << "OK process-e2e\n";
  return 0;
}
