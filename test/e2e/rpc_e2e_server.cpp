#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "ant_server/rpc/rpc_server.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "echo.pb.h"

namespace {

class EchoService final : public ant_rpc::EchoService {
 public:
  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request,
            ant_rpc::EchoResponse* response, google::protobuf::Closure* done) override {
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }
};

int ParsePort(const char* value) {
  char* end = nullptr;
  const long port = std::strtol(value, &end, 10);
  return end && *end == '\0' && port >= 0 && port <= 65535 ? static_cast<int>(port) : -1;
}

}  // namespace

int main(int argc, char** argv) {
  int requested_port = 0;
  if (argc == 3 && std::string(argv[1]) == "--port") {
    requested_port = ParsePort(argv[2]);
  } else if (argc != 1) {
    std::cerr << "usage: rpc_e2e_server [--port PORT]\n";
    return 2;
  }
  if (requested_port < 0) {
    std::cerr << "invalid port\n";
    return 2;
  }

  Scheduler scheduler(1, 1);
  ant_rpc::RpcServer server(scheduler.GetIOContext(0), requested_port, butil::IP_ANY);
  if (server.GetSocketFd() < 0) {
    std::cerr << "failed to listen: " << errno << '\n';
    return 1;
  }
  if (!server.AddService(std::make_shared<EchoService>())) {
    std::cerr << "failed to register Echo service\n";
    return 1;
  }
  if (!server.Start()) {
    std::cerr << "failed to start RPC server\n";
    return 1;
  }

  scheduler.Start();
  std::cout << "READY " << server.GetEndPoint().port << std::endl;

  // The Python harness owns this process and closes stdin to request orderly
  // shutdown. A production server would instead use its signal/admin path.
  std::string command;
  std::getline(std::cin, command);
  server.Stop();
  server.Join();
  scheduler.Stop();
  return 0;
}
