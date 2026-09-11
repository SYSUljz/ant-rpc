#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ant_server/rpc/rpc_server.hpp"
#include "ant_server/scheduler/scheduler.hpp"
#include "echo.pb.h"

namespace {

class EchoService final : public ant_rpc::EchoService {
 public:
  ~EchoService() override { JoinAsyncWorkers(); }

  void Echo(google::protobuf::RpcController*, const ant_rpc::EchoRequest* request, ant_rpc::EchoResponse* response,
            google::protobuf::Closure* done) override {
    // This deliberately completes on a native thread rather than the server
    // worker. The E2E harness verifies both the delayed response and that a
    // duplicate done->Run() is reduced to one wire response.
    if (request->message().starts_with("async:")) {
      const std::string message = request->message();
      std::lock_guard<std::mutex> lock(async_workers_mu_);
      async_workers_.emplace_back([message, response, done] {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        response->set_message("Async: " + message);
        done->Run();
        done->Run();
      });
      return;
    }

    // The process test uses this controlled delay to prove that ServerConnection
    // keeps receiving one TCP connection while an earlier worker call runs.
    if (request->message().starts_with("sleep:")) {
      const auto milliseconds = std::strtol(request->message().c_str() + 6, nullptr, 10);
      if (milliseconds > 0 && milliseconds <= 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
      }
    }
    response->set_message("Echo: " + request->message());
    if (done) {
      done->Run();
    }
  }

 private:
  void JoinAsyncWorkers() {
    std::vector<std::thread> workers;
    {
      std::lock_guard<std::mutex> lock(async_workers_mu_);
      workers.swap(async_workers_);
    }
    for (auto& worker : workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  std::mutex async_workers_mu_;
  std::vector<std::thread> async_workers_;
};

int ParsePort(const char* value) {
  char* end = nullptr;
  const long port = std::strtol(value, &end, 10);
  return end && *end == '\0' && port >= 0 && port <= 65535 ? static_cast<int>(port) : -1;
}

}  // namespace

int main(int argc, char** argv) {
  ant_server::logging::Initialize();
  int requested_port = 0;
  int requested_admin_port = 0;
  if (argc == 3 && std::string(argv[1]) == "--port") {
    requested_port = ParsePort(argv[2]);
  } else if (argc == 5 && std::string(argv[1]) == "--port" && std::string(argv[3]) == "--admin-port") {
    requested_port = ParsePort(argv[2]);
    requested_admin_port = ParsePort(argv[4]);
  } else if (argc != 1) {
    std::cerr << "usage: rpc_e2e_server [--port PORT --admin-port PORT]\n";
    return 2;
  }
  if (requested_port < 0 || requested_admin_port < 0) {
    std::cerr << "invalid port\n";
    return 2;
  }

  Scheduler scheduler(2, 2);
  ant_rpc::RpcServer server(scheduler.GetIOContext(0), requested_port, butil::IP_ANY,
                            ant_rpc::RpcServerOptions {.io_contexts = 2});
  if (!server.AddService(std::make_shared<EchoService>())) {
    std::cerr << "failed to register Echo service\n";
    return 1;
  }
  if (!server.Start()) {
    std::cerr << "failed to start RPC server: " << errno << '\n';
    return 1;
  }

  ant_rpc::AdminServer admin_server(scheduler.GetIOContext(0), requested_admin_port);
  if (!admin_server.AddServer("e2e", server) || !admin_server.Start()) {
    std::cerr << "failed to start admin server: " << errno << '\n';
    server.Stop();
    server.Join();
    return 1;
  }

  scheduler.Start();
  std::cout << "READY " << server.GetEndPoint().port << " " << admin_server.GetEndPoint().port << std::endl;

  // The Python harness owns this process and closes stdin to request orderly
  // shutdown. A production server would instead use its signal/admin path.
  std::string command;
  std::getline(std::cin, command);
  admin_server.Stop();
  server.Stop();
  server.Join();
  scheduler.Stop();
  return 0;
}
