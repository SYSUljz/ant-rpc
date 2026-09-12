#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ant_rpc/rpc/rpc_server.hpp"
#include "ant_rpc/scheduler/scheduler.hpp"
#include "ant_rpc/testing/socket_fault_injector.hpp"
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

    // A process-test rendezvous: stdout is not part of the RPC protocol. It
    // lets the Python harness issue Stop only after this request is genuinely
    // in flight, rather than merely buffered in the TCP stack.
    if (request->message() == "graceful-slow") {
      std::cout << "CALL_STARTED" << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(120));
    }

    if (request->message() == "queue-blocker") {
      std::cout << "QUEUE_BLOCKER_STARTED" << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
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

bool ParseSize(const char* value, std::size_t& result) {
  if (value == nullptr || value[0] == '-') {
    return false;
  }
  errno = 0;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  if (errno == ERANGE || end == nullptr || *end != '\0' || parsed > std::numeric_limits<std::size_t>::max()) {
    return false;
  }
  result = static_cast<std::size_t>(parsed);
  return true;
}

struct E2eServerConfig {
  int port {0};
  int admin_port {0};
  std::size_t worker_threads {2};
  std::size_t max_pending_worker_tasks {8192};
  std::size_t max_outbound_bytes {16U * 1024U * 1024U};
  std::size_t max_write_bytes {0};
};

bool ParseArguments(int argc, char** argv, E2eServerConfig& config) {
  if ((argc - 1) % 2 != 0) {
    return false;
  }
  for (int index = 1; index < argc; index += 2) {
    const std::string option = argv[index];
    if (option == "--port") {
      config.port = ParsePort(argv[index + 1]);
      if (config.port < 0) {
        return false;
      }
    } else if (option == "--admin-port") {
      config.admin_port = ParsePort(argv[index + 1]);
      if (config.admin_port < 0) {
        return false;
      }
    } else if (option == "--worker-threads") {
      if (!ParseSize(argv[index + 1], config.worker_threads) || config.worker_threads == 0) {
        return false;
      }
    } else if (option == "--max-pending-worker-tasks") {
      if (!ParseSize(argv[index + 1], config.max_pending_worker_tasks) || config.max_pending_worker_tasks == 0) {
        return false;
      }
    } else if (option == "--max-outbound-bytes") {
      if (!ParseSize(argv[index + 1], config.max_outbound_bytes)) {
        return false;
      }
    } else if (option == "--max-write-bytes") {
      if (!ParseSize(argv[index + 1], config.max_write_bytes)) {
        return false;
      }
    } else {
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  ant_rpc::logging::Initialize();
  E2eServerConfig config;
  if (!ParseArguments(argc, argv, config)) {
    std::cerr << "invalid port\n";
    return 2;
  }

  Scheduler scheduler(config.worker_threads, 2);
  ant_rpc::testing::SocketFaultInjector::SetMaxWriteBytes(config.max_write_bytes);
  ant_rpc::RpcServer server(scheduler.GetIOContext(0), config.port, butil::IP_ANY,
                            ant_rpc::RpcServerOptions {.io_contexts = 2,
                                                       .max_pending_worker_tasks = config.max_pending_worker_tasks,
                                                       .max_outbound_bytes_per_connection = config.max_outbound_bytes,
                                                       .graceful_stop_timeout = std::chrono::milliseconds(500)});
  if (!server.AddService(std::make_shared<EchoService>())) {
    std::cerr << "failed to register Echo service\n";
    return 1;
  }
  if (!server.Start()) {
    std::cerr << "failed to start RPC server: " << errno << '\n';
    return 1;
  }

  ant_rpc::AdminServer admin_server(scheduler.GetIOContext(0), config.admin_port);
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
  std::cout << "STOPPING" << std::endl;
  server.Join();
  scheduler.Stop();
  return 0;
}
