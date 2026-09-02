#include <atomic>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "ant_server/rpc/channel.hpp"
#include "ant_server/rpc/controller.hpp"
#include "echo.pb.h"

namespace {

int ParsePort(const char* value) {
  char* end = nullptr;
  const long port = std::strtol(value, &end, 10);
  return end && *end == '\0' && port > 0 && port <= 65535 ? static_cast<int>(port) : -1;
}

int ParsePositiveInt(const char* value) {
  char* end = nullptr;
  const long result = std::strtol(value, &end, 10);
  return end && *end == '\0' && result > 0 && result <= 1'000'000 ? static_cast<int>(result) : -1;
}

uint64_t ParseSeed(const char* value) {
  char* end = nullptr;
  const unsigned long long result = std::strtoull(value, &end, 10);
  return end && *end == '\0' ? static_cast<uint64_t>(result) : 0;
}

std::string RandomMessage(std::mt19937_64& generator, int request_index) {
  constexpr char kAlphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<std::size_t> length(0, 512);
  std::uniform_int_distribution<std::size_t> character(0, sizeof(kAlphabet) - 2);
  std::string message = "request-" + std::to_string(request_index) + "-";
  message.reserve(message.size() + length(generator));
  const std::size_t suffix_length = length(generator);
  for (std::size_t index = 0; index < suffix_length; ++index) {
    message += kAlphabet[character(generator)];
  }
  return message;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3 && argc != 9) {
    std::cerr << "usage: rpc_e2e_client HOST PORT [--requests N --threads N --seed N]\n";
    return 2;
  }
  const int port = ParsePort(argv[2]);
  if (port < 0) {
    std::cerr << "invalid port\n";
    return 2;
  }

  int requests = 1;
  int threads = 1;
  uint64_t seed = 1;
  if (argc == 9) {
    if (std::string(argv[3]) != "--requests" || std::string(argv[5]) != "--threads" ||
        std::string(argv[7]) != "--seed") {
      std::cerr << "invalid options\n";
      return 2;
    }
    requests = ParsePositiveInt(argv[4]);
    threads = ParsePositiveInt(argv[6]);
    seed = ParseSeed(argv[8]);
    if (requests < 0 || threads < 0 || seed == 0) {
      std::cerr << "invalid random workload options\n";
      return 2;
    }
  }

  if (!ant_rpc::InitRuntime({.worker_threads = static_cast<std::size_t>(threads), .io_threads = 2})) {
    std::cerr << "runtime init failed\n";
    return 1;
  }
  std::atomic<int> next_request {0};
  std::atomic<int> failed {0};
  std::vector<std::thread> workers;
  workers.reserve(static_cast<std::size_t>(threads));
  for (int thread_index = 0; thread_index < threads; ++thread_index) {
    workers.emplace_back([&, thread_index] {
      ant_rpc::RpcChannel channel;
      const int init_result = channel.Init(argv[1], port);
      if (init_result != 0) {
        std::cerr << "thread " << thread_index << " channel init failed: " << init_result << '\n';
        failed.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      ant_rpc::EchoService_Stub stub(&channel);
      std::mt19937_64 generator(seed + static_cast<uint64_t>(thread_index));
      while (true) {
        const int request_index = next_request.fetch_add(1, std::memory_order_relaxed);
        if (request_index >= requests) {
          break;
        }
        ant_rpc::EchoRequest request;
        ant_rpc::EchoResponse response;
        ant_rpc::RpcController controller;
        const std::string message = RandomMessage(generator, request_index);
        request.set_message(message);
        stub.Echo(&controller, &request, &response, nullptr);
        if (controller.Failed() || response.message() != "Echo: " + message) {
          std::cerr << "request " << request_index << " on thread " << thread_index
                    << " failed: " << controller.ErrorCode() << " " << controller.ErrorText() << ", response='"
                    << response.message() << "'\n";
          failed.fetch_add(1, std::memory_order_relaxed);
        }
      }
      channel.Close();
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  if (!ant_rpc::ShutdownRuntime()) {
    std::cerr << "runtime shutdown failed\n";
    return 1;
  }
  if (failed.load(std::memory_order_acquire) != 0) {
    std::cerr << "random workload had " << failed.load(std::memory_order_relaxed) << " failed RPCs\n";
    return 1;
  }
  std::cout << "OK random-e2e requests=" << requests << " threads=" << threads << "\n";
  return 0;
}
