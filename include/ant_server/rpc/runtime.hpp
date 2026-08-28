#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>

#include "ant_server/constants.hpp"
#include "ant_server/scheduler/scheduler.hpp"

namespace ant_server::rpc {

// Process-wide execution resources used by the default RpcChannel API.
// Channel-specific behavior (timeouts, framing, TCP options) belongs in
// RpcChannelOptions, not here.
struct RuntimeOptions {
  std::size_t worker_threads {std::thread::hardware_concurrency() > 2 ? std::thread::hardware_concurrency() - 2 : 1};
  std::size_t io_threads {std::thread::hardware_concurrency() > 2 ? std::size_t {2} : std::size_t {1}};
  std::size_t uring_entries {ant_server::constants::kDefaultServerUringSize};

  [[nodiscard]] bool IsValid() const noexcept { return worker_threads > 0 && io_threads > 0 && uring_entries > 0; }
};

// Runtime owns the scheduler rather than exposing its Contexts publicly. It
// is public to permit isolated tests and embedded applications; ordinary code
// should use InitRuntime() and RpcChannel's default constructor.
class Runtime {
 public:
  explicit Runtime(const RuntimeOptions& options)
      : scheduler_(options.worker_threads, options.io_threads, options.uring_entries) {}

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  void Start() { scheduler_.Start(); }
  void Shutdown() { scheduler_.Stop(); }
  [[nodiscard]] bool IsRunning() const noexcept { return scheduler_.IsRunning(); }

  // Internal channel-placement policy. A channel remains pinned to the
  // selected Context for its complete lifetime.
  Context& AcquireChannelContext() { return scheduler_.GetBalancedIOContext(); }

 private:
  Scheduler scheduler_;
};

namespace detail {

inline std::mutex default_runtime_mu;
inline std::shared_ptr<Runtime> default_runtime;

}  // namespace detail

// Starts the one process-default RPC runtime. It must be called before a
// default-constructed RpcChannel is initialized. Returns false for invalid
// options or if a runtime is already active.
inline bool InitRuntime(const RuntimeOptions& options = {}) {
  if (!options.IsValid()) {
    return false;
  }

  std::lock_guard<std::mutex> lock(detail::default_runtime_mu);
  if (detail::default_runtime) {
    return false;
  }
  auto runtime = std::make_shared<Runtime>(options);
  runtime->Start();
  detail::default_runtime = std::move(runtime);
  return true;
}

namespace detail {

// Returns a strong reference so a default RpcChannel can keep the Runtime
// alive while it is bound to one of its IO Contexts.
inline std::shared_ptr<Runtime> AcquireDefaultRuntime() {
  std::lock_guard<std::mutex> lock(default_runtime_mu);
  return default_runtime;
}

}  // namespace detail

inline bool IsRuntimeInitialized() {
  std::lock_guard<std::mutex> lock(detail::default_runtime_mu);
  return static_cast<bool>(detail::default_runtime);
}

// A running default channel holds an additional Runtime reference. Refusing
// shutdown in that case prevents a channel from retaining a dangling Context.
// Close or destroy every default channel before calling ShutdownRuntime().
inline bool ShutdownRuntime() {
  std::shared_ptr<Runtime> runtime;
  {
    std::lock_guard<std::mutex> lock(detail::default_runtime_mu);
    if (!detail::default_runtime || detail::default_runtime.use_count() != 1) {
      return false;
    }
    runtime = std::move(detail::default_runtime);
  }
  runtime->Shutdown();
  return true;
}

}  // namespace ant_server::rpc
