#pragma once

#include <atomic>
#include <cstddef>

// Test-only deterministic socket fault controls. Targets opt in through
// ANT_RPC_ENABLE_TEST_SOCKET_FAULT_INJECTION; production targets neither
// include nor pay for this hook.
namespace ant_rpc::testing {

class SocketFaultInjector final {
 public:
  static void SetMaxWriteBytes(std::size_t bytes) noexcept { max_write_bytes_.store(bytes, std::memory_order_release); }

  [[nodiscard]] static std::size_t MaxWriteBytes() noexcept { return max_write_bytes_.load(std::memory_order_acquire); }

 private:
  inline static std::atomic<std::size_t> max_write_bytes_ {0};
};

}  // namespace ant_rpc::testing
