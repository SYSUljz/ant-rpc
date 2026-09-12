#pragma once

#include <cstdint>

namespace ant_rpc {
namespace utils {

// Ultra-fast lock-free thread-local pseudo-random number generator for P2C load sampling
inline uint32_t fast_random() {
  thread_local uint32_t x = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(&x)) ^ 0x9e3779b9;
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  return x;
}

}  // namespace utils
using utils::fast_random;
}  // namespace ant_rpc
