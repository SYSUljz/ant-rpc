#pragma once

#include <atomic>
#include <cstdint>

namespace ant_server::metrics {

// A monotonically increasing, thread-safe counter. Counters deliberately have
// no reset operation: a future registry can safely expose them as process-life
// totals.
class Counter {
 public:
  void Add(uint64_t delta = 1) noexcept { value_.fetch_add(delta, std::memory_order_relaxed); }

  [[nodiscard]] uint64_t Value() const noexcept { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> value_ {0};
};

// A thread-safe signed value for quantities that may rise and fall, such as
// active connections or queued outbound bytes.
class Gauge {
 public:
  void Set(int64_t value) noexcept { value_.store(value, std::memory_order_relaxed); }

  void Add(int64_t delta) noexcept { value_.fetch_add(delta, std::memory_order_relaxed); }

  [[nodiscard]] int64_t Value() const noexcept { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> value_ {0};
};

}  // namespace ant_server::metrics
