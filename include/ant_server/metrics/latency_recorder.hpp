#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>

namespace ant_server::metrics {

struct LatencySnapshot {
  uint64_t count {0};
  uint64_t total_microseconds {0};
  uint64_t min_microseconds {0};
  uint64_t max_microseconds {0};
  uint64_t average_microseconds {0};

  [[nodiscard]] bool empty() const noexcept { return count == 0; }
};

// A process-lifetime latency summary. It intentionally does not calculate
// percentiles or use a time window; those need a later registry/sampling
// design. Snapshot() is thread-safe but weakly consistent across fields, which
// is appropriate for observability rather than control flow.
class LatencyRecorder {
 public:
  void Record(std::chrono::microseconds latency) noexcept {
    if (latency.count() < 0) {
      return;
    }
    RecordMicroseconds(static_cast<uint64_t>(latency.count()));
  }

  void RecordMicroseconds(uint64_t latency) noexcept {
    AddToTotal(latency);
    UpdateMin(latency);
    UpdateMax(latency);
    count_.fetch_add(1, std::memory_order_release);
  }

  [[nodiscard]] LatencySnapshot Snapshot() const noexcept {
    const uint64_t count = count_.load(std::memory_order_acquire);
    if (count == 0) {
      return {};
    }

    const uint64_t total = total_microseconds_.load(std::memory_order_relaxed);
    return {
        .count = count,
        .total_microseconds = total,
        .min_microseconds = min_microseconds_.load(std::memory_order_relaxed),
        .max_microseconds = max_microseconds_.load(std::memory_order_relaxed),
        .average_microseconds = total / count,
    };
  }

 private:
  void AddToTotal(uint64_t latency) noexcept {
    uint64_t current = total_microseconds_.load(std::memory_order_relaxed);
    while (true) {
      const uint64_t next = std::numeric_limits<uint64_t>::max() - current < latency
                                ? std::numeric_limits<uint64_t>::max()
                                : current + latency;
      if (total_microseconds_.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                                     std::memory_order_relaxed)) {
        return;
      }
    }
  }

  void UpdateMin(uint64_t latency) noexcept {
    uint64_t current = min_microseconds_.load(std::memory_order_relaxed);
    while (latency < current &&
           !min_microseconds_.compare_exchange_weak(current, latency, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
    }
  }

  void UpdateMax(uint64_t latency) noexcept {
    uint64_t current = max_microseconds_.load(std::memory_order_relaxed);
    while (latency > current &&
           !max_microseconds_.compare_exchange_weak(current, latency, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
    }
  }

  std::atomic<uint64_t> count_ {0};
  std::atomic<uint64_t> total_microseconds_ {0};
  std::atomic<uint64_t> min_microseconds_ {std::numeric_limits<uint64_t>::max()};
  std::atomic<uint64_t> max_microseconds_ {0};
};

}  // namespace ant_server::metrics
