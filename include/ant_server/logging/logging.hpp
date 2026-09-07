#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <source_location>
#include <sstream>
#include <string>
#include <string_view>

#include "absl/log/absl_log.h"
#include "absl/log/globals.h"
#include "absl/log/initialize.h"

namespace ant_server::logging {

enum class Event { kServerStarted, kServerStopped, kServerStartFailed, kConnectionFailed, kOverload, kSlowRpc, kCount };

struct Options {
  absl::LogSeverityAtLeast min_severity = absl::LogSeverityAtLeast::kInfo;
  uint64_t warning_interval_ms = 1000;
  uint64_t slow_rpc_us = 100000;
};

// Fixed event cardinality; no per-request map or allocations for throttling.
class RateLimiter {
 public:
  bool Allow(uint64_t now_ms, uint64_t interval_ms) {
    uint64_t next = next_ms_.load(std::memory_order_relaxed);
    while (now_ms >= next) {
      if (next_ms_.compare_exchange_weak(next, now_ms + interval_ms, std::memory_order_relaxed)) {
        return true;
      }
    }
    suppressed_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  uint64_t TakeSuppressed() { return suppressed_.exchange(0, std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> next_ms_ {0};
  std::atomic<uint64_t> suppressed_ {0};
};

inline std::atomic<bool> enabled {false};
inline std::atomic<uint64_t> warning_interval_ms {1000};
inline std::atomic<uint64_t> slow_rpc_us {100000};
inline std::array<RateLimiter, static_cast<std::size_t>(Event::kCount)> limiters;

inline uint64_t NextConnectionId() {
  static std::atomic<uint64_t> next {1};
  return next.fetch_add(1, std::memory_order_relaxed);
}

// Call once at application startup. If the application already initialized
// Abseil, pass false. Abseil configuration affects the entire process.
inline void Initialize(Options options = {}, bool initialize_abseil = true) {
  if (initialize_abseil) {
    static std::once_flag once;
    std::call_once(once, [] { absl::InitializeLog(); });
  }
  absl::SetMinLogLevel(options.min_severity);
  absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfo);
  warning_interval_ms.store(options.warning_interval_ms);
  slow_rpc_us.store(options.slow_rpc_us);
  enabled.store(true, std::memory_order_release);
}

// Quote untrusted strings so a peer cannot inject additional log lines.
inline std::string Quote(std::string_view text) {
  std::string result = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : text) {
    if (c == '"' || c == '\\') {
      result += '\\';
    }
    if (c < 32 || c == 127) {
      result += "\\x";
      result += hex[c >> 4];
      result += hex[c & 15];
    } else {
      result += static_cast<char>(c);
    }
  }
  return result + '"';
}

template <typename Fields>
void Write(Event event, Fields&& fields, std::source_location location = std::source_location::current()) {
  if (!enabled.load(std::memory_order_acquire)) {
    return;
  }
  const auto index = static_cast<std::size_t>(event);
  constexpr std::array names {"server_started",    "server_stopped", "server_start_failed",
                              "connection_failed", "overload",       "slow_rpc"};
  if (index >= names.size()) {
    return;
  }
  const auto severity = event == Event::kServerStartFailed ? absl::LogSeverity::kError
                        : index < 2                        ? absl::LogSeverity::kInfo
                                                           : absl::LogSeverity::kWarning;
  if (severity < absl::MinLogLevel()) {
    return;
  }
  uint64_t suppressed = 0;
  if (severity == absl::LogSeverity::kWarning) {
    const auto now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    if (!limiters[index].Allow(static_cast<uint64_t>(now), warning_interval_ms.load())) {
      return;
    }
    suppressed = limiters[index].TakeSuppressed();
  }
  std::ostringstream message;
  message << "event=" << names[index];
  fields(message);
  if (suppressed) {
    message << " suppressed=" << suppressed;
  }
  ABSL_LOG(LEVEL(severity)).AtLocation(location.file_name(), static_cast<int>(location.line())) << message.str();
}

}  // namespace ant_server::logging
