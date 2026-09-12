#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

namespace ant_rpc::constants {

// Cache line alignment for hardware interference
#if defined(__cpp_lib_hardware_interference_size)
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winterference-size"
#endif
inline constexpr std::size_t kCacheLineSize = std::hardware_destructive_interference_size;
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#else
inline constexpr std::size_t kCacheLineSize = 64;
#endif

// Scheduler & Queue constants
inline constexpr std::size_t kDefaultSpmcCapacity = 256;
inline constexpr std::size_t kGlobalCheckInterval = 61;

// io_uring & Context constants
inline constexpr std::size_t kDefaultUringQueueSize = 256;

// Provided Buffer Ring Service constants
inline constexpr std::size_t kNumProvidedBuffers = 1024;
inline constexpr std::size_t kProvidedBufferSize = 4096;
inline constexpr uint16_t kProvidedBufferGroupId = 1;

// Timing Wheel & TimerKeeper constants
inline constexpr std::size_t kDefaultTimingWheelSlots = 4096;
inline constexpr std::size_t kDefaultTimingWheelSlotMask = kDefaultTimingWheelSlots - 1;
inline constexpr int64_t kDefaultTimingWheelTickMs = 10;
inline constexpr std::size_t kDefaultTimerNumBuckets = 16;
inline constexpr std::size_t kDefaultTimerBucketMask = kDefaultTimerNumBuckets - 1;
inline constexpr std::size_t kDefaultTimerHeapReserveSize = 4096;

// TypeErasedTask constants
inline constexpr std::size_t kTaskSboSize = 48;

// HTTP Server constants
inline constexpr std::size_t kDefaultHttpBufferSize = 16000;
inline constexpr int kDefaultServerUringSize = 256;

}  // namespace ant_rpc::constants

// Global backward compatibility alias for kCacheLineSize
using ant_rpc::constants::kCacheLineSize;
