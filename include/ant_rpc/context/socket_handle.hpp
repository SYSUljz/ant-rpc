#pragma once

#include <cstdint>

// A connection stores this semantic handle rather than assuming every integer
// is a process fd. io_uring direct accept returns a registered-file index;
// epoll always returns a native fd.
struct SocketHandle {
  enum class Kind : uint8_t { kNativeFd, kRegisteredFile };

  static constexpr SocketHandle Native(int fd) noexcept { return {fd, Kind::kNativeFd}; }
  static constexpr SocketHandle Registered(int index) noexcept { return {index, Kind::kRegisteredFile}; }
  [[nodiscard]] constexpr bool valid() const noexcept { return value >= 0; }

  int value {-1};
  Kind kind {Kind::kNativeFd};
};
