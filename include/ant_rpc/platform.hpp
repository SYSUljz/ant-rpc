#pragma once

#include <cstddef>
#include <cstdio>

#if defined(__linux__)
#include <pthread.h>
#endif

namespace ant_rpc::platform {

// Linux comm names are limited to 15 visible characters. Keep the prefixes
// short so even a five-digit benchmark worker index remains distinguishable.
inline void SetCurrentThreadName(const char* name) noexcept {
#if defined(__linux__)
  (void)pthread_setname_np(pthread_self(), name);
#else
  (void)name;
#endif
}

inline void SetIndexedThreadName(const char* prefix, std::size_t index) noexcept {
#if defined(__linux__)
  char name[16];
  const int length = std::snprintf(name, sizeof(name), "%s-%zu", prefix, index);
  if (length > 0 && static_cast<std::size_t>(length) < sizeof(name)) {
    SetCurrentThreadName(name);
  }
#else
  (void)prefix;
  (void)index;
#endif
}

}  // namespace ant_rpc::platform
