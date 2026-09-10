#pragma once

// The selected Context is a compile-time decision. Do not introduce a common
// Read/Write backend interface here: epoll is readiness based while io_uring
// is completion based.
#if defined(ANT_SERVER_USE_EPOLL) == defined(ANT_SERVER_USE_IO_URING)
#error "Define exactly one of ANT_SERVER_USE_EPOLL or ANT_SERVER_USE_IO_URING"
#endif

#if defined(ANT_SERVER_USE_EPOLL)
#include "ant_server/context/epoll_context.hpp"
class Context final : public EpollContext {
 public:
  using EpollContext::EpollContext;
};
#else
#include "ant_server/context/io_uring_context.hpp"
class Context final : public IoUringContext {
 public:
  using IoUringContext::IoUringContext;
};
#endif
