#pragma once

#if defined(ANT_RPC_USE_EPOLL)
#include "ant_rpc/awaiter/epoll_connect_awaiter.hpp"
#else
#include "ant_rpc/awaiter/io_uring_connect_awaiter.hpp"
#endif
