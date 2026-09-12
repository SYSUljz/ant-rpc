#pragma once

#if defined(ANT_RPC_USE_EPOLL)
#include "ant_rpc/awaiter/epoll_socket_awaiter.hpp"
#else
#include "ant_rpc/awaiter/io_uring_socket_awaiter.hpp"
#endif
