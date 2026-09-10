#pragma once

#if defined(ANT_SERVER_USE_EPOLL)
#include "ant_server/awaiter/epoll_socket_awaiter.hpp"
#else
#include "ant_server/awaiter/io_uring_socket_awaiter.hpp"
#endif
