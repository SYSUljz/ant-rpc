#pragma once

#if defined(ANT_SERVER_USE_EPOLL)
#include "ant_server/awaiter/epoll_connect_awaiter.hpp"
#else
#include "ant_server/awaiter/io_uring_connect_awaiter.hpp"
#endif
