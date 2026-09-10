#pragma once

#if defined(ANT_SERVER_USE_EPOLL)
#include "ant_server/handler/epoll_acceptor.hpp"
#else
#include "ant_server/handler/io_uring_acceptor.hpp"
#endif
