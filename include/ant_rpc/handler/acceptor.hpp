#pragma once

#if defined(ANT_RPC_USE_EPOLL)
#include "ant_rpc/handler/epoll_acceptor.hpp"
#else
#include "ant_rpc/handler/io_uring_acceptor.hpp"
#endif
