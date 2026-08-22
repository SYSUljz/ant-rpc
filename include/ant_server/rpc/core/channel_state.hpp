#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "ant_server/rpc/slot_table.hpp"

namespace ant_server::rpc::detail {

// State shared by the public channel facade and its IO-thread driver.
struct ChannelState {
  SlotTable<65536> slots;
  std::atomic<bool> running {false};

  std::mutex close_mu;
  std::condition_variable close_cv;
  bool closed {true};
};

}  // namespace ant_server::rpc::detail
