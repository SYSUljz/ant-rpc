#pragma once

#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <unordered_map>

#include <sys/epoll.h>

#include "ant_rpc/context/context_control.hpp"

// A readiness reactor. It stores only who is waiting for readiness; socket
// awaiters retain buffers and retry recv/send themselves after notification.
class EpollContext : public ContextControl {
 public:
  EpollContext(std::size_t, Scheduler& scheduler, Executor& executor, TimerKeeper& timer_keeper)
      : ContextControl(scheduler, executor, timer_keeper), epoll_fd_(epoll_create1(EPOLL_CLOEXEC)) {}
  ~EpollContext() override {
    if (epoll_fd_ >= 0) {
      close(epoll_fd_);
    }
  }

  void RegisterReadable(int fd, IOHandler* handler) { Register(fd, handler, nullptr); }
  void RegisterWritable(int fd, IOHandler* handler) { Register(fd, nullptr, handler); }
  bool CancelWait(IOHandler* handler) {
    bool removed = false;
    for (auto it = waits_.begin(); it != waits_.end();) {
      if (it->second.readable == handler) {
        it->second.readable = nullptr;
        removed = true;
      }
      if (it->second.writable == handler) {
        it->second.writable = nullptr;
        removed = true;
      }
      if (!it->second.readable && !it->second.writable) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->first, nullptr);
        it = waits_.erase(it);
      } else {
        UpdateInterest(it->first, it->second);
        ++it;
      }
    }
    return removed;
  }

  int ProcessEvents(int = 1) {
    EnsureWakeup();
    epoll_event events[64];
    const int count = epoll_wait(epoll_fd_, events, 64, -1);
    if (count < 0) {
      return errno == EINTR ? -EINTR : -errno;
    }
    for (int i = 0; i < count; ++i) {
      if (events[i].data.u64 == kWakeupTag) {
        DrainMailboxesOnIoThread();
        continue;
      }
      const int fd = static_cast<int>(events[i].data.u64 >> 1);
      auto it = waits_.find(fd);
      if (it == waits_.end()) {
        continue;
      }
      IOHandler* readable = nullptr;
      IOHandler* writable = nullptr;
      const uint32_t flags = events[i].events;
      if (flags & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        readable = it->second.readable;
        it->second.readable = nullptr;
      }
      if (flags & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
        writable = it->second.writable;
        it->second.writable = nullptr;
      }
      if (!it->second.readable && !it->second.writable) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        waits_.erase(it);
      } else {
        UpdateInterest(fd, it->second);
      }
      if (readable) {
        readable->prepare_complete(0, flags);
        readable->on_complete();
      }
      if (writable && writable != readable) {
        writable->prepare_complete(0, flags);
        writable->on_complete();
      }
    }
    return count;
  }
  void Stop() { Wakeup(); }

 private:
  struct Waiters {
    IOHandler* readable {nullptr};
    IOHandler* writable {nullptr};
    bool armed {false};
  };
  static constexpr uint64_t kWakeupTag = 1;
  void EnsureWakeup() {
    if (wakeup_armed_) {
      return;
    }
    epoll_event event {};
    event.events = EPOLLIN;
    event.data.u64 = kWakeupTag;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd(), &event) == 0) {
      wakeup_armed_ = true;
    }
  }
  void Register(int fd, IOHandler* readable, IOHandler* writable) {
    auto& waiters = waits_[fd];
    if (readable) {
      waiters.readable = readable;
    }
    if (writable) {
      waiters.writable = writable;
    }
    UpdateInterest(fd, waiters);
  }
  void UpdateInterest(int fd, Waiters& waiters) {
    epoll_event event {};
    event.events = EPOLLRDHUP | (waiters.readable ? EPOLLIN : 0) | (waiters.writable ? EPOLLOUT : 0);
    event.data.u64 = static_cast<uint64_t>(static_cast<uint32_t>(fd)) << 1;
    const int op = waiters.armed ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (epoll_ctl(epoll_fd_, op, fd, &event) == 0) {
      waiters.armed = true;
    }
  }

  int epoll_fd_ {-1};
  bool wakeup_armed_ {false};
  std::unordered_map<int, Waiters> waits_;
};
