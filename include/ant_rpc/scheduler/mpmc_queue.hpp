#pragma once

#include "absl/base/internal/spinlock.h"
#include "ant_rpc/type.hpp"

class IntrusiveSpinLockQueue {
 public:
  IntrusiveSpinLockQueue() = default;
  ~IntrusiveSpinLockQueue() = default;

  IntrusiveSpinLockQueue(const IntrusiveSpinLockQueue&) = delete;
  IntrusiveSpinLockQueue& operator=(const IntrusiveSpinLockQueue&) = delete;
  IntrusiveSpinLockQueue(IntrusiveSpinLockQueue&&) = delete;
  IntrusiveSpinLockQueue& operator=(IntrusiveSpinLockQueue&&) = delete;

  // Single item push (FIFO)
  void Push(TaskNode* node) {
    if (!node) {
      return;
    }
    node->next = nullptr;

    absl::base_internal::SpinLockHolder holder(&lock_);
    if (!tail_) {
      head_ = node;
      tail_ = node;
    } else {
      tail_->next = node;
      tail_ = node;
    }
  }

  // Batch push: connects batch_head -> ... -> batch_tail into queue tail in O(1)
  void PushBatch(TaskNode* batch_head, TaskNode* batch_tail) {
    if (!batch_head || !batch_tail) {
      return;
    }
    batch_tail->next = nullptr;

    absl::base_internal::SpinLockHolder holder(&lock_);
    if (!tail_) {
      head_ = batch_head;
      tail_ = batch_tail;
    } else {
      tail_->next = batch_head;
      tail_ = batch_tail;
    }
  }

  // Single item pop (FIFO)
  TaskNode* Pop() {
    absl::base_internal::SpinLockHolder holder(&lock_);
    if (!head_) {
      return nullptr;
    }

    TaskNode* node = head_;
    head_ = head_->next;
    if (!head_) {
      tail_ = nullptr;
    }

    node->next = nullptr;
    return node;
  }

  bool empty() const {
    absl::base_internal::SpinLockHolder holder(&lock_);
    return head_ == nullptr;
  }

 private:
  TaskNode* head_ {nullptr};
  TaskNode* tail_ {nullptr};
  mutable absl::base_internal::SpinLock lock_;
};
