#pragma once

#include "concurrentqueue/concurrentqueue.h"
#include "ant_rpc/type.hpp"

class ConcurrentGlobalQueue {
 public:
  ConcurrentGlobalQueue() = default;
  ~ConcurrentGlobalQueue() = default;

  ConcurrentGlobalQueue(const ConcurrentGlobalQueue&) = delete;
  ConcurrentGlobalQueue& operator=(const ConcurrentGlobalQueue&) = delete;
  ConcurrentGlobalQueue(ConcurrentGlobalQueue&&) = delete;
  ConcurrentGlobalQueue& operator=(ConcurrentGlobalQueue&&) = delete;

  // Single item push (Lock-free FIFO)
  void Push(TaskNode* node) {
    if (!node) {
      return;
    }
    node->next = nullptr;
    queue_.enqueue(node);
  }

  // Batch push: connects batch into queue
  void PushBatch(TaskNode* const* nodes, std::size_t count) {
    if (nodes && count > 0) {
      queue_.enqueue_bulk(nodes, count);
    }
  }

  // Compatible overload for linked list batch push
  void PushBatch(TaskNode* batch_head, TaskNode* batch_tail) {
    for (TaskNode* curr = batch_head; curr != nullptr; curr = curr->next) {
      queue_.enqueue(curr);
      if (curr == batch_tail) {
        break;
      }
    }
  }

  // Single item pop (Lock-free FIFO)
  TaskNode* Pop() {
    TaskNode* node = nullptr;
    if (queue_.try_dequeue(node)) {
      return node;
    }
    return nullptr;
  }

  // Batch pop (Lock-free bulk dequeue)
  std::size_t PopBatch(TaskNode** buffer, std::size_t max_items) {
    return queue_.try_dequeue_bulk(buffer, max_items);
  }

  bool empty() const {
    return queue_.size_approx() == 0;
  }

  std::size_t size_approx() const {
    return queue_.size_approx();
  }

 private:
  moodycamel::ConcurrentQueue<TaskNode*> queue_;
};
