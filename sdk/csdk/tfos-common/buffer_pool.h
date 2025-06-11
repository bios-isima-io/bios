/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Manages a pool of buffers used for requests and responses.
 *
 * Not thread safe as it is assumed a single thread does allocation and removal
 */
#ifndef TFOSCSDK_POOLS_BUFFER_POOL_H_
#define TFOSCSDK_POOLS_BUFFER_POOL_H_

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "tfoscsdk/properties.h"

namespace tfos::csdk {

template <size_t M = Properties::DEFAULT_BUFFER_SIZE>
union alignas(CACHE_ALIGNMENT_SIZE) Buffer {
 public:
  Buffer<M> *GetNext() const { return next_; }

  void SetNext(Buffer<M> *next) { next_ = next; }

  uint8_t *GetAsItem() const { return (uint8_t *)item_; }

  static Buffer<M> *GetAsBuffer(uint8_t *item) { return reinterpret_cast<Buffer<M> *>(item); }

 private:
  // next node when in free list
  Buffer *next_;
  // actual item
  uint8_t item_[M];
};

template <size_t M = Properties::DEFAULT_BUFFER_SIZE, size_t N = Properties::NUM_BUFFERS_PER_POOL>
class BufferPool {
 public:
  BufferPool() : ptr_storage_area_(new Buffer<M>[N]) {
    // create the free list
    for (uint32_t i = 1; i < N; i++) {
      ptr_storage_area_[i - 1].SetNext(&ptr_storage_area_[i]);
    }
    ptr_storage_area_[N - 1].SetNext(nullptr);
    free_list_ = &ptr_storage_area_[0];
  }
  ~BufferPool() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(BufferPool);

  uint8_t *GetBuffer() {
    // this will not happen due to the reactive nature of CSDK. Unless there is
    // a bug So assert if it does
    assert(free_list_ != nullptr);
    Buffer<M> *current_item = free_list_;
    free_list_ = free_list_->GetNext();
    allocated_pool_.insert(current_item);
    return current_item->GetAsItem();
  }

  void ReturnBuffer(uint8_t *buf) {
    Buffer<M> *buf_ptr = Buffer<M>::GetAsBuffer(buf);
    int i = allocated_pool_.erase(buf_ptr);
    if (i == 0) {
      // TODO(ramesh) : Log warning
    } else {
      buf_ptr->SetNext(free_list_);
      free_list_ = buf_ptr;
    }
  }

  void ReleasePool() {
    if (allocated_pool_.empty()) {
      return;
    }
    // TODO(ramesh) : Log warning that there are still pending requests
    // this should happen iff and only if it is a forced close..
    Buffer<M> *prev = nullptr;
    for (auto cur : allocated_pool_) {
      if (prev == nullptr) {
        cur->SetNext(free_list_);
        free_list_ = cur;
      } else {
        prev->SetNext(cur);
      }
      prev = cur;
    }
  }

 private:
  Buffer<M> *free_list_;
  std::unique_ptr<Buffer<M>[]> ptr_storage_area_;

  std::unordered_set<Buffer<M> *> allocated_pool_;
};

}  // namespace tfos::csdk

#endif  // TFOSCSDK_POOLS_BUFFER_POOL_H_
