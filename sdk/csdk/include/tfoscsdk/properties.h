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
 * All CSDK changeable config properties goes here for now. In the future this
 * may be dynamically loaded from a config file, to size the installations
 * appropriately based on the form factor of the device for embedded usage.
 */
#ifndef TFOSCSDK_COMMON_PROPERTIES_H_
#define TFOSCSDK_COMMON_PROPERTIES_H_

#define DISABLE_COPY_MOVE_AND_ASSIGN(TypeName) \
  TypeName(const TypeName &) = delete;         \
  TypeName(const TypeName &&) = delete;        \
  void operator=(const TypeName &) = delete

#ifndef CACHE_ALIGNMENT_SIZE
#define CACHE_ALIGNMENT_SIZE 64
#endif

#include <cstddef>

namespace tfos::csdk {

class Properties {
 public:
  // for c++3 and above const int can be declared in header files
  static const size_t DEFAULT_BUFFER_SIZE = 4096;
  static const int NUM_BUFFERS_PER_POOL = 1024;
  static const int NUM_TICKETS = NUM_BUFFERS_PER_POOL - 1;
  static const int TICKET_THRESHOLD = 128;

  static const int MAX_PIPES_PER_LC = 2;
  static const int MAX_PARALLEL_LC = 1024;
  static const int MAX_PARALLEL_PIPES = MAX_PIPES_PER_LC * MAX_PARALLEL_LC;

  enum WaitStrategy { BUSY_SPIN, YIELDING, SLEEPING, BLOCKING };
  static const WaitStrategy DEFAULT_DISRUPTOR_WAIT_STRATEGY = BUSY_SPIN;

  // default max pool size for physical or logical pools
  static const size_t MAX_POOL_SIZE = 16;

  // various queue sizes..must be a power of 2
  static const size_t REQUEST_QUEUE_SIZE = 1024;
  static const size_t RESPONSE_QUEUE_SIZE = 1024;
  static const size_t CONTROL_QUEUE_SIZE = 64;

  // queue model
  enum TfosQueueModel {
    SIMPLE,                    // a single queue for all sources and sinks
    SINK_SIDE_MULTIPLEXING,    // a single queue per sink
    SOURCE_SIDE_MULTIPLEXING,  // a single queue per source
    CHAIN                      // a chain of event processors, most complex..
                               // CHAIN uses single writer principle to reduce queue contention
  };

  static const TfosQueueModel DEFAULT_QUEUE_MODEL = SIMPLE;
};

}  // namespace tfos::csdk

#endif  // TFOSCSDK_COMMON_PROPERTIES_H_
