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
 * Interface to a logical pipe that can be used as a bi-directional pipe to send
 * and receive messages.
 */
#ifndef TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_PIPE_H_
#define TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_PIPE_H_

#include "tfoscsdk/api/data_pipe.h"
#include "tfoscsdk/properties.h"

namespace tfos::csdk {

class LogicalPipe {
 public:
  DISABLE_COPY_MOVE_AND_ASSIGN(LogicalPipe);

  virtual ~LogicalPipe() noexcept = default;

  /**
   * Get a reference to the data pipe.
   *
   * It is assumed that the call is made every time inside the scope of a begin
   * and end of logical connection reference, so that the returned reference
   * will never be dangling.
   *
   * @return implicit reference to a data pipe object
   */
  virtual DataPipe &getDataPipe() const = 0;

  /**
   * Signal when the underlying data pipe is ready for message flow.
   */
  virtual void signalPipeReady() const = 0;

  /**
   * Asynchronously close the pipe.
   *
   * @param force_flag if true, do not wait to drain the pipe
   */
  virtual void asyncClosePipe(bool force_flag) = 0;

  /**
   * Push a request message through the logical pipe
   *
   * @param request_message
   */
  virtual void pushRequest(request_message_t &request_message) = 0;

  /**
   * Allocate a buffer to encode a request message.
   *
   * Allocation failure is rare as message will be allocated from pre-allocated
   * message and the flow api triggers new requests only when there is capacity.
   * The only case, where new buffers may need to be allocated is when a single
   * message is larger than the available capacity.
   *
   * @param size  size of the buffer
   *
   * @return buffer pointer. nullptr if we runout of space.
   */
  virtual uint8_t *allocateBuffer(int64_t size = Properties::DEFAULT_BUFFER_SIZE) = 0;

 protected:
  LogicalPipe() = default;
};

}  // namespace tfos::csdk

#endif  // TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_PIPE_H_
