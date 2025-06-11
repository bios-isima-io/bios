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
 * LogicalConnection interface exposed outside the CSDK.
 */
#ifndef TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_CONNECTION_H_
#define TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_CONNECTION_H_

#include <any>

#include "tfoscsdk/api/logical_pipe.h"

namespace tfos::csdk {

typedef int32_t pipe_handle_t;

class LogicalConnection {
 public:
  DISABLE_COPY_MOVE_AND_ASSIGN(LogicalConnection);
  ~LogicalConnection() noexcept = default;

  /**
   * A data store for language specific layers to store and retrieve their own
   * data. Type erasure happens with std::any without the calling layers loosing
   * the type information.
   *
   * For performance reasons, it is recommended to store only pointer like types
   * for objects that has a life time more than this logical connection.
   * Otherwise, the object itself will need to be copy constructable.
   *
   * @param data_ref Reference to stored data
   */
  virtual void storeConnectionData(const std::any &data_ref) = 0;

  /**
   * Get the stored data against this connection.
   *
   * @return stored data
   */
  virtual std::any getConnectionData() const = 0;

  /**
   * Open a logical pipe on this logical connection.
   *
   * @return A pipe handle which is a positive integer on success. A negative
   * return value signifies failure
   */
  virtual pipe_handle_t openPipe() = 0;

  /**
   * Get a read write reference to the logical pipe.
   *
   * It is guaranteed that the logical pipe object will have a lifetime until
   * the pipe is closed. So it is important that the callers do not store these
   * pointers permanently, but use it on demand by calling this method through
   * the logical connection as logical connection references are counted
   * atomically
   *
   * @param pipe_handle
   *
   * @return Reference to logical pipe
   */
  virtual LogicalPipe *getPipe(pipe_handle_t pipe_handle) = 0;

 protected:
  LogicalConnection() = default;
};

}  // namespace tfos::csdk

#endif /* TFOSSDK_SRC_INCLUDES_TFOSSDK_LOGICAL_CONNECTION_H_ */
