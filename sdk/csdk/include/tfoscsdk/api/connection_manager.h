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
 * ConnectionManager interface visible outside the CSDK
 */
#ifndef TFOSSDK_SRC_INCLUDES_TFOSSDK_CONNECTION_MANAGER_H_
#define TFOSSDK_SRC_INCLUDES_TFOSSDK_CONNECTION_MANAGER_H_

#include "tfoscsdk/api/data_pipe.h"
#include "tfoscsdk/api/data_pipe_factory.h"
#include "tfoscsdk/api/logical_connection.h"

namespace tfos::csdk {

typedef uint64_t connection_handle_t;

class ConnectionManager {
 public:
  static const int INVALID_CONNECTION_HANDLE = 0;
  static ConnectionManager *getInstance();

  virtual ~ConnectionManager() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(ConnectionManager);

  /**
   * Checkout a logical connection for use.
   *
   * The {@code pipe_factory} must have a lifetime greater than the checked out connection. It is
   * assumed that the object is either in the heap (possibly a singleton) or static.
   *
   * @param pipe_factory A const unowned reference to a pipe factory
   * @param session_id Session Id of the opened session
   *
   * @return a unique (non-repeating) handle that identifies this connection
   */
  virtual connection_handle_t checkoutConnection(const DataPipeFactory &pipe_factory,
                                                 int32_t session_id) = 0;

  /**
   * Release the logical connection.
   *
   * The operation is idempotent and will be harmless when called multiple
   * times.
   *
   * @param connection_id
   *
   * @return 0 if closed (including already closed). negative error code, if connection cannot be
   * released
   */
  virtual int releaseConnection(connection_handle_t connection_id) = 0;

  /**
   * Begin using a logical connection reference.
   *
   * The returned reference is a nullable, non-owned reference. If the returned reference is
   * non-null, the caller MUST call {@code endLogicalConnectionUsage}. If the returned reference is
   * null, end.. call is a noop.
   *
   * It is recommended that the returned reference is not stored and is only used locally within the
   * stack. In the future a wrapper class may be made available for conveniently referencing and
   * de-referencing the logical connection object in the stack as local variables.
   *
   * @param connection_id handle that uniquely identifies the connection
   *
   * @return nullable reference to a logical connection.
   */
  virtual LogicalConnection *beginLogicalConnectionUsage(connection_handle_t connection_id) = 0;

  /**
   * Signifies that calling thread is done with using the logical connection reference for the given
   * connection id.
   *
   * TODO: Create a wrapper class that can be used to explicitly begin and implicitly end logical
   * connection usage.
   *
   * @param connection_id
   */
  virtual void endLogicalConnectionUsage(connection_handle_t connection_id) = 0;

 protected:
  ConnectionManager() = default;
};

}  // namespace tfos::csdk

#endif /* TFOSSDK_SRC_INCLUDES_TFOSSDK_CONNECTION_MANAGER_H_ */
