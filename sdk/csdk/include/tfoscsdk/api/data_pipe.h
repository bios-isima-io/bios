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

/*
 * Data pipe and associated structures that needs to be implemented by language
 * layers (e.g JNI) to communicate with the SDK.
 */
#ifndef TFOSSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_H_
#define TFOSSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_H_

#include <any>
#include <string>

#include "tfoscsdk/message_struct.h"
#include "tfoscsdk/properties.h"

namespace tfos::csdk {

typedef int32_t env_handle_t;
typedef int32_t pipe_handle_t;
typedef uint64_t connection_handle_t;

/**
 * A pure virtual class that needs to be implemented by language specific layers
 * to connect the data pipe to their respective languages.
 *
 * <p>
 * The actual pipe implementation is opaque to CSDK layer.
 *
 * <p>
 * It can be assumed that the CSDK layer will call the methods of the pipe only
 * after calling attach() at least once from every native thread.
 *
 * <p>
 * For implementations of data pipe in language specific layers, it may be
 * possible that thread local spaces may be used, in which case the {@code
 * env_handle_t} becomes redundant. However, it is left to the implementations
 * whether the passed in handle is used or not.
 *
 */
class DataPipe {
 public:
  virtual ~DataPipe() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(DataPipe);

  virtual pipe_handle_t getPipeHandle() const = 0;
  // called to initialize the pipe for new native thread
  virtual env_handle_t attach() = 0;
  // tell the pipe that it ready to receive requests through tfos-logical
  // connections
  virtual void request(env_handle_t env_handle, int n) const = 0;
  // push the response when it is available
  virtual bool pushResponse(env_handle_t env_handle, const response_message_t &response) const = 0;
  // close the data pipe
  virtual void close(env_handle_t env_handle, bool success) = 0;
  // called to detach the pipe for native thread
  virtual void detach(env_handle_t env_handle) = 0;

 protected:
  DataPipe() = default;
};

}  // namespace tfos::csdk

#endif /* TFOSSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_H_ */
