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
 * Abstract factory interface for creating language specific data pipes
 */
#ifndef TFOSCSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_FACTORY_H_
#define TFOSCSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_FACTORY_H_

#include "tfoscsdk/api/data_pipe.h"
#include "tfoscsdk/api/logical_pipe.h"
#include "tfoscsdk/properties.h"

namespace tfos::csdk {

/**
 * Abstract Factory that creates (and destroys) the language specific data pipe.
 *
 * <p>
 * Language specific implementation MUST provide their own implementation of the
 * factory when checking out a logical connection.
 */
class DataPipeFactory {
 public:
  virtual ~DataPipeFactory() = default;
  DataPipeFactory() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(DataPipeFactory);

  virtual DataPipe *create(connection_handle_t connection_id, pipe_handle_t pipe_handle,
                           std::any connection_data_ref, LogicalPipe *p_logical_pipe) const = 0;
};

}  // namespace tfos::csdk

#endif  // TFOSCSDK_SRC_INCLUDES_TFOSSDK_DATA_PIPE_FACTORY_H_
