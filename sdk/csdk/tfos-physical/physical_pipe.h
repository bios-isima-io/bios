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
 * Physical pipe implementation
 */
#ifndef TFOSCSDK_SRC_PHYSICAL_PIPE_H_
#define TFOSCSDK_SRC_PHYSICAL_PIPE_H_

#include "tfoscsdk/message_struct.h"

namespace tfos::csdk {

class PhysicalPipe {
 public:
  virtual std::string name() = 0;

  virtual void Initialize(response_cb cb, void *cb_args) = 0;

  virtual void HandleRequest(const RequestMessage &request_message) = 0;

  virtual bool IsActive() = 0;

  virtual void Shutdown() = 0;

  virtual std::string GetLastConnectionError() { return ""; }

 protected:
  // You cannot delete a pipe from outside. Use Shutdown method to terminate.
  virtual ~PhysicalPipe() = default;
};
}  // namespace tfos::csdk

#endif  // TFOSCSDK_SRC_PHYSICAL_PIPE_H_
