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
 * Echo Control
 */
#ifndef TFOSCSDK_API_ECHO_ECHO_CONTROL_H_
#define TFOSCSDK_API_ECHO_ECHO_CONTROL_H_

#include "tfoscsdk/properties.h"

namespace tfos::csdk {

class EchoControl {
 public:
  virtual ~EchoControl() = default;
  DISABLE_COPY_MOVE_AND_ASSIGN(EchoControl);
  static EchoControl *getInstance();

  virtual void Start() = 0;
  virtual void Stop() = 0;
  virtual int StartEchoPipe() = 0;
  virtual void KillEchoPipe(int pipe_id) = 0;

 protected:
  EchoControl() = default;
};

}  // namespace tfos::csdk

#endif  // TFOSCSDK_API_ECHO_ECHO_CONTROL_H_
