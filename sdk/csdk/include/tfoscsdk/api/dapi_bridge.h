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
 * Bridge to direct api
 */
#ifndef TFOSCSDK_TFOSCSDK_INCLUDES_TFOSCSDK_API_DAPI_BRIDGE_H_
#define TFOSCSDK_TFOSCSDK_INCLUDES_TFOSCSDK_API_DAPI_BRIDGE_H_

#include <tfoscsdk/properties.h>
#include <tfoscsdk/types.h>

namespace tfos::csdk {

class DapiBridge {
 public:
  static DapiBridge *getInstance();

  virtual ~DapiBridge() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(DapiBridge);

  /**
   * Initialize the bridge between reactive and direct
   */
  virtual void initialize(capi_t capi) = 0;
  virtual void terminate() = 0;
  virtual capi_t getDirectApiId() = 0;

 protected:
  DapiBridge() = default;
};

}  // namespace tfos::csdk

#endif  // TFOSCSDK_TFOSCSDK_INCLUDES_TFOSCSDK_API_DAPI_BRIDGE_H_
