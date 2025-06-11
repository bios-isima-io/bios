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
package io.isima.bios.req;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface Message {
  enum ModelType {
    JSON,
    PROTOBUF
  }

  /**
   * Underlying model for the request or response.
   *
   * @return Model type, default is JSON unless explicitly overridden
   */
  @JsonIgnore
  default ModelType getModelType() {
    return ModelType.JSON;
  }
}
