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
package io.isima.bios.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;

public enum ImportPayloadType {
  JSON,
  CSV,
  UNKNOWN;

  private static final EnumStringifier<ImportPayloadType> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  static ImportPayloadType forValue(String value) {
    try {
      return stringifier.destringify(value);
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
