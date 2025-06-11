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

public enum SubjectType {
  TABLE,
  TOPIC,
  SUB_PATH;

  private static final EnumStringifier<SubjectType> stringifier = new EnumStringifier<>(values());

  /**
   * Package-scope method that is used for deserializing the enum entry.
   *
   * @param value JSON data element
   * @return The decoded entry
   */
  @JsonCreator
  public static SubjectType destringify(String value) {
    return stringifier.destringify(value);
  }

  /**
   * Package-scope method that is used for serializing the enum entry.
   *
   * @return The encoded string.
   */
  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
