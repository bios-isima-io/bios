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

public enum TimeunitType {
  MILLISECOND,
  SECOND,
  MINUTE,
  HOUR,
  DAY;

  /**
   * Method to convert a string to TimeunitType entry.
   *
   * <p>This method is meant be used by JSON deserializer. Unlike simple valueOf() enum method, this
   * method converts input string to an entry in case insensitive manner.
   *
   * @param value Input string
   * @return An {@link TimeunitType} entry.
   * @throws IllegalArgumentException when input string does not match any ActionType names.
   */
  @JsonCreator
  public static TimeunitType forValue(String value) {
    try {
      return TimeunitType.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          String.format("type %s is unsupported", value.toLowerCase()));
    }
  }

  @JsonValue
  public String toValue() {
    return name().toLowerCase();
  }
}
