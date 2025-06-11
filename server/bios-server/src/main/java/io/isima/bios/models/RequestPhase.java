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

public enum RequestPhase {
  INITIAL,
  FINAL,
  ;

  /**
   * Method to convert a string to RequestPhase entry.
   *
   * <p>This method is meant be used by JSON deserializer. Unlike simple valuOf() enum method, this
   * method converts input string to an entry in case insensitive manner.
   *
   * @param value Input string
   * @return An {@link RequestPhase} entry.
   * @throws IllegalArgumentException when input string does not match any RequestPhase names.
   */
  @JsonCreator
  public static RequestPhase forValue(String value) {
    try {
      return RequestPhase.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(String.format("'%s' is invalid", value.toLowerCase()));
    }
  }

  @JsonValue
  public String toValue() {
    return name().toLowerCase();
  }
}
