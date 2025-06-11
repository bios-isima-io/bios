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
import io.isima.bios.errors.exception.ConstraintViolationException;

public enum MissingAttributePolicyV1 {
  STRICT,
  USE_DEFAULT,
  FAIL_PARENT_LOOKUP;

  /**
   * Method to convert a string to ActionType entry.
   *
   * <p>This method is meant be used by JSON deserializer. Unlike simple valuOf() enum method, this
   * method converts input string to an entry in case insensitive manner.
   *
   * @param value Input string
   * @return An {@link MissingAttributePolicyV1} entry.
   * @throws IllegalArgumentException when input string does not match any ActionType names.
   */
  @JsonCreator
  public static MissingAttributePolicyV1 forValue(String value) {
    try {
      return MissingAttributePolicyV1.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          String.format("Unknown MissingAttributePolicyV1 option: %s", value.toLowerCase()));
    }
  }

  @JsonValue
  public String toValue() {
    return name().toLowerCase();
  }

  public static MissingLookupPolicy translateToMlp(MissingAttributePolicyV1 sourcePolicy)
      throws ConstraintViolationException {
    if (sourcePolicy == null) {
      return null;
    }
    switch (sourcePolicy) {
      case STRICT:
        return MissingLookupPolicy.REJECT;
      case USE_DEFAULT:
        return MissingLookupPolicy.STORE_FILL_IN_VALUE;
      case FAIL_PARENT_LOOKUP:
        return MissingLookupPolicy.FAIL_PARENT_LOOKUP;
      default:
        throw new ConstraintViolationException(
            "Policy " + sourcePolicy.name() + " is not supported");
    }
  }
}
