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

/** Specifies preference to fetch counter values. */
public enum Preference {
  /**
   * The fetch method prioritizes performance in execution when specified.
   *
   * <p>Counter value recency may be sacrificed.
   */
  PERFORMANCE,
  /**
   * The fetch method prioritizes recency in execution when specified.
   *
   * <p>The elapsed time would be longer with this preference.
   */
  RECENCY;

  private static final EnumStringifier<Preference> stringifier = new EnumStringifier<>(values());

  @JsonCreator
  static Preference forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
