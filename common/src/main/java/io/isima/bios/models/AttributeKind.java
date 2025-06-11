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
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** The real-world kind of values, currently only applicable to quantity category. */
@Getter
@RequiredArgsConstructor
public enum AttributeKind {
  OTHER_KIND,

  MONEY,
  TIMESTAMP,
  DURATION,
  FREQUENCY,
  DIMENSIONLESS,

  LATITUDE,
  LONGITUDE,

  DISTANCE,
  SPEED,
  VOLUME,
  WEIGHT,

  DATA_SIZE,
  DATA_RATE,
  DISPLAY_SIZE;

  public static UnitDisplayPosition getUnitDisplayPosition(AttributeKind kind) {
    if (kind == null) {
      return UnitDisplayPosition.SUFFIX;
    }

    switch (kind) {
      case MONEY:
        return UnitDisplayPosition.PREFIX;
      default:
        return UnitDisplayPosition.SUFFIX;
    }
  }

  public static PositiveIndicator getPositiveIndicator(AttributeKind kind) {
    if (kind == null) {
      return PositiveIndicator.HIGH;
    }

    switch (kind) {
      case DURATION:
      case TIMESTAMP:
        return PositiveIndicator.LOW;
      case LATITUDE:
      case LONGITUDE:
        return PositiveIndicator.NEUTRAL;
      default:
        return PositiveIndicator.HIGH;
    }
  }

  private static final EnumStringifier<AttributeKind> stringifier = new EnumStringifier<>(values());

  @JsonCreator
  public static AttributeKind forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
