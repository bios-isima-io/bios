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

/**
 * Category of attributes and possibly metrics in the future, e.g. (numeric) quantity, dimension.
 */
@Getter
@RequiredArgsConstructor
public enum AttributeCategory {
  QUANTITY,
  DIMENSION,
  DESCRIPTION;

  private static final EnumStringifier<AttributeCategory> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  public static AttributeCategory forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
