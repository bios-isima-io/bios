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
package io.isima.bios.dto.recommendation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;

/** Shop enhancement query type. */
public enum QueryType {
  RECENT_VIEWS,
  NUM_VIEWERS,
  TRENDING_PRODUCTS,
  TRENDING_PRODUCTS_OF_TYPE,
  FREQUENTLY_VIEWED_TOGETHER,
  FIND_SIMILAR_PRODUCTS_IN_HISTORY,
  PRODUCT_POPULARITY;

  private static final EnumStringifier<QueryType> stringifier = new EnumStringifier<>(values());

  @JsonCreator
  public static QueryType forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
