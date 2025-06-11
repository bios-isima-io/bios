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
package io.isima.bios.codec.proto;

import io.isima.bios.models.AttributeType;
import java.util.List;
import java.util.function.Function;

/**
 * Attribute Definition that uniquely identifies an attribute.
 *
 * <p>This is for internal protobuf purpose only and will not be exposed outside.
 */
public interface AttributeDefinition {
  /**
   * Name of the attribute.
   *
   * @return attribute name
   */
  String getName();

  /**
   * Type information of the attribute.
   *
   * <p>Encapsulates a java type under a bios type.
   *
   * @return Bios type
   */
  AttributeType attributeType();

  int getIndexInValuesArray();

  /**
   * Extracts from specified proto list in a type safe manner.
   *
   * @param list proto list of given internal type
   * @param itemIdx index into item
   * @return extracted type
   */
  @SuppressWarnings("unchecked")
  default <T> T extractFromList(List<?> list, int itemIdx) {
    if (itemIdx < list.size()) {
      return (T) list.get(itemIdx);
    }
    return null;
  }

  /**
   * Extracts and converts to this type.
   *
   * @param list list from which to extract
   * @param itemIdx index from which to extract
   * @param converter type converter
   * @param <U> input type
   * @return converted value
   */
  default <U, T> T extractFromList(List<U> list, int itemIdx, Function<U, T> converter) {
    if (itemIdx < list.size()) {
      U item = list.get(itemIdx);
      return converter.apply(item);
    }
    return null;
  }
}
