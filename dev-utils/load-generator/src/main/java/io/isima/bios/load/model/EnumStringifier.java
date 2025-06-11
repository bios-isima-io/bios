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
package io.isima.bios.load.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class used for serializing/deserializing biOS control plain schema enum values.
 *
 * <p>
 * biOS control plain has camel case naming convention (such as storeDefaultValue) although Java
 * enum has naming convention of upper snake case (such as STORE_DEFAULT_VALUE). This class helps
 * converting Java enum names to/from biOS camel case names for JSON serialization/deserialization.
 * </p>
 *
 * @param <E> Enum class to stringify
 */
class EnumStringifier<E extends Enum<E>> {
  private final Map<String, E> jsonToEntry;
  private final List<String> entryToJson;

  /**
   * The constructor.
   *
   * <p>
   * The constructor builds the conversion table from JSON (camel name) to Enum entry and the table
   * from enum to JSON (camel name).
   * </p>
   *
   * @param values Enum values. Values would be available by method <code>E.values()</code>
   */
  EnumStringifier(E[] values) {
    jsonToEntry = new HashMap<>();
    entryToJson = new ArrayList<>();

    for (E entry : values) {
      final String stringified = buildStringifiedName(entry);
      jsonToEntry.put(stringified.toLowerCase(), entry);
      entryToJson.add(stringified);
    }
  }

  /**
   * Method to deserialize an enum value.
   *
   * @param value Serialized enum value
   * @return Resolved enum value
   * @throws NullPointerException when the parameter is null.
   * @throws IllegalArgumentException when the input value is invalid.
   */
  E destringify(String value) {
    if (value == null) {
      throw new NullPointerException("'value' must not be null");
    }
    final E entry = jsonToEntry.get(value.toLowerCase());
    if (entry == null) {
      throw new IllegalArgumentException("Unknown value: " + value);
    }
    return entry;
  }

  /**
   * Method to serialize an enum entry.
   *
   * @param entry Enum entry
   * @return serialized enum value
   */
  String stringify(E entry) {
    return entryToJson.get(entry.ordinal());
  }

  /**
   * Utility method to generate a camel case name from an Enum entry.
   *
   * @param entry Input enum entry
   * @return output camel-case name
   */
  private String buildStringifiedName(E entry) {
    final String name = entry.name();
    StringBuilder sb = new StringBuilder();
    boolean capital = true;
    for (int i = 0; i < name.length(); ++i) {
      final char ch = name.charAt(i);
      if (ch == '_') {
        capital = true;
      } else {
        sb.append(capital ? ch : Character.toLowerCase(ch));
        capital = false;
      }
    }
    return sb.toString();
  }
}
