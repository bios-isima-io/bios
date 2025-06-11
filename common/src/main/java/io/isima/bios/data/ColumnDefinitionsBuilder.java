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
package io.isima.bios.data;

import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.proto.DataProto;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds column definitions for a set of attributes. */
public class ColumnDefinitionsBuilder {

  private final Map<String, ColumnDefinition> definitions;

  int integerIndex;
  int stringIndex;
  int decimalIndex;
  int booleanIndex;
  int blobIndex;

  int totalIndex;

  /** The constructor. */
  public ColumnDefinitionsBuilder() {
    definitions = new LinkedHashMap<>();
  }

  /**
   * Adds multiple attributes to the builder.
   *
   * @param attributes Attributes to add
   * @return self
   */
  public ColumnDefinitionsBuilder addAttributes(Collection<? extends AttributeConfig> attributes) {
    attributes.forEach(this::addAttribute);
    return this;
  }

  /**
   * Add an attribute to the builder.
   *
   * @param attribute The attribute to ad
   * @return Self
   */
  public ColumnDefinitionsBuilder addAttribute(AttributeConfig attribute) {
    final var name = attribute.getName();
    final var type = attribute.getType();
    final var builder = DataProto.ColumnDefinition.newBuilder();
    builder.setName(name);
    switch (type) {
      case INTEGER:
        builder.setType(DataProto.AttributeType.INTEGER);
        builder.setIndexInValueArray(integerIndex++);
        break;
      case STRING:
        builder.setType(DataProto.AttributeType.STRING);
        builder.setIndexInValueArray(stringIndex++);
        break;
      case DECIMAL:
        builder.setType(DataProto.AttributeType.DECIMAL);
        builder.setIndexInValueArray(decimalIndex++);
        break;
      case BOOLEAN:
        builder.setType(DataProto.AttributeType.BOOLEAN);
        builder.setIndexInValueArray(booleanIndex++);
        break;
      case BLOB:
        builder.setType(DataProto.AttributeType.BLOB);
        builder.setIndexInValueArray(blobIndex++);
        break;
      default:
    }
    definitions.put(name, new ColumnDefinition(builder.build(), totalIndex++, attribute));
    return this;
  }

  /**
   * Ad an attribute to the buidler.
   *
   * @param name Attribute name
   * @param type Attribute data type
   * @return Self
   */
  public ColumnDefinitionsBuilder addAttribute(String name, AttributeType type) {
    final var attribute = new AttributeConfig(name, type);
    return addAttribute(attribute);
  }

  /** Returns mutable column definitions. */
  public Map<String, ColumnDefinition> getDefinitions() {
    return definitions;
  }

  /** Returns an immutable column definitions of current status. */
  public Map<String, ColumnDefinition> build() {
    return Collections.unmodifiableMap(definitions);
  }
}
