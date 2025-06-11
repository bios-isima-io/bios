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
import io.isima.bios.models.proto.DataProto;
import lombok.Getter;

/**
 * BIOS DataProto.ColumnDefinition plus total data position.
 *
 * <p>The class is used as {@link DefaultRecord} attribute access aid.
 */
public class ColumnDefinition {
  private final DataProto.ColumnDefinition definitionProto;
  @Getter private final int position;
  @Getter private final AttributeConfig attribute;

  public ColumnDefinition(
      DataProto.ColumnDefinition definition, int position, AttributeConfig attribute) {
    this.definitionProto = definition;
    this.position = position;
    this.attribute = attribute;
  }

  public int getIndexInValueArray() {
    return definitionProto.getIndexInValueArray();
  }

  public String getName() {
    return definitionProto.getName();
  }

  public DataProto.AttributeType getType() {
    return definitionProto.getType();
  }

  public DataProto.ColumnDefinition asProtoMessage() {
    return definitionProto;
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder("{");
    sb.append("name=")
        .append(definitionProto.getName())
        .append(", type=")
        .append(definitionProto.getType())
        .append(", index=")
        .append(definitionProto.getIndexInValueArray())
        .append(", pos=")
        .append(position);
    return sb.append("}").toString();
  }
}
