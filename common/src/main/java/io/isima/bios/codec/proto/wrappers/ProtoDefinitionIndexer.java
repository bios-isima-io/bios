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
package io.isima.bios.codec.proto.wrappers;

import io.isima.bios.codec.proto.AttributeDefinition;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.proto.DataProto;
import java.util.Objects;

/**
 * Abstracts out the protobuf specific implementation of the attribute definition per definition
 * that comes in query result messages before every record dump.
 */
public class ProtoDefinitionIndexer implements AttributeDefinition {
  private final DataProto.ColumnDefinition.Builder definitionBuilder;

  public ProtoDefinitionIndexer(
      String attributeName, DataProto.AttributeType type, int indexInValueArray) {
    this.definitionBuilder = DataProto.ColumnDefinition.newBuilder();
    definitionBuilder.setName(attributeName);
    definitionBuilder.setType(type);
    definitionBuilder.setIndexInValueArray(indexInValueArray);
  }

  @Override
  public String getName() {
    return definitionBuilder.getName();
  }

  @Override
  public AttributeType attributeType() {
    return AttributeType.fromProto(definitionBuilder.getType());
  }

  @Override
  public int getIndexInValuesArray() {
    return definitionBuilder.getIndexInValueArray();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AttributeDefinition)) {
      return false;
    }
    AttributeDefinition that = (AttributeDefinition) o;
    return getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName());
  }

  public DataProto.ColumnDefinition toDefinitionProto() {
    return definitionBuilder.build();
  }

  public DataProto.AttributeType getProtoAttributeType() {
    return definitionBuilder.getType();
  }

  public void updateTypeAndIndex(DataProto.AttributeType type, int index) {
    definitionBuilder.setType(type);
    definitionBuilder.setIndexInValueArray(index);
  }
}
