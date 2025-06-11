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

import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.exceptions.InvalidDataRetrievalException;
import io.isima.bios.models.Attribute;
import io.isima.bios.models.AttributeValue;
import io.isima.bios.models.Record;
import io.isima.bios.models.proto.DataProto;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Protobuf implementation of Record used on client side.
 *
 * <p>This is an internal implementation and must not be directly exposed to external users (client
 * or server), except through interfaces.
 */
public class RecordProtobufReader implements Record {
  private final DataProto.Record record;
  private final Map<String, ProtoAttribute> attributes;

  private UUID eventId;
  private Long timestamp;

  /** Constructs from an existing protobuf record abstraction. */
  public RecordProtobufReader(
      DataProto.Record record, List<DataProto.ColumnDefinition> columnDefinitions) {
    this.record = record;
    attributes = new LinkedHashMap<>();
    columnDefinitions.forEach(
        (definition) -> {
          ProtoAttribute attribute =
              new ProtoAttribute(
                  definition.getName(), definition.getIndexInValueArray(), definition.getType());
          attributes.put(definition.getName(), attribute);
        });
    eventId = UuidMessageConverter.fromProtoUuid(record.getEventId());
    timestamp = record.getTimestamp() > 0 ? record.getTimestamp() : null;
  }

  @Override
  public UUID getEventId() {
    return eventId;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public Collection<? extends Attribute> attributes() {
    return attributes.values();
  }

  @Override
  public AttributeValue getAttribute(String name) {
    return attributes.get(name);
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder("{");
    String delimiter = "";
    if (getEventId() != null) {
      sb.append("eventId=").append(getEventId());
      delimiter = ", ";
    }
    if (getTimestamp() != null) {
      sb.append(delimiter).append("timestamp=").append(getTimestamp());
      delimiter = ", ";
    }
    sb.append(delimiter).append("attrs={");
    sb.append(
        attributes.entrySet().stream()
            .map((entry) -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(", ")));
    return sb.append("}}").toString();
  }

  /**
   * Returns attribute information including name and value.
   *
   * <p>Interfaces directly with protobuf primitives and primitive arrays
   */
  private final class ProtoAttribute implements Attribute {
    private final String name;
    private final int index;
    private final DataProto.AttributeType actualType;

    ProtoAttribute(String name, int index, DataProto.AttributeType type) {
      this.name = name;
      this.index = index;
      this.actualType = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public long asLong() {
      switch (actualType) {
        case INTEGER:
          return record.getLongValues(index);
        case DECIMAL:
          return (long) record.getDoubleValues(index);
        case STRING:
          try {
            return Long.parseLong(record.getStringValues(index));
          } catch (NumberFormatException e) {
            break;
          }
        default:
          break;
      }
      throw new InvalidDataRetrievalException(invalidTypeConversion());
    }

    @Override
    public double asDouble() {
      switch (actualType) {
        case INTEGER:
          return record.getLongValues(index);
        case DECIMAL:
          return record.getDoubleValues(index);
        case STRING:
          try {
            return Double.parseDouble(record.getStringValues(index));
          } catch (NumberFormatException e) {
            break;
          }
        default:
          break;
      }
      throw new InvalidDataRetrievalException(invalidTypeConversion());
    }

    @Override
    public boolean asBoolean() {
      switch (actualType) {
        case BOOLEAN:
          return record.getBooleanValues(index);
        case STRING:
          try {
            return Boolean.parseBoolean(record.getStringValues(index));
          } catch (NumberFormatException e) {
            break;
          }
        default:
          break;
      }
      throw new InvalidDataRetrievalException(invalidTypeConversion());
    }

    @Override
    public byte[] asByteArray() {
      if (actualType == DataProto.AttributeType.BLOB) {
        return record.getBlobValues(index).toByteArray();
      }
      throw new InvalidDataRetrievalException(invalidTypeConversion());
    }

    @Override
    public String asString() throws InvalidDataRetrievalException {
      switch (actualType) {
        case STRING:
          return record.getStringValues(index);
        case INTEGER:
          return String.valueOf(record.getLongValues(index));
        case BOOLEAN:
          return String.valueOf(record.getBooleanValues(index));
        case DECIMAL:
          return String.valueOf(record.getDoubleValues(index));
        case BLOB:
          return record.getBlobValues(index).toString();
        default:
          throw new InvalidDataRetrievalException(invalidTypeConversion());
      }
    }

    @Override
    public Object asObject() {
      switch (actualType) {
        case STRING:
          return record.getStringValues(index);
        case INTEGER:
          return record.getLongValues(index);
        case BOOLEAN:
          return record.getBooleanValues(index);
        case DECIMAL:
          return record.getDoubleValues(index);
        case BLOB:
          return record.getBlobValues(index);
        default:
          throw new InvalidDataRetrievalException(invalidTypeConversion());
      }
    }

    private String invalidTypeConversion() {
      return String.format("Invalid conversion from actual data type %s", actualType);
    }

    @Override
    public String toString() {
      return asString();
    }
  }
}
