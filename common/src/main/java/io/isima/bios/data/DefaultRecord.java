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

import com.google.protobuf.ByteString;
import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.exceptions.InvalidDataRetrievalException;
import io.isima.bios.models.Attribute;
import io.isima.bios.models.AttributeValue;
import io.isima.bios.models.Record;
import io.isima.bios.models.proto.DataProto;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Default Record implementation backed by BIOS Record protobuf message or builder.
 *
 * <p>The class consists of a Record protobuf message that is meant to store values in memory with
 * minimum footprint. A shared immutable objects, ColumnDefinition, is also attached to help
 * accessing attribute values.
 *
 * <p>An instance can be created either in read-only or writable mode, determined by the variant of
 * the constructors. In read-only mode, the set methods are not usable. An {@link
 * IllegalStateException} is thrown in case a set method is called in read-only mode.
 *
 * <p>The record schema must be predetermined by a definitions map in order to instantiate this
 * class. If the schema is unknown. Use implementation class {@link TBD} instead.
 *
 * <p>The class is NOT thread safe. The user of the class is responsible to guarantee non-concurrent
 * method calls.
 */
public class DefaultRecord implements Record {

  protected final Map<String, ColumnDefinition> definitions;
  protected DataProto.RecordOrBuilder record;
  protected DataProto.Record.Builder builder;
  protected UUID uuid;
  protected Long timestamp;
  protected boolean[] isSet;

  public DefaultRecord(Map<String, ColumnDefinition> definitions) {
    Objects.requireNonNull(definitions);

    this.definitions = definitions;
    builder = DataProto.Record.newBuilder();
    record = builder;
    isSet = new boolean[definitions.size()];
  }

  @Override
  public UUID getEventId() {
    return uuid;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public Collection<? extends Attribute> attributes() {
    return definitions.entrySet().stream()
        .filter((entry) -> isSet[entry.getValue().getPosition()])
        .map((entry) -> new MyAttribute(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public DefaultAttributeValue getAttribute(String name) {
    Objects.requireNonNull(name);
    final var definition = definitions.get(name);
    if (definition != null && (isSet == null || isSet[definition.getPosition()])) {
      return new DefaultAttributeValue(definition);
    }
    return null;
  }

  public Map<String, ColumnDefinition> getDefinitions() {
    return definitions;
  }

  public DataProto.Record asProtoRecord() {
    return builder != null ? builder.build() : (DataProto.Record) record;
  }

  public void setEventId(UUID id) {
    builder.setEventId(UuidMessageConverter.toProtoUuid(id));
    uuid = id;
  }

  public void setTimestamp(Long timestamp) {
    builder.setTimestamp(timestamp);
    this.timestamp = timestamp;
  }

  public void set(String name, String value) {
    final var definition = validateDefinition(name, DataProto.AttributeType.STRING, value);
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  protected void set(ColumnDefinition definition, String value) {
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  public void set(String name, long value) {
    final var definition = validateDefinition(name, DataProto.AttributeType.INTEGER, value);
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  public void set(String name, double value) {
    final var definition = validateDefinition(name, DataProto.AttributeType.DECIMAL, value);
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  public void set(String name, boolean value) {
    final var definition = validateDefinition(name, DataProto.AttributeType.BOOLEAN, value);
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  public void set(String name, ByteBuffer value) {
    final var definition = validateDefinition(name, DataProto.AttributeType.BLOB, value);
    final var index = definition.getIndexInValueArray();
    setInternal(value, index, definition.getPosition());
  }

  public void set(String name, AttributeValue value) {
    final var definition = validateDefinition(name, null, null);
    final var index = definition.getIndexInValueArray();
    final var position = definition.getPosition();
    switch (definition.getType()) {
      case STRING:
        setInternal(value.asString(), index, position);
        break;
      case INTEGER:
        setInternal(value.asLong(), index, position);
        break;
      case DECIMAL:
        setInternal(value.asDouble(), index, position);
        break;
      case BOOLEAN:
        setInternal(value.asBoolean(), index, position);
        break;
      case BLOB:
        setInternal(ByteBuffer.wrap(value.asByteArray()), index, position);
        break;
      default:
        throw new UnsupportedOperationException("setting data of type " + definition.getType());
    }
  }

  public void set(String name, Object value) {
    final var definition = validateDefinition(name, null, value);
    final var index = definition.getIndexInValueArray();
    final var position = definition.getPosition();
    switch (definition.getType()) {
      case STRING:
        setInternal((String) value, index, position);
        break;
      case INTEGER:
        setInternal((Long) value, index, position);
        break;
      case DECIMAL:
        setInternal((Double) value, index, position);
        break;
      case BOOLEAN:
        setInternal((Boolean) value, index, position);
        break;
      case BLOB:
        setInternal((ByteBuffer) value, index, position);
        break;
      default:
        throw new UnsupportedOperationException("setting data of type " + definition.getType());
    }
    isSet[definition.getPosition()] = true;
  }

  private void setInternal(long value, int index, int position) {
    if (builder.getLongValuesCount() > index) {
      builder.setLongValues(index, value);
    } else {
      while (builder.getLongValuesCount() < index) {
        builder.addLongValues(0);
      }
      builder.addLongValues(value);
      markSet(position);
    }
  }

  private void setInternal(String value, int index, int position) {
    if (builder.getStringValuesCount() > index) {
      builder.setStringValues(index, value);
    } else {
      while (builder.getStringValuesCount() < index) {
        builder.addStringValues("");
      }
      builder.addStringValues(value);
      markSet(position);
    }
  }

  private void setInternal(double value, int index, int position) {
    if (builder.getDoubleValuesCount() > index) {
      builder.setDoubleValues(index, value);
    } else {
      while (builder.getDoubleValuesCount() < index) {
        builder.addDoubleValues(0);
      }
      builder.addDoubleValues(value);
      markSet(position);
    }
  }

  private void setInternal(boolean value, int index, int position) {
    if (builder.getBooleanValuesCount() > index) {
      builder.setBooleanValues(index, value);
    } else {
      while (builder.getBooleanValuesCount() < index) {
        builder.addBooleanValues(false);
      }
      builder.addBooleanValues(value);
      markSet(position);
    }
  }

  private void setInternal(ByteBuffer value, int index, int position) {
    if (builder.getBlobValuesCount() > index) {
      builder.setBlobValues(index, ByteString.copyFrom(value));
    } else {
      while (builder.getBlobValuesCount() < index) {
        builder.addBlobValues(ByteString.EMPTY);
      }
      builder.addBlobValues(ByteString.copyFrom(value));
      markSet(position);
    }
  }

  private void markSet(int position) {
    if (position >= isSet.length) {
      isSet = Arrays.copyOf(isSet, Math.max(isSet.length * 2, 8));
    }
    isSet[position] = true;
  }

  protected ColumnDefinition validateDefinition(
      String name, DataProto.AttributeType expectedType, Object value) {
    Objects.requireNonNull(name);
    if (builder == null) {
      throw new IllegalStateException("The object is built in read only mode");
    }
    final var definition = definitions.get(name);
    if (definition == null) {
      throw new IllegalArgumentException("Invalid attribute name: " + name);
    }
    if (expectedType != null && definition.getType() != expectedType) {
      throw new IllegalArgumentException("Invalid attribute type: " + name);
    }
    return definition;
  }

  public class DefaultAttributeValue implements AttributeValue, Comparable<DefaultAttributeValue> {

    @Getter private final ColumnDefinition definition;

    public DefaultAttributeValue(ColumnDefinition definition) {
      this.definition = definition;
    }

    @Override
    public boolean asBoolean() throws InvalidDataRetrievalException {
      if (definition.getType() != DataProto.AttributeType.BOOLEAN) {
        throw new InvalidDataRetrievalException();
      }
      return record.getBooleanValues(definition.getIndexInValueArray());
    }

    @Override
    public long asLong() throws InvalidDataRetrievalException {
      if (definition.getType() != DataProto.AttributeType.INTEGER) {
        throw new InvalidDataRetrievalException();
      }
      return record.getLongValues(definition.getIndexInValueArray());
    }

    @Override
    public double asDouble() throws InvalidDataRetrievalException {
      if (definition.getType() != DataProto.AttributeType.DECIMAL) {
        throw new InvalidDataRetrievalException();
      }
      return record.getDoubleValues(definition.getIndexInValueArray());
    }

    @Override
    public byte[] asByteArray() throws InvalidDataRetrievalException {
      if (definition.getType() != DataProto.AttributeType.BLOB) {
        throw new InvalidDataRetrievalException();
      }
      return record.getBlobValues(definition.getIndexInValueArray()).toByteArray();
    }

    @Override
    public String asString() throws InvalidDataRetrievalException {
      if (definition.getType() != DataProto.AttributeType.STRING) {
        throw new InvalidDataRetrievalException();
      }
      return record.getStringValues(definition.getIndexInValueArray());
    }

    @Override
    public Object asObject() {
      switch (definition.getType()) {
        case STRING:
          return asString();
        case INTEGER:
          return asLong();
        case DECIMAL:
          return asDouble();
        case BOOLEAN:
          return asBoolean();
        case BLOB:
          return ByteBuffer.wrap(asByteArray());
        default:
          throw new UnsupportedOperationException("setting data of type " + definition.getType());
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DefaultAttributeValue)) {
        // TODO(Naoki): Add support for comparing with AttributeValue
        return false;
      }
      final var that = (DefaultAttributeValue) obj;
      if (definition.getType() != that.definition.getType()) {
        return false;
      }
      switch (definition.getType()) {
        case STRING:
          return asString().equals(that.asString());
        case INTEGER:
          return asLong() == that.asLong();
        case DECIMAL:
          return asDouble() == that.asDouble();
        case BOOLEAN:
          return asBoolean() == that.asBoolean();
        case BLOB:
          // NOTE: slow
          return Arrays.equals(asByteArray(), that.asByteArray());
        default:
          throw new UnsupportedOperationException("Unknown attribute type " + definition.getType());
      }
    }

    @Override
    public int hashCode() {
      switch (definition.getType()) {
        case STRING:
          return asString().hashCode();
        case INTEGER:
          return Long.valueOf(asLong()).hashCode();
        case DECIMAL:
          return Double.valueOf(asDouble()).hashCode();
        case BOOLEAN:
          return Boolean.valueOf(asBoolean()).hashCode();
        case BLOB:
          return Arrays.hashCode(asByteArray());
        default:
          throw new UnsupportedOperationException("Unknown attribute type " + definition.getType());
      }
    }

    @Override
    public String toString() {
      switch (definition.getType()) {
        case STRING:
          return asString();
        case INTEGER:
          return Long.toString(asLong());
        case DECIMAL:
          return Double.toString(asDouble());
        case BOOLEAN:
          return Boolean.toString(asBoolean());
        case BLOB:
          return Arrays.toString(asByteArray());
        default:
          throw new UnsupportedOperationException("Unknown attribute type " + definition.getType());
      }
    }

    @Override
    public int compareTo(DefaultAttributeValue that) {
      switch (definition.getType()) {
        case STRING:
          return asString().compareTo(that.asString());
        case INTEGER:
          return Long.compare(asLong(), that.asLong());
        case DECIMAL:
          return Double.compare(asDouble(), that.asDouble());
        case BOOLEAN:
          return Boolean.compare(asBoolean(), that.asBoolean());
        case BLOB:
          return Arrays.compare(asByteArray(), that.asByteArray());
        default:
          throw new UnsupportedOperationException("Unknown attribute type " + definition.getType());
      }
    }
  }

  private class MyAttribute extends DefaultAttributeValue implements Attribute {
    private final String myName;

    public MyAttribute(String name, ColumnDefinition definition) {
      super(definition);
      this.myName = name;
    }

    @Override
    public String name() {
      return myName;
    }
  }
}
