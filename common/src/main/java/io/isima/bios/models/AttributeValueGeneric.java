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

import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.exceptions.InvalidDataRetrievalException;
import java.util.Base64;
import java.util.Objects;
import lombok.ToString;

@ToString
public class AttributeValueGeneric implements AttributeValue {
  private final AttributeType type;
  private final Object value;

  // TODO(Naoki): Provide some creator
  public AttributeValueGeneric(String src, AttributeType type) {
    if (src == null || type == null) {
      throw new NullPointerException();
    }
    this.type = type;
    value = type.fromString(src);
  }

  public AttributeValueGeneric(Object value, AttributeType type) {
    Objects.requireNonNull(type);
    this.type = type;
    this.value = value;
  }

  /**
   * Returns the attribute type.
   *
   * @return Attribute type.
   */
  public AttributeType getType() {
    return type;
  }

  /**
   * Returns the attribute value as a String.
   *
   * @return Attribute value as a String.
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as
   *     String.
   */
  @Override
  public String asString() {
    return value.toString();
  }

  /**
   * Returns the attribute value as a Long.
   *
   * @return Attribute value as a Long.
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as a
   *     Long.
   */
  @Override
  public long asLong() {
    switch (type) {
      case INTEGER:
        return (Long) value;
      case DECIMAL:
        return ((Double) value).longValue();
      case STRING:
        try {
          return Long.valueOf((String) value);
        } catch (NumberFormatException e) {
          throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
        }
      default:
        throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
    }
  }

  /**
   * Returns the attribute value as a Double.
   *
   * @return Attribute value as a Double.
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as a
   *     Double.
   */
  @Override
  public double asDouble() {
    switch (type) {
      case INTEGER:
        return ((Long) value).doubleValue();
      case DECIMAL:
        return (Double) value;
      case STRING:
        try {
          return Double.valueOf((String) value);
        } catch (NumberFormatException e) {
          throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
        }
      default:
        throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
    }
  }

  /**
   * Returns the attribute value as a Boolean.
   *
   * @return Attribute value as a Boolean.
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as a
   *     Boolean.
   */
  @Override
  public boolean asBoolean() {
    switch (type) {
      case STRING:
        try {
          return Boolean.valueOf((String) value);
        } catch (NumberFormatException e) {
          throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
        }
      case BOOLEAN:
        return (Boolean) value;
      default:
        throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
    }
  }

  /**
   * Returns the attribute value as a byte[].
   *
   * @return Attribute value as a byte[].
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as a
   *     byte[].
   */
  @Override
  public byte[] asByteArray() {
    if (type == AttributeType.BLOB) {
      return (byte[]) value;
    } else {
      throw new InvalidDataRetrievalException("Invalid conversion from data type " + type);
    }
  }

  /**
   * Returns the attribute value as an Object.
   *
   * @return Attribute value as an Object.
   * @throws InvalidDataRetrievalException thrown when the attribute value is not available as an
   *     Object.
   */
  @Override
  public Object asObject() {
    return value;
  }

  @JsonValue
  Object getJsonValue() {
    if (type == AttributeType.BLOB) {
      return Base64.getEncoder().encodeToString((byte[]) value);
    } else {
      return value;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AttributeValueGeneric)) {
      return false;
    }
    final var that = (AttributeValueGeneric) other;
    return Objects.equals(type, that.type) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }
}
