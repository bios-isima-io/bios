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

import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * AttributeConfig class enhanced for usages in server.
 *
 * <p>The class has several utility methods for faster execution, such as finding index of an enum
 * value and converting a value of surface type to internal. As a drawback, an instance has to run
 * {@link #compile()} to make it functional, so the class is not good at frequent instantiations.
 */
public class ServerAttributeConfig extends AttributeConfig {
  private Map<String, Integer> enums;
  private Converter converter;

  public ServerAttributeConfig() {}

  public ServerAttributeConfig(String name, AttributeType type) {
    super(name, type);
    compile();
  }

  public boolean hasAllowedValues() {
    return allowedValues != null;
  }

  /**
   * Returns the enum index of a value if the attribute has allowed values.
   *
   * @param value Value to resolve the index
   * @return The index of the value. Returns null if the value is not in the allowed values or the
   *     attribute does not have allowed values.
   */
  public Integer getEnumIndex(String value) {
    return enums != null ? enums.get(value) : null;
  }

  public void compile() {
    if (allowedValues != null) {
      enums = new HashMap<>();
      for (int index = 0; index < allowedValues.size(); ++index) {
        final var attribute = allowedValues.get(index);
        // we assume the enum type is always string
        enums.put(attribute.asString(), index);
      }
    }
    // compile the string to internal value converter, used for inserting events

    switch (type) {
      case STRING:
        converter = ServerAttributeConfig::convertString;
        break;
      case INTEGER:
        converter = ServerAttributeConfig::convertToInteger;
        break;
      case DECIMAL:
        converter = ServerAttributeConfig::convertToDecimal;
        break;
      case BOOLEAN:
        converter = ServerAttributeConfig::convertToBoolean;
        break;
      case BLOB:
        converter = ServerAttributeConfig::convertToBlob;
        break;
      default:
        // We can't throw an exception here, TFOS deprecated types would come here,
        // then IT tests fail if we throw.
        converter = (src, attributeConfig) -> src;
    }
  }

  /**
   * Converts a string source into tye attribute's internal data type.
   *
   * <p>The method converts a string enum to an index if the attribute is of a string type and has
   * allowed values. The converter does not fill a default value if the value is missing.
   *
   * @throws InvalidValueException If the value is not convertible to the attribute's type.
   */
  public Object toInternalValue(String value) throws InvalidValueSyntaxException {
    return converter.convert(value, this);
  }

  /** A Converter converts a string data source into the attribute type's internal data type. */
  @FunctionalInterface
  private interface Converter {
    Object convert(String src, ServerAttributeConfig attributeConfig)
        throws InvalidValueSyntaxException;
  }

  // static value converters ////////////////////////////////////////

  static Object convertString(String src, ServerAttributeConfig attributeConfig)
      throws InvalidValueSyntaxException {
    if (attributeConfig.hasAllowedValues()) {
      final var index = attributeConfig.getEnumIndex(src);
      if (index == null) {
        throw new InvalidValueSyntaxException(
            String.format(
                "Specified value is not in the list of allowed values;"
                    + " attribute=%s, type=%s, src=%s",
                attributeConfig.name, attributeConfig.type, src));
      }
      return index;
    } else {
      return src;
    }
  }

  static Object convertToInteger(String src, ServerAttributeConfig attributeConfig)
      throws InvalidValueSyntaxException {
    try {
      return Long.valueOf(src);
    } catch (NumberFormatException e) {
      throw new InvalidValueSyntaxException(
          String.format(
              "Input must be a numeric string; attribute=%s, type=%s, value=%s",
              attributeConfig.name, attributeConfig.type, src));
    }
  }

  static Object convertToDecimal(String src, ServerAttributeConfig attributeConfig)
      throws InvalidValueSyntaxException {
    try {
      final var value = Double.valueOf(src);
      if (value.isNaN() || value.isInfinite()) {
        throw new InvalidValueSyntaxException(
            String.format(
                "%s value is not allowed; attribute=%s, type=%s",
                src, attributeConfig.name, attributeConfig.type));
      }
      return value;
    } catch (NumberFormatException e) {
      throw new InvalidValueSyntaxException(
          String.format(
              "Invalid value syntax; attribute=%s, type=%s, value=%s",
              attributeConfig.name, attributeConfig.type, src));
    }
  }

  static Object convertToBoolean(String src, ServerAttributeConfig attributeConfig) {
    return Boolean.valueOf(src);
  }

  static Object convertToBlob(String src, ServerAttributeConfig attributeConfig)
      throws InvalidValueSyntaxException {
    try {
      return ByteBuffer.wrap(Base64.getDecoder().decode(src));
    } catch (IllegalArgumentException e) {
      throw new InvalidValueSyntaxException(
          String.format(
              "Invalid base64 syntax; attribute=%s, type=%s, value=%s",
              attributeConfig.name, attributeConfig.type, src));
    }
  }
}
