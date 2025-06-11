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
package io.isima.bios.models.v1;

import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.MissingAttributePolicyV1;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to handle TFOS attributes. */
public class Attributes {

  private static final Logger logger = LoggerFactory.getLogger(Attributes.class);

  /**
   * Method to convert an input string from user to a internal data object.
   *
   * <p>The method conducts the conversion based on the ValueType information in the given attribute
   * descriptor.
   *
   * @param src Source string.
   * @param desc Attribute descriptor.
   * @return Converted data object.
   * @throws TfosException When the conversion fails
   */
  public static Object convertValue(final String src, AttributeDesc desc)
      throws InvalidValueSyntaxException, InvalidEnumException {
    InternalAttributeType attributeType = desc.getAttributeType();
    // Use default value if necessary.
    String normalized = src;
    if (attributeType != STRING && attributeType != BLOB) {
      normalized = src.trim();
      if (desc.getMissingValuePolicy() == MissingAttributePolicyV1.USE_DEFAULT
          && normalized.isEmpty()) {
        return desc.getInternalDefaultValue();
      }
    }
    try {
      if (attributeType != ENUM) {
        return attributeType.parse(normalized);
      } else {
        int index = desc.getEnum().indexOf(normalized);
        if (index >= 0) {
          return Integer.valueOf(index);
        } else {
          throw new InvalidEnumException("Specified name is not in the list of enum entries");
        }
      }
    } catch (InvalidValueSyntaxException | InvalidEnumException e) {
      e.appendMessage(
          String.format(
              "attribute=%s, type=%s, value=%s", desc.getName(), attributeType.biosName(), src));
      throw e;
    }
  }

  /**
   * Method to convert an internal data object to type for output, i.e., JSON serialization ready.
   *
   * @param value Data object of internal type
   * @param desc Attribute descriptor of the data object
   * @return Converted data of output type
   */
  public static Object dataEngineToPlane(
      Object value, AttributeDesc desc, String tenantName, String streamName) {
    if (desc == null) {
      throw new IllegalArgumentException("parameter desc may not be null");
    }
    if (value == null) {
      return null;
    }
    switch (desc.getAttributeType()) {
      case ENUM:
        {
          final int index = (Integer) value;
          if (index < 0 || index >= desc.getEnum().size()) {
            // Throwing RunTimeException is not really good; ApplicationException is better, but
            // it's
            // not an unchecked exception.
            throw new RuntimeException(
                String.format(
                    "Invalid enum index %d; tenant=%s, stream=%s, attribute=%s, enumValues=%d",
                    index, tenantName, streamName, desc.getName(), desc.getEnum().size()));
          }
          return desc.getEnum().get(index);
        }
      default:
        return value;
    }
  }

  /**
   * Parse JSON-deserialized defaultValue input to convert to TFOS internal data type.
   *
   * @param defaultValue JSON-deserialized defaultValue input. The type is string, integer, or
   *     boolean.
   * @param desc Attribute descriptor of the input data object
   * @return Data object converted to TFOS internal type
   * @throws InvalidValueSyntaxException Thrown when input data is invalid for the expected type.
   */
  public static Object parseDefaultValue(Object defaultValue, AttributeDesc desc)
      throws InvalidValueSyntaxException {
    final String name = desc.getName();
    final InternalAttributeType type = desc.getAttributeType();
    boolean checkIfString = type == ENUM;
    if (type == BLOB) {
      if (defaultValue instanceof byte[]) {
        return ByteBuffer.wrap((byte[]) defaultValue);
      }
      checkIfString = true;
    }
    if (checkIfString && !(defaultValue instanceof String)) {
      throw new InvalidValueSyntaxException(
          String.format(
              "%s%s default value must be a string; attribute=%s, defaultValue=%s",
              type.name().substring(0, 1),
              type.name().substring(1).toLowerCase(),
              name,
              defaultValue));
    }
    try {
      return Attributes.convertValue(defaultValue.toString(), desc);
    } catch (InvalidEnumException e) {
      throw new InvalidValueSyntaxException(
          String.format(
              "Enum default value must match one of entries; attribute=%s, defaultValue=%s",
              name, defaultValue));
    }
  }
}
