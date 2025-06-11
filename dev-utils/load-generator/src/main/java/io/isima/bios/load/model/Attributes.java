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

/**
 * Utility class to handle TFOS attributes.
 */
public class Attributes {

  /**
   * Method to convert an input string from user to a internal data object.
   *
   * <p>
   * The method conducts the conversion based on the ValueType information in the given attribute
   * descriptor.
   * </p>
   *
   * @param src Source string.
   * @param desc Attribute descriptor.
   * @return Converted data object.
   */
  public static Object convertValue(final String src, AttributeDesc desc)
      throws Exception {
    AttributeType attributeType = desc.getAttributeType();
    // Use default value if necessary.
    String normalized = src;
    if (attributeType != AttributeType.STRING && attributeType != AttributeType.BLOB) {
      normalized = src.trim();
    }
    try {
      return attributeType.fromString(normalized);
//      if (attributeType != AttributeType.ENUM) {
//        return attributeType.parse(normalized);
//      } else {
//        int index = desc.getEnum().indexOf(normalized);
//        if (index >= 0) {
//          return Integer.valueOf(index);
//        } else {
//          throw new Exception("Specified name is not in the list of enum entries");
//        }
//      }
    } catch (Exception e) {
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
//  public static Object dataEngineToPlane(Object value, AttributeDesc desc) {
//    if (desc == null) {
//      throw new IllegalArgumentException("parameter desc may not be null");
//    }
//    if (value == null) {
//      return null;
//    }
//    switch (desc.getAttributeType()) {
//      case ENUM: {
//        final int index = (Integer) value;
//        return desc.getEnum().get(index);
//      }
//      default:
//        return value;
//    }
//  }

  /**
   * Parse JSON-deserialized defaultValue input to convert to TFOS internal data type.
   *
   * @param defaultValue JSON-deserialized defaultValue input. The type is string, integer, or
   *        boolean.
   * @param desc Attribute descriptor of the input data object
   * @return Data object converted to TFOS internal type
   */
  public static Object parseDefaultValue(Object defaultValue, AttributeDesc desc)
      throws Exception {
    final String name = desc.getName();
    final AttributeType type = desc.getAttributeType();
//    if ((type == AttributeType.ENUM || type == AttributeType.BLOB)
//        && !(defaultValue instanceof String)) {
//      throw new Exception(String.format(
//          "%s%s default value must be a string; attribute=%s, defaultValue=%s",
//          type.name().substring(0, 1), type.name().substring(1).toLowerCase(), name,
//          defaultValue));
//    }
    try {
      return Attributes.convertValue(defaultValue.toString(), desc);
    } catch (Exception e) {
      throw new Exception(String.format(
          "Enum default value must match one of entries; attribute=%s, defaultValue=%s",
          name, defaultValue));
    }
  }
}
