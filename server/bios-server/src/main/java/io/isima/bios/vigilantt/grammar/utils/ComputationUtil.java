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
package io.isima.bios.vigilantt.grammar.utils;

import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;

public class ComputationUtil {

  public static Double getValueAsDouble(Object value) throws UnexpectedValueException {
    if (value instanceof Double) {
      return ((Double) value);
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else {
      throw new UnexpectedValueException(
          String.format("Attribute value %s " + "cannot be converted to double", value));
    }
  }

  public static Boolean getValueAsBooleanOrNull(String value) {
    final String lowerCaseValue = ((String) value).toLowerCase();
    if (lowerCaseValue.equals("true")) {
      return true;
    } else if (lowerCaseValue.equals("false")) {
      return false;
    }
    return null;
  }

  public static Boolean getValueAsBoolean(Object value) throws UnexpectedValueException {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      Boolean valueAsBoolean = getValueAsBooleanOrNull((String) value);
      if (valueAsBoolean != null) {
        return valueAsBoolean;
      }
    }
    throw new UnexpectedValueException(
        String.format("Attribute value %s can't be converted to" + " Boolean ", value));
  }

  public static String getValueAsString(Object value) throws UnexpectedValueException {
    if (value instanceof String) {
      return (String) value;
    } else {
      throw new UnexpectedValueException(
          String.format("Attribute value %s can't be converted to" + " string ", value));
    }
  }

  public static boolean isConvertibleToDouble(Object value) {
    return value instanceof Number;
  }

  public static boolean isConvertibleToDouble(String value) {
    boolean result = false;
    try {
      Double.parseDouble(value);
      result = true;
    } catch (NumberFormatException e) {
      // Input string can't be parsed to double
    }
    return result;
  }

  public static boolean isConvertibleToBoolean(Object value) {
    return value instanceof Boolean;
  }

  public static boolean isConvertibleToBoolean(String value) {
    final Boolean booleanValue = getValueAsBooleanOrNull(value);
    if (booleanValue != null) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean isContainsPredicate(String value) {
    return value.equals("contains");
  }
}
