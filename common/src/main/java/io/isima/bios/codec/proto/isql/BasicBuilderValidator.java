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
package io.isima.bios.codec.proto.isql;

import java.util.List;
import java.util.Map;

/**
 * Validates build items mainly for nulls, empty lists and strings and builds an errorList that is
 * thrown at the end when the build is invoked.
 */
public final class BasicBuilderValidator {
  /**
   * Validates that the time range is within bounds.
   *
   * @param startTime Start time
   * @param delta Delta from start time
   * @throws IllegalArgumentException if invalid
   */
  public void validateTimeRange(long startTime, long delta) {
    if (startTime < 0
        || startTime + delta < 0
        || (delta > 0 && (startTime > (Long.MAX_VALUE - delta)))) {
      throw new IllegalArgumentException(
          "Time range is out of bounds. Valid range is between 0 and " + Long.MAX_VALUE);
    }
  }

  /**
   * Checks for a null or empty string.
   *
   * @param param String parameter to check
   * @param paramName Descriptive name of the parameter
   * @throws IllegalArgumentException if invalid
   */
  public void validateStringParam(String param, String paramName) {
    if (null == param || param.isBlank()) {
      throw new IllegalArgumentException(
          String.format("Parameter '%s' may not be null or empty", paramName));
    }
  }

  public void validatePositive(int param, String paramName) {
    if (param <= 0) {
      throw new IllegalArgumentException(
          String.format("Parameter '%s' must be a positive integer", paramName));
    }
  }

  public void validatePositive(long param, String paramName) {
    if (param <= 0) {
      throw new IllegalArgumentException(
          String.format("Parameter '%s' must be a positive long", paramName));
    }
  }

  /**
   * Checks for a non empty, non null string list.
   *
   * @throws IllegalArgumentException if invalid
   */
  public void validateStringArray(String[] attributes, String paramName) {
    checkArray(attributes, paramName);
    for (int i = 0; i < attributes.length; ++i) {
      final String s = attributes[i];
      checkStringItem(s, i, paramName);
    }
  }

  /**
   * Checks for a non empty, non null string list.
   *
   * @throws IllegalArgumentException if invalid
   */
  public void validateStringList(List<String> attributes, String paramName) {
    validateStringList(attributes, paramName, 0);
  }

  /**
   * Checks for a non empty, non null string list.
   *
   * @throws IllegalArgumentException if invalid
   */
  public void validateStringList(List<String> attributes, String paramName, int expectedSize) {
    checkList(attributes, paramName, expectedSize);
    for (int i = 0; i < attributes.size(); ++i) {
      final String s = attributes.get(i);
      checkStringItem(s, i, paramName);
    }
  }

  /**
   * Checks the object for null.
   *
   * @param obj Object to check
   * @param paramName Descriptive name of the param
   * @throws IllegalArgumentException if invalid
   */
  public void validateObject(Object obj, String paramName) {
    if (obj == null) {
      throw new IllegalArgumentException(
          String.format("Parameter '%s' may not be null", paramName));
    }
  }

  public void validateMap(Map<String, Object> attributes, String paramName) {
    if (attributes == null) {
      throw new IllegalArgumentException(String.format("Map '%s' may not be null", paramName));
    }
    if (attributes.size() <= 0) {
      throw new IllegalArgumentException(String.format("Map '%s' cannot be empty", paramName));
    }
  }

  public void validateObjectArray(Object[] array, int expectedSize, String paramName) {
    checkArray(array, paramName);
    if (expectedSize > 0 && array.length != expectedSize) {
      throw new IllegalArgumentException(
          String.format("'%s' must have only %d element(s)", paramName, expectedSize));
    }
  }

  private void checkArray(Object[] array, String paramName) {
    if (array == null || array.length <= 0) {
      throw new IllegalArgumentException(
          String.format("Array Parameter '%s' may not be null or empty", paramName));
    }
  }

  private <T> void checkList(List<T> list, String paramName, int expectedSize) {
    if (list == null || list.size() <= 0) {
      throw new IllegalArgumentException(
          String.format("List Parameter '%s' may not be null or empty", paramName));
    }
    if (expectedSize > 0 && list.size() != expectedSize) {
      throw new IllegalArgumentException(
          String.format("List Parameter '%s' must have '%d' items", paramName, expectedSize));
    }
  }

  private void checkStringItem(String s, int i, String paramName) {
    if (null == s || s.isBlank()) {
      throw new IllegalArgumentException("'" + paramName + "[" + i + "]' may not be null or empty");
    }
  }
}
