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
package io.isima.bios.req;

import io.isima.bios.models.ViewFunction;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates build items mainly for nulls, empty lists and strings and builds an errorList that is
 * thrown at the end when the build is invoked.
 *
 * <p>This validator skips all validation that is already validated during compile time.
 */
public final class BasicBuilderValidator {
  private List<String> errorList;

  /**
   * Validates that the time range is within bounds.
   *
   * @param startTime Start time
   * @param delta Delta from start time
   */
  public void validateTimeRange(long startTime, long delta) {
    if (startTime < 0
        || startTime + delta < 0
        || (delta > 0 && (startTime > (Long.MAX_VALUE - delta)))) {
      addError("Time range is out of bounds. Valid range is between 0 and " + Long.MAX_VALUE);
    }
  }

  /**
   * Checks for a null or empty string.
   *
   * @param param String parameter to check
   * @param paramName Descriptive name of the parameter
   * @return true if valid and no errors so far, false otherwise
   */
  public boolean validateStringParam(String param, String paramName) {
    if (null == param || param.isBlank()) {
      addError(String.format("Parameter '%s' may not be null or empty", paramName));
      return false;
    }
    return true;
  }

  /**
   * Checks for a null or empty string and uses additional information to create error information.
   *
   * @param param String parameter to check
   * @param paramName Descriptive name for parameter
   * @param additional Additional info to be appended to the descriptive name separated by #
   */
  public void validateStringParam(String param, String paramName, String... additional) {
    if (null == param || param.isBlank()) {
      addError(
          String.format(
              "Parameter '%s' may not be null or empty", concatParamName(paramName, additional)));
    }
  }

  /**
   * Checks for a non empty, non null string list.
   *
   * @param stringList list of strings
   * @param paramName Descriptive name for parameter
   * @return true if valid
   */
  public boolean validateStringList(List<String> stringList, String paramName) {
    boolean success = checkNull(stringList, paramName);
    if (success) {
      int i = 0;
      for (String s : stringList) {
        if (null == s || s.isBlank()) {
          addError("'" + paramName + "[" + i + "]' may not be null or empty");
          success = false;
        }
        i++;
      }
    }
    return success;
  }

  /**
   * Checks a list of objects for null objects.
   *
   * @param objectList list of objects
   * @param paramName Descriptive name for the parameter
   * @return true if valid
   */
  public boolean validateList(List objectList, String paramName) {
    boolean success = checkNull(objectList, paramName);
    if (success) {
      int i = 0;
      for (Object s : objectList) {
        if (null == s) {
          addError("'" + paramName + "[" + i + "]' may not be null");
          success = false;
        }
        i++;
      }
    }
    return success;
  }

  /**
   * Validates the specified aggregate and view combination for correctness.
   *
   * @param viewFunction enum specifying the view function
   * @param aggregateCount number of aggregates specified
   */
  public void validateAggregatesAndView(ViewFunction viewFunction, int aggregateCount) {
    if (viewFunction.equals(ViewFunction.GROUP)) {
      if (aggregateCount <= 0) {
        addError("Parameter 'aggregates' must be specified for 'groupBy' view");
      }
    }
  }

  /**
   * Checks zero length arrays.
   *
   * @param array object array
   * @param paramName Descriptor for the parameter
   */
  public void validateArray(Object[] array, String paramName) {
    if (array == null || array.length <= 0) {
      addError(String.format("Array Parameter '%s' may not be null or empty", paramName));
    }
  }

  /**
   * Checks the object for null.
   *
   * @param obj Object to check
   * @param paramName Descriptive name of the param
   * @return true if valid
   */
  public boolean validateObject(Object obj, String paramName) {
    if (obj == null) {
      addError(String.format("Parameter '%s' may not be null", paramName));
      return false;
    }
    return errorList == null;
  }

  /**
   * Checks if the object is buildable and there are no errors during the build process.
   *
   * @return true if buildable
   */
  public boolean isBuildable() {
    return errorList == null || errorList.isEmpty();
  }

  /**
   * Gets the list of errors as a single print friendly string.
   *
   * @param requestType Type of the request that was build
   * @return List of errors as a single string
   */
  public String getAndClearError(String requestType) {
    StringBuilder sb =
        new StringBuilder("Cannot build ")
            .append(requestType)
            .append(". Following errors were detected")
            .append(":")
            .append(System.lineSeparator());
    errorList.forEach((s) -> sb.append(s).append(System.lineSeparator()));
    errorList = null;
    return sb.toString();
  }

  private boolean checkNull(List param, String paramName) {
    if (null == param || param.size() <= 0) {
      addError(String.format("List Parameter '%s' may not be null or empty", paramName));
      return false;
    }
    return true;
  }

  private void addError(String error) {
    if (errorList == null) {
      // create error list only if there is an error
      errorList = new ArrayList<>();
    }
    errorList.add(error);
  }

  private String concatParamName(String paramName, String[] additional) {
    if (additional == null || additional.length <= 0) {
      return paramName;
    }
    StringBuilder sb = new StringBuilder(paramName);
    for (String a : additional) {
      sb.append("#").append(a);
    }
    return sb.toString();
  }
}
