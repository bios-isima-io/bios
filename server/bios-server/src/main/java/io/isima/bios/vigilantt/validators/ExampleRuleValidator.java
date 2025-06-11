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
package io.isima.bios.vigilantt.validators;

public class ExampleRuleValidator implements RuleValidator {

  public ExampleRuleValidator() {}

  public boolean isValid(String rule) {
    String[] parts = rule.split(">");
    if (parts.length != 2 || isNotAlphanumeric(parts[0].trim()) || isNotInteger(parts[1].trim())) {
      return false;
    }
    return true;
  }

  private boolean isNotAlphanumeric(String input) {
    boolean stringMatch = input.matches("^[a-zA-Z0-9_]*$");
    return !stringMatch;
  }

  private boolean isNotInteger(String input) {
    boolean isConvertibleToInteger = false;
    try {
      Integer.parseInt(input);
      isConvertibleToInteger = true;
    } catch (NumberFormatException e) {
      // Not a valid integer
    }
    return !isConvertibleToInteger;
  }
}
