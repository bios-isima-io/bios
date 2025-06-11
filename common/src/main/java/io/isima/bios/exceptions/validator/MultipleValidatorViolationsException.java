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
package io.isima.bios.exceptions.validator;

import java.util.List;
import lombok.Getter;

@Getter
public class MultipleValidatorViolationsException extends ValidatorException {

  private final List<ValidatorException> errors;

  public MultipleValidatorViolationsException(List<ValidatorException> errors) {
    super(errorsToMessage(errors), ValidationError.MULTIPLE_ERRORS);
    this.errors = errors;
  }

  private static String errorsToMessage(List<ValidatorException> errors) {
    final var sb = new StringBuilder();
    sb.append(errors.size()).append(" violations: ");
    String context = "";
    String delimiter = "";
    for (int i = 0; i < errors.size(); ++i) {
      final var error = errors.get(i);
      final var elements = error.getMessage().split(";", 2);
      final var message = elements[0].trim();
      if (elements.length > 1) {
        final String currentContext = elements[1].trim();
        if (currentContext.length() > context.length()) {
          context = currentContext;
        }
      }
      sb.append(delimiter)
          .append("[")
          .append(i)
          .append("] ")
          .append(error.getErrorType())
          .append(" - ")
          .append(message);
      delimiter = ", ";
    }
    sb.append("; ").append(context);
    return sb.toString();
  }
}
