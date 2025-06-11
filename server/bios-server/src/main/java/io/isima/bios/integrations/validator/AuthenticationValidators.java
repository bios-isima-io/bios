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
package io.isima.bios.integrations.validator;

import io.isima.bios.admin.ValidationState;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.IntegrationsAuthentication;
import io.isima.bios.models.IntegrationsAuthenticationType;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AuthenticationValidators {

  public static void validate(
      IntegrationsAuthentication authentication,
      List<IntegrationsAuthenticationType> allowedTypes,
      ValidationState state,
      List<ValidatorException> errors) {
    Objects.requireNonNull(allowedTypes);

    if (authentication == null) {
      errors.add(
          new ConstraintViolationValidatorException(state.buildMessage("Property must be set")));
      return;
    }

    if (!allowedTypes.isEmpty() && !allowedTypes.contains(authentication.getType())) {
      final var allowed =
          allowedTypes.stream()
              .map((type) -> String.format("'%s'", type))
              .collect(Collectors.joining(", "));
      errors.add(
          new ConstraintViolationValidatorException(
              state
                  .next("type")
                  .buildMessage(
                      "Type must be one of %s, but %s was specified",
                      allowed, authentication.getType())));
      return;
    }

    switch (authentication.getType()) {
      case LOGIN:
        validateLogin(authentication, state, errors);
        break;
      case MONGODB_X509:
        validateMongoX509(authentication, state, errors);
        break;
      default:
        // TODO(BIOS-5507): Add validators for other auth types
    }
  }

  private static void validateLogin(
      IntegrationsAuthentication authentication,
      ValidationState state,
      List<ValidatorException> errors) {

    requireUser(authentication, state, errors);

    if (authentication.getPassword() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.next("password").buildMessage("Property 'password' is required")));
    }
  }

  private static void validateMongoX509(
      IntegrationsAuthentication authentication,
      ValidationState state,
      List<ValidatorException> errors) {
    requireUser(authentication, state, errors);
  }

  private static boolean requireUser(
      IntegrationsAuthentication authentication,
      ValidationState state,
      List<ValidatorException> errors) {
    if (authentication.getUser() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.next("user").buildMessage("Property 'user' is required")));
      return false;
    }
    return true;
  }
}
