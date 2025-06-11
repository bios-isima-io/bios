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

import static io.isima.bios.models.IntegrationsAuthenticationType.LOGIN;
import static io.isima.bios.models.IntegrationsAuthenticationType.MONGODB_X509;

import io.isima.bios.admin.ValidationState;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CdcSourceConfigValidator {

  public static void validate(
      ImportSourceConfig config, ValidationState state, List<ValidatorException> errors) {
    final var allowedAuthType = new ArrayList<>(List.of(LOGIN));
    if (config.getType() == ImportSourceType.MONGODB) {
      require(config.getEndpoints(), "endpoints", state, errors);
      require(config.getReplicaSet(), "replicaSet", state, errors);
      allowedAuthType.add(MONGODB_X509);
    } else {
      requireAny(
          Arrays.asList(config.getDatabaseHost(), config.getEndpoint()),
          List.of("databaseHost", "endpoint"),
          state,
          errors);
    }
    require(config.getDatabaseName(), "databaseName", state, errors);

    AuthenticationValidators.validate(
        config.getAuthentication(), allowedAuthType, state.next("authentication"), errors);

    validateSsl(config, state.next("ssl"), errors);
  }

  private static void validateSsl(
      ImportSourceConfig config, ValidationState state, List<ValidatorException> errors) {
    final var ssl = config.getSsl();
    if (ssl == null) {
      return;
    }
    require(ssl.getMode(), "mode", state, errors);
    if (ssl.getClientCertificateContent() != null) {
      require(ssl.getClientCertificatePassword(), "clientCertificatePassword", state, errors);
    }
  }

  private static <T> void require(
      T value, String name, ValidationState state, List<ValidatorException> errors) {
    if (value == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.next(name).buildMessage("Property must be set")));
    } else if (value instanceof String && value.toString().isBlank()) {
      errors.add(
          new InvalidValueValidatorException(
              state.next(name).buildMessage("Value must not be blank")));
    }
  }

  /** Requires any of given parameters. */
  private static <T> void requireAny(
      List<T> parameters,
      List<String> names,
      ValidationState state,
      List<ValidatorException> errors) {
    if (parameters.stream()
        .noneMatch(
            (parameter) ->
                parameter != null
                    && (!(parameter instanceof String) || !parameter.toString().isBlank()))) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage("One of properties (%s) must be set", String.join(", ", names))));
    }
  }
}
