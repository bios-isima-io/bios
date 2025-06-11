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

import static io.isima.bios.models.IntegrationsAuthenticationType.HTTP_AUTHORIZATION_HEADER;
import static io.isima.bios.models.IntegrationsAuthenticationType.HTTP_HEADERS_PLAIN;
import static io.isima.bios.models.IntegrationsAuthenticationType.IN_MESSAGE;
import static io.isima.bios.models.IntegrationsAuthenticationType.LOGIN;

import io.isima.bios.admin.ValidationState;
import io.isima.bios.admin.Validators;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.ImportSourceConfig;
import java.util.List;
import java.util.Set;

public class WebhookSourceConfigValidator {

  public static void validate(
      ImportSourceConfig config, ValidationState state, List<ValidatorException> errors) {
    Validators.requires(
        config.getWebhookPath(), "webhookPath", "for a Webhook data source", state, errors);

    final var authentication = config.getAuthentication();
    if (authentication != null) {
      final var authState = state.next("authentication");
      if (!Set.of(LOGIN, IN_MESSAGE, HTTP_AUTHORIZATION_HEADER, HTTP_HEADERS_PLAIN)
          .contains(authentication.getType())) {
        errors.add(
            new ConstraintViolationValidatorException(
                authState
                    .next("type")
                    .buildMessage(
                        String.format(
                            "Type '%s' is not supported for Webhook data source",
                            authentication.getType().stringify()))));
      } else {
        AuthenticationValidators.validate(authentication, List.of(), state, errors);
      }
    }
    if (config.getPayloadValidation() != null) {
      PayloadValidationValidator.validate(config.getPayloadValidation(), state, errors);
    }
  }
}
