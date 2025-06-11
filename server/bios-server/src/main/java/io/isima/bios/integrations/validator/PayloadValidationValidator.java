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
import io.isima.bios.models.IntegrationsPayloadValidation;
import java.util.List;

public class PayloadValidationValidator {
  public static void validate(
      IntegrationsPayloadValidation payloadValidation,
      ValidationState state,
      List<ValidatorException> errors) {
    switch (payloadValidation.getType()) {
      case HMAC_SIGNATURE:
        if (null == payloadValidation.getHmacHeader()
            || payloadValidation.getHmacHeader().isBlank()) {
          errors.add(
              new ConstraintViolationValidatorException(
                  state.next("hmacHeader").buildMessage("'hmacHeader' must be set")));
        }
        if (null == payloadValidation.getSharedSecret()
            || payloadValidation.getSharedSecret().isBlank()) {
          errors.add(
              new ConstraintViolationValidatorException(
                  state.next("sharedSecret").buildMessage("'sharedSecret' must be set")));
        }
        break;
      default:
        errors.add(
            new ConstraintViolationValidatorException(
                state.next("type").buildMessage("Unsupported payload validation type")));
    }
  }
}
