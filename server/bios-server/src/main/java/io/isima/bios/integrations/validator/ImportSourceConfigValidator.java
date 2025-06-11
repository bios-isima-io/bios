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
import io.isima.bios.exceptions.validator.MultipleValidatorViolationsException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.ImportSourceConfig;
import java.util.ArrayList;
import java.util.List;

public class ImportSourceConfigValidator {

  protected static final String IMPORT_SOURCE_CONFIG = "importSourceConfig";
  protected static final String IMPORT_SOURCE_NAME = "importSourceName";
  protected static final String IMPORT_SOURCE_ID = "importSourceId";
  protected static final String TYPE = "type";

  protected final ImportSourceConfig config;
  protected final ValidationState state;
  protected final List<ValidatorException> errors;

  protected boolean used;

  public static ImportSourceConfigValidator newValidator(ImportSourceConfig config) {
    return new ImportSourceConfigValidator(config);
  }

  protected ImportSourceConfigValidator(ImportSourceConfig config) {
    this.config = config;
    state = new ValidationState(IMPORT_SOURCE_CONFIG);
    errors = new ArrayList<>();
    used = false;
  }

  public void validate() throws ValidatorException {
    if (used) {
      throw new IllegalStateException("This validator is used already. Create a new instance");
    }
    used = true;

    if (config.getImportSourceName() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(IMPORT_SOURCE_NAME + " is required")));
    } else {
      state.putContext(IMPORT_SOURCE_NAME, config.getImportSourceName());
    }

    if (config.getImportSourceId() == null) {
      errors.add(
          new ConstraintViolationValidatorException(
              state.buildMessage(IMPORT_SOURCE_ID + " is required")));
    } else {
      state.putContext(IMPORT_SOURCE_ID, config.getImportSourceId());
    }

    if (config.getType() == null) {
      errors.add(
          new ConstraintViolationValidatorException(state.buildMessage(TYPE + "must be set")));
    } else {
      state.putContext(TYPE, config.getType().stringify());
    }

    switch (config.getType()) {
      case MYSQL:
      case POSTGRES:
      case SQLSERVER:
      case ORACLE:
      case MONGODB:
        CdcSourceConfigValidator.validate(config, state, errors);
        break;
      case WEBHOOK:
        WebhookSourceConfigValidator.validate(config, state, errors);
        break;
      default:
        // we'll go with the default validator only
    }

    // check collected errors and throw
    if (errors.size() > 1) {
      throw new MultipleValidatorViolationsException(errors);
    } else if (errors.size() == 1) {
      throw errors.get(0);
    }
  }
}
