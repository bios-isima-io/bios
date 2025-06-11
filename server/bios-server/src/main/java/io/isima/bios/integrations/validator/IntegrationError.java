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

import io.isima.bios.errors.BiosError;
import javax.ws.rs.core.Response.Status;

public enum IntegrationError implements BiosError {
  SOURCE_CONNECTION_ERROR(
      "INTEGRATION00", Status.SERVICE_UNAVAILABLE, "Failed to connect import source"),
  SOURCE_SIGN_IN_FAILURE("INTEGRATION01", Status.BAD_REQUEST, "Failed to sign in import source"),
  SOURCE_ACCESS_RULE_VIOLATION("INTEGRATION02", Status.BAD_REQUEST, "Source access rule violation"),
  IMPORT_SOURCE_UNABLE_TO_PARSE("INTEGRATION03", Status.BAD_REQUEST, "Unable to parse source data"),
  IMPORT_SOURCE_CONFIGURATION_ERROR(
      "INTEGRATION04", Status.BAD_REQUEST, "Import source configuration error"),
  ;

  private final String errorCode;
  private final Status status;
  private final String message;

  private IntegrationError(String errorCode, Status status, String message) {
    this.errorCode = errorCode;
    this.status = status;
    this.message = message;
  }

  @Override
  public String getErrorCode() {
    return errorCode;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public String getErrorMessage() {
    return message;
  }
}
