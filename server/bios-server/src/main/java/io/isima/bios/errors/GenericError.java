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
package io.isima.bios.errors;

import javax.ws.rs.core.Response.Status;

public enum GenericError implements BiosError {
  APPLICATION_ERROR("GENERIC00", Status.INTERNAL_SERVER_ERROR, "Generic server error"),
  TIMEOUT("GENERIC01", Status.INTERNAL_SERVER_ERROR, "Operation timed out"),
  INVALID_VALUE_SYNTAX("GENERIC02", Status.BAD_REQUEST, "Invalid value syntax"),
  INVALID_ENUM_ENTRY("GENERIC03", Status.BAD_REQUEST, "Invalid enum"),
  CONSTRAINT_VIOLATION("GENERIC04", Status.BAD_REQUEST, "Constraint violation"),
  CONSTRAINT_WARNING(
      "GENERIC05", Status.BAD_REQUEST, "Constraint warning (Set force flag to override)"),
  NOT_IMPLEMENTED("GENERIC06", Status.NOT_IMPLEMENTED, "Not implemented"),
  QUERY_SCALE_TOO_LARGE("GENERIC07", Status.REQUEST_ENTITY_TOO_LARGE, "Too large query scale"),
  INVALID_ID("GENERIC08", Status.NOT_FOUND, "Specified ID is invalid"),
  INVALID_REQUEST("GENERIC09", Status.BAD_REQUEST, "Invalid request"),
  OPERATION_CANCELED("GENERIC0a", Status.SERVICE_UNAVAILABLE, "Operation canceled"),
  OPERATION_UNEXECUTABLE("GENERIC0b", Status.BAD_REQUEST, "Unable to execute requested operation"),
  SERVER_MEMORY_CAPACITY_EXCEEDED(
      "GENERIC0c", Status.SERVICE_UNAVAILABLE, "Server memory capacity exceeded"),
  INVALID_CONFIGURATION(
      "GENERIC0d",
      Status.INTERNAL_SERVER_ERROR,
      "Unable to complete operation due to invalid server configuration"),
  BIOS_APPS_DEPLOYMENT_FAILED(
      "GENERIC0e",
      Status.INTERNAL_SERVER_ERROR,
      "Failed to deploy bios apps service, check the server log"),
  BUSY("GENERIC0f", Status.SERVICE_UNAVAILABLE, "Server busy"),
  SERVER_IN_MAINTENANCE("GENERIC10", Status.SERVICE_UNAVAILABLE, "Server in maintenance"),
  ;

  private final String errorCode;
  private final Status status;
  private final String message;

  private GenericError(String errorCode, Status status, String message) {
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
