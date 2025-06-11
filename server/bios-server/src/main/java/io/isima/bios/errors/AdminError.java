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

public enum AdminError implements BiosError {
  GENERIC_ADMIN_ERROR("ADMIN00", Status.INTERNAL_SERVER_ERROR, "Generic Admin Error"),
  TENANT_LIST_ERROR("ADMIN01", Status.INTERNAL_SERVER_ERROR, "Error while fetching tenant list"),
  STREAM_LIST_ERROR("ADMIN02", Status.INTERNAL_SERVER_ERROR, "Error while fetching stream list"),
  TENANT_NOT_FOUND("ADMIN03", Status.NOT_FOUND, "Tenant not found"),
  STREAM_NOT_FOUND("ADMIN04", Status.NOT_FOUND, "Stream not found"),
  ATTRIBUTE_NOT_FOUND("ADMIN05", Status.NOT_FOUND, "Stream attribute not found"),
  ADDITIONAL_ATTRIBUTE_NOT_FOUND(
      "ADMIN06", Status.NOT_FOUND, "Stream additional attribute not found"),
  PREPROCESS_NOT_FOUND("ADMIN07", Status.NOT_FOUND, "Stream preprocess not found"),
  TENANT_ALREADY_EXISTS("ADMIN08", Status.CONFLICT, "Tenant with same name already exists"),
  STREAM_ALREADY_EXISTS("ADMIN09", Status.CONFLICT, "Stream with same name already exists"),
  ATTRIBUTE_ALREADY_EXISTS("ADMIN10", Status.CONFLICT, "Attribute with same name already exists"),
  PREPROCESS_ALREADY_EXISTS("ADMIN11", Status.CONFLICT, "Preprocess with same name already exists"),
  TENANT_VERSION_ERROR(
      "ADMIN12",
      Status.INTERNAL_SERVER_ERROR,
      "Error while fetching tenant config for a particular version"),
  JSON_CONVERSION_ERROR(
      "ADMIN13", Status.INTERNAL_SERVER_ERROR, "Error while converting to JSON format"),
  STREAM_VERSION_MISMATCH("ADMIN14", Status.BAD_REQUEST, "Given stream version no longer exists"),
  NO_SUCH_ENTITY("ADMIN15", Status.NOT_FOUND, "No such entity"),
  ADMIN_CHANGE_REQUEST_TO_SAME(
      "ADMIN16", Status.BAD_REQUEST, "Requested config for change is identical to existing"),
  INVALID_VALUE("ADMIN17", Status.BAD_REQUEST, "Invalid value"),
  INVALID_DEFAULT_VALUE("ADMIN18", Status.BAD_REQUEST, "Invalid default value"),
  INVALID_PRIMARY_KEY("ADMIN19", Status.BAD_REQUEST, "Invalid primary key"),
  INVALID_ATTRIBUTE("ADMIN20", Status.BAD_REQUEST, "Invalid attribute"),
  SIGNAL_NOT_FOUND("ADMIN21", Status.NOT_FOUND, "Signal not found"),
  CONTEXT_NOT_FOUND("ADMIN22", Status.NOT_FOUND, "Context not found"),
  SIGNAL_ALREADY_EXISTS(
      "ADMIN23", Status.CONFLICT, "Signal or Context with same name already exists"),
  CONTEXT_ALREADY_EXISTS("ADMIN24", Status.CONFLICT, "Stream with same name already exists"),
  ALREADY_EXISTS("ADMIN25", Status.CONFLICT, "Specified entity already exists");

  private final String errorCode;
  private final Status status;
  private final String message;

  private AdminError(String errorCode, Status status, String message) {
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
