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

public enum EventExtractError implements BiosError {
  GENERIC_EVENT_EXTRACT_ERROR(
      "EXTRACT00", Status.INTERNAL_SERVER_ERROR, "Generic event extract error"),
  INVALID_ATTRIBUTE("EXTRACT01", Status.BAD_REQUEST, "Attribute name is invalid"),
  ATTRIBUTES_CANNOT_BE_EMPTY(
      "EXTRACT02", Status.BAD_REQUEST, "Attribute list cannot be empty for DISTINCT view"),
  MORE_THAN_ONE_ATTRIBUTE_NOT_ALLOWED(
      "EXTRACT03", Status.BAD_REQUEST, "Only one attribute can be specified for DISTINCT view"),
  ATTRIBUTE_SHOULD_MATCH_VIEW_SETBY(
      "EXTRACT04", Status.BAD_REQUEST, "Attribute should match View.by for DISTINCT view"),
  INVALID_QUERY("EXTRACT05", Status.BAD_REQUEST, "Query failed"),
  FILTER_SYNTAX_ERROR("EXTRACT06", Status.BAD_REQUEST, "Filter syntax error"),
  INVALID_FILTER("EXTRACT07", Status.BAD_REQUEST, "Invalid filter");

  private final String errorCode;
  private final Status status;
  private final String message;

  private EventExtractError(String errorCode, Status status, String message) {
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
