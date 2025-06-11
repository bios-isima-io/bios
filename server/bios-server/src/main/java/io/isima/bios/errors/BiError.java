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

public enum BiError implements BiosError {
  GENERIC_BI_ERROR("BI00", Status.INTERNAL_SERVER_ERROR, "Generic BI error"),
  NO_ASSIGN_SELF(
      "BI01",
      Status.BAD_REQUEST,
      "You cannot assign yourself to other group. Please ask admin to do this for you."),
  NO_ENABLE_SELF(
      "BI02",
      Status.BAD_REQUEST,
      "You cannot enable your own account. Please ask admin to do this for you."),
  NO_DISABLE_SELF(
      "BI03",
      Status.BAD_REQUEST,
      "You cannot disable your own account. Please ask admin to do this for you."),
  INVALID_PAGE_NUMBER("BI04", Status.BAD_REQUEST, "Page must be positive integer."),
  INVALID_PAGE_SIZE("BI05", Status.BAD_REQUEST, "Page size is out of range (1-250)"),
  OUT_OF_RANGE_PAGE("BI06", Status.BAD_REQUEST, "Page is out of range.");

  private final String errorCode;
  private final Status status;
  private final String message;

  private BiError(String errorCode, Status status, String message) {
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
