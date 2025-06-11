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

public enum AlertsError implements BiosError {
  GENERIC_ALERTS_ERROR("ALERTS00", Status.INTERNAL_SERVER_ERROR, "Generic alerts error"),
  INVALID_ALERT_CONDITION_ERROR("ALERTS01", Status.BAD_REQUEST, "Invalid Alert condition"),
  UNEXPECTED_VALUE_ERROR(
      "ALERT02",
      Status.INTERNAL_SERVER_ERROR,
      "Unexpected attribute value on evaluating alert expression");

  private final String errorCode;
  private final Status status;
  private final String message;

  AlertsError(String errorCode, Status status, String message) {
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
