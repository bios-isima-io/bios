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

public enum TeachBiosError implements BiosError {
  GENERIC_TEACH_BIOS_ERROR(
      "TEACHBIOS00", Status.INTERNAL_SERVER_ERROR, "Generic error in Teach bi(OS) API"),
  MAX_ROW_LIMIT_EXCEEDED(
      "TEACHBIOS01",
      Status.BAD_REQUEST,
      "Oops, you just input more than 1000 rows, which drained "
          + "bi(OS)'s superpower. Please try again and make sure to input less than 1000 rows. "),
  EMPTY_INPUT_NOW_ALLOWED(
      "TEACHBIOS02",
      Status.BAD_REQUEST,
      "Oops, you did not input anything. Please make sure to type " + "in something."),
  INVALID_INPUT(
      "TEACHBIOS03",
      Status.BAD_REQUEST,
      "Oops, it looks like bi(OS) did not understand your input rows."
          + " Did you format them correctly?"),
  UNSUPPORTED_DELIMITER(
      "TEACHBIOS04",
      Status.BAD_REQUEST,
      "Oops, bi(OS)'s superpower does not reach delimiter in input "
          + "yet.Please make sure to use correct delimiter in your input rows for now. "),
  TENANT_NAME_MISMATCH(
      "TEACHBIOS05",
      Status.BAD_REQUEST,
      "Oops, tenant name in request is different from tenant to which" + " user belongs"),
  MAX_COLUMN_LIMIT_EXCEEDED(
      "TEACHBIOS06",
      Status.BAD_REQUEST,
      "Oops, you just input more than 1000 columns, which drained "
          + "bi(OS)'s superpower. Please try again and make sure to input less than 1000 columns. ");

  private final String errorCode;
  private final Status status;
  private final String message;

  TeachBiosError(String errorCode, Status status, String message) {
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
