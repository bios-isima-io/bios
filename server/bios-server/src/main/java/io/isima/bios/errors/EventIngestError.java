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

public enum EventIngestError implements BiosError {
  NO_TENANT("INGEST00", Status.BAD_REQUEST, "Tenant name is not set"),
  NO_STREAM("INGEST01", Status.BAD_REQUEST, "Stream name is not set"),
  KEYVALUE_SYNTAX_ERROR(
      "INGEST02",
      Status.BAD_REQUEST,
      "Syntax Error: All event values should be key-value pairs or only values"),
  TOO_MANY_VALUES(
      "INGEST03", Status.BAD_REQUEST, "Source event string has more values than pre-defined"),
  TOO_FEW_VALUES(
      "INGEST04", Status.BAD_REQUEST, "Source event string has less values than pre-defined"),
  UNKNOWN_KEY("INGEST05", Status.BAD_REQUEST, "Unknown key is specified"),
  FOREIGN_KEY_MISSING(
      "0x500006", Status.BAD_REQUEST, "Missed to lookup context with a foreign key"),
  CSV_SYNTAX_ERROR("0x500007", Status.BAD_REQUEST, "CSV syntax error"),
  ATOMIC_OPERATION_ABORTED(
      "0x500008",
      Status.BAD_GATEWAY,
      "Atomic operation aborted due to an error from another bundled operation"),
  ATOMIC_COMMIT_fAILED("0x500009", Status.INTERNAL_SERVER_ERROR, "Atomic operation commit failed"),
  ;

  private final String errorCode;
  private final Status status;
  private final String message;

  private EventIngestError(String errorCode, Status status, String message) {
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
