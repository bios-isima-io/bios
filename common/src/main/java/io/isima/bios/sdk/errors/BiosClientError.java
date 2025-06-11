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
package io.isima.bios.sdk.errors;

import io.isima.bios.errors.BiosError;
import javax.ws.rs.core.Response.Status;

public enum BiosClientError implements BiosError {
  OK(0, Status.OK, "OK"),
  GENERIC_CLIENT_ERROR(
      0x100001, Status.INTERNAL_SERVER_ERROR, "An unexpected error occurred on the client"),
  INVALID_ARGUMENT(
      0x100002, Status.BAD_REQUEST, "CommonError while validating arguments passed to the API"),
  SESSION_INACTIVE(
      0x100003,
      Status.INTERNAL_SERVER_ERROR,
      "A request for an operation was issued to the session, but it is closed already"),
  PARSER_ERROR(0x100004, Status.BAD_REQUEST, "Failed to convert a string to a TFOS request object"),
  CLIENT_ALREADY_STARTED(0x100005, Status.BAD_REQUEST, "Session has already started"),
  REQUEST_TOO_LARGE(0x100006, Status.BAD_REQUEST, "Request entity too large"),
  INVALID_STATE(0x100007, Status.INTERNAL_SERVER_ERROR, "Request entity too large"),
  CLIENT_CHANNEL_ERROR(
      0x100008,
      Status.INTERNAL_SERVER_ERROR,
      "SDK failed to communicate with peer for an unexpected reason"),
  SERVER_CONNECTION_FAILURE(0x200001, Status.BAD_GATEWAY, "SDK failed to connect server"),
  SERVER_CHANNEL_ERROR(0x200002, Status.BAD_GATEWAY, "SDK encountered a communication error"),
  UNAUTHORIZED(0x200003, Status.UNAUTHORIZED, "Not authorized"),
  FORBIDDEN(0x200004, Status.FORBIDDEN, "Permission denied"),
  SERVICE_UNAVAILABLE(0x200005, Status.SERVICE_UNAVAILABLE, "Server is currently unavailable"),
  GENERIC_SERVER_ERROR(
      0x200006, Status.INTERNAL_SERVER_ERROR, "An unexpected problem happened on server side"),
  TIMEOUT(0x200007, Status.GATEWAY_TIMEOUT, "Operation has timed out"),
  BAD_GATEWAY(
      0x200008, Status.BAD_GATEWAY, "Load balancer proxy failed due to upstream server crash"),
  SERVICE_UNDEPLOYED(0x200009, Status.NOT_FOUND, "Requested resource was not found on the server"),
  OPERATION_CANCELLED(0x20000a, Status.BAD_GATEWAY, "Operation has been cancelled by peer"),
  OPERATION_UNEXECUTABLE(0x20000b, Status.BAD_REQUEST, "Unable to execute the requested operation"),
  SESSION_EXPIRED(0x20000c, Status.UNAUTHORIZED, "Session has expired"),
  SERVER_IN_MAINTENANCE(0x200005, Status.SERVICE_UNAVAILABLE, "Server is in maintenance mode"),
  NO_SUCH_TENANT(
      0x210001,
      Status.NOT_FOUND,
      "SDK tried an operation that requires a tenant, but the specified tenant does not exist"),
  NO_SUCH_STREAM(
      0x210002,
      Status.NOT_FOUND,
      "SDK tried an operation that requires a stream, but the specified stream does not exist"),
  NOT_FOUND(0x210003, Status.NOT_FOUND, "Target entity does not exist in server"),
  TENANT_ALREADY_EXISTS(
      0x210004,
      Status.BAD_REQUEST,
      "SDK tried to add an tenant, but specified tenant already exists"),
  STREAM_ALREADY_EXISTS(
      0x210005,
      Status.BAD_REQUEST,
      "SDK tried to add a stream, but specified stream already exists"),
  RESOURCE_ALREADY_EXISTS(
      0x210006,
      Status.BAD_REQUEST,
      "SDK tried to add an entity to server, but the target already exists"),
  BAD_INPUT(0x210007, Status.BAD_REQUEST, "Server rejected input due to invalid data or format"),
  NOT_IMPLEMENTED(0x210008, Status.NOT_IMPLEMENTED, "Not implemented"),
  CONSTRAINT_WARNING(0x210009, Status.BAD_REQUEST, "Not implemented"),
  SCHEMA_VERSION_CHANGED(0x21000b, Status.BAD_REQUEST, "Given stream version no longer exists"),
  INVALID_REQUEST(0x21000c, Status.BAD_REQUEST, "Given stream version no longer exists"),
  BULK_INGEST_FAILED(0x21000d, Status.INTERNAL_SERVER_ERROR, "Bulk ingest failed"),
  SERVER_DATA_ERROR(0x21000e, Status.OK, "Server returned malformed data"),
  SCHEMA_MISMATCHED(0x21000f, Status.BAD_REQUEST, "Schema mismatched"),
  USER_ID_NOT_VERIFIED(0x210010, Status.UNAUTHORIZED, "User ID is not verified yet");

  private final int errorNumber;
  private final String errorCode;
  private final String message;
  private Status status;

  private BiosClientError(int errorNumber, Status status, String message) {
    this.errorNumber = errorNumber;
    this.errorCode = String.format("CLIENT-%06x", errorNumber);
    this.status = status;
    this.message = message;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  @Override
  public String getErrorCode() {
    return errorCode;
  }

  public int getErrorNumber() {
    return errorNumber;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public Status getStatus() {
    return this.status;
  }

  @Override
  public String getErrorMessage() {
    return message;
  }

  @Override
  public String toString() {
    return name()
        + ": {errorCode="
        + errorCode
        + ", status="
        + status.toString()
        + ", message="
        + message
        + "}";
  }
}
