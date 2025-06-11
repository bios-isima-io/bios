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

public enum AuthError implements BiosError {
  GENERIC_AUTH_ERROR("AUTH00", Status.INTERNAL_SERVER_ERROR, "Generic Authentication error"),
  USERNAME_NOT_FOUND("AUTH01", Status.UNAUTHORIZED, "Username not found"),
  INVALID_PASSWORD("AUTH02", Status.UNAUTHORIZED, "Invalid password"),
  PERMISSION_DENIED("AUTH03", Status.FORBIDDEN, "Permission denied"),
  INSUFFICIENT_CREDENTIALS(
      "AUTH04", Status.UNAUTHORIZED, "Credentials does not have sufficient parameters"),
  NO_SUCH_USER("AUTH05", Status.UNAUTHORIZED, "User entry not found"),
  DISABLED_USER("AUTH06", Status.UNAUTHORIZED, "User is disabled"),
  INVALID_TOKEN("AUTH07", Status.UNAUTHORIZED, "Invalid token"),
  MISSING_TOKEN("AUTH08", Status.UNAUTHORIZED, "Missing token"),
  INVALID_SCOPE("AUTH09", Status.UNAUTHORIZED, "Invalid scope"),
  SESSION_EXPIRED("AUTH0a", Status.UNAUTHORIZED, "Session expired"),
  INVALID_ACCESS_REQUEST("AUTH10", Status.FORBIDDEN, "Invalid access request"),
  USER_ID_NOT_VERIFIED("AUTH11", Status.UNAUTHORIZED, "User id is not verified yet");

  private final String errorCode;
  private final Status status;
  private final String message;

  private AuthError(String errorCode, Status status, String message) {
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
