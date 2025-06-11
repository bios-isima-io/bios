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

public enum SignupError implements BiosError {
  GENERIC_SIGNUP_ERROR("SIGNUP00", Status.INTERNAL_SERVER_ERROR, "Generic Signup error"),
  INVALID_REQUEST("SIGNUP01", Status.BAD_REQUEST, "Invalid request"),
  INVALID_EMAIL_ADDRESS("SIGNUP02", Status.BAD_REQUEST, "Email address is invalid"),
  INVALID_TOKEN("SIGNUP03", Status.UNAUTHORIZED, "Invalid token"),
  LINK_EXPIRED("SIGNUP04", Status.BAD_REQUEST, "Link is expired"),
  USER_ALREADY_EXISTS("SIGNUP05", Status.CONFLICT, "User already exists"),
  ADMIN_PASSWORD_MISMATCH("SIGNUP06", Status.BAD_REQUEST, "Admin password mismatch"),
  TENANT_ALREADY_EXISTS("SIGNUP07", Status.CONFLICT, "Tenant already exists"),
  ON_PREM_DISABLED("SIGNUP08", Status.BAD_REQUEST, "On prem user managent is disabled"),
  INVALID_EMAIL_DOMAIN("SIGNUP09", Status.BAD_REQUEST, "Please use your work email"),
  JUPYTER_HUB_SIGNUP_FAILED("SIGNUP0a", Status.INTERNAL_SERVER_ERROR, "JupyterHub signup failed"),
  INVALID_DOMAIN("SIGNUP0b", Status.FORBIDDEN, "Tried to signup to wrong domain"),
  USER_CONFLICT("SIGNUP0c", Status.CONFLICT, "User conflict"),
  ;

  private final String errorCode;
  private final Status status;
  private final String message;

  private SignupError(String errorCode, Status status, String message) {
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
