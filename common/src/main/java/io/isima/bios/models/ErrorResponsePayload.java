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
package io.isima.bios.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import javax.ws.rs.core.Response;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class ErrorResponsePayload {
  private String errorCode;
  private Response.Status status;
  private String message;

  public ErrorResponsePayload() {}

  public ErrorResponsePayload(Response.Status status, String message) {
    this.errorCode = "0x000000";
    this.status = status;
    this.message = message;
  }

  public ErrorResponsePayload(String errorCode, Response.Status status, String message) {
    this.errorCode = errorCode;
    this.status = status;
    this.message = message;
  }

  /**
   * Method to get errorCode.
   *
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * Method to set errorCode.
   *
   * @param errorCode the errorCode to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * Method to get status.
   *
   * @return the status
   */
  public Response.Status getStatus() {
    return status;
  }

  /**
   * Method to set status.
   *
   * @param status the status to set
   */
  public void setStatus(Response.Status status) {
    this.status = status;
  }

  /**
   * Method to get message.
   *
   * @return the message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Method to set message.
   *
   * @param message the message to set
   */
  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("errorCode=")
            .append(errorCode)
            .append("status=")
            .append(status)
            .append("(")
            .append(status.getStatusCode())
            .append("), message=")
            .append(message);
    return sb.toString();
  }
}
