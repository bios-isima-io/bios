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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.UUID;
import javax.validation.constraints.NotNull;

/** DTO to carry a result of a ingest bulk entry when there are errors in a batch */
@JsonInclude(Include.NON_NULL)
public class IngestResultJson implements IngestResultOrError {

  /** The event ID. */
  @NotNull private UUID eventId;

  /** Ingest timestamp on successful operation. */
  Long timestamp;

  /** Error code on unsuccessful operation. */
  Integer statusCode;

  /** Error message on unsuccessful operation. */
  String errorMessage;

  /** Server error code. */
  String serverErrorCode;

  public UUID getEventId() {
    return eventId;
  }

  public IngestResultOrError setEventId(UUID eventId) {
    this.eventId = eventId;
    return this;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public Integer getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(Integer statusCode) {
    this.statusCode = statusCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getServerErrorCode() {
    return serverErrorCode;
  }

  public void setServerErrorCode(String serverErrorCode) {
    this.serverErrorCode = serverErrorCode;
  }

  @Override
  public String toString() {
    return "{"
        + (eventId != null ? "eventId=" + eventId + ", " : "")
        + (timestamp != null ? "timestamp=" + timestamp + ", " : "")
        + (statusCode != null ? "errorCode=" + statusCode + ", " : "")
        + (errorMessage != null ? "errorMessage=" + errorMessage : "")
        + "}";
  }
}
