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
package io.isima.bios.dto.bulk;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.UUID;
import javax.validation.constraints.NotNull;

/** DTO to carry a result of a ingest bulk entry when there are errors in a batch */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InsertResultGeneric implements InsertSuccessOrError {

  /** The event ID. */
  @NotNull private UUID eventId;

  /** Ingest timestamp on successful operation. */
  Long timestamp;

  /** Error code on unsuccessful operation. */
  Integer statusCode;

  /** Error message on unsuccessful operation. */
  String errorMessage;

  /** Server error code */
  String serverErrorCode;

  public InsertResultGeneric(UUID eventId, Long timestamp) {
    this.eventId = eventId;
    this.timestamp = timestamp;
    this.statusCode = 0;
  }

  public InsertResultGeneric(Integer statusCode, String errorMessage, String serverErrorCode) {
    this.statusCode = statusCode;
    this.errorMessage = errorMessage;
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

  @Override
  public UUID getEventId() {
    return eventId;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getServerErrorCode() {
    return serverErrorCode;
  }

  @Override
  public Integer getStatusCode() {
    return statusCode;
  }
}
