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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.isima.bios.errors.BiosError;
import io.isima.bios.models.ErrorResponsePayload;
import java.util.List;
import javax.ws.rs.core.Response;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InsertBulkErrorResponse extends ErrorResponsePayload {
  private List<? extends InsertSuccessOrError> resultsWithError = null;

  public InsertBulkErrorResponse() {}

  public InsertBulkErrorResponse(
      Response.Status status, String message, List<? extends InsertSuccessOrError> results) {
    super(status, message);
    this.resultsWithError = results;
  }

  public InsertBulkErrorResponse(
      BiosError base, String message, List<? extends InsertSuccessOrError> results) {
    super(base.getErrorCode(), base.getStatus(), message);
    this.resultsWithError = results;
  }

  public String getServerErrorCode() {
    return super.getErrorCode();
  }

  public String getServerErrorMessage() {
    return super.getMessage();
  }

  public List<? extends InsertSuccessOrError> getResultsWithError() {
    return resultsWithError;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (getServerErrorCode() != null) {
      sb.append("overall=").append(getServerErrorCode());
    }
    if (getServerErrorMessage() != null) {
      sb.append("errorMessage=").append(getServerErrorMessage());
    }
    if (resultsWithError != null) {
      sb.append(", details:");
      for (var result : resultsWithError) {
        if (result.getEventId() == null) {
          sb.append("\n  ").append(result.toString());
        }
      }
    }
    return sb.toString();
  }
}
