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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.isima.bios.dto.bulk.InsertSuccessOrError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import java.util.UUID;
import javax.ws.rs.core.Response.Status;

/**
 * Bulk insert result holder.
 *
 * <p>Instances are created before executing inserts in the InsertBulkState object. Each insert
 * stages fills the success or error values on completion, so that the parent stage can collect the
 * results at the end.
 *
 * <p>The result is either an InsertResponseRecord instance or a Throwable. The result instance has
 * only one generic pointer to keep both types in order to save memory space.
 */
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({"eventId", "timestamp", "serverErrorCode", "statusCode", "errorMessage"})
public class InsertBulkEachResult implements InsertSuccessOrError {

  @JsonIgnore private Object result;

  public InsertBulkEachResult() {
    result = null;
  }

  @JsonIgnore
  public void setSuccess(InsertResponseRecord record) {
    result = record;
  }

  @JsonIgnore
  public void setError(Throwable t) {
    result = t;
  }

  @JsonIgnore
  public InsertResponseRecord getRecord() {
    return (result instanceof InsertResponseRecord) ? (InsertResponseRecord) result : null;
  }

  @JsonIgnore
  public Throwable getError() {
    return (result instanceof Throwable) ? (Throwable) result : null;
  }

  @JsonIgnore
  public boolean isError() {
    return result instanceof Throwable;
  }

  @JsonIgnore
  public boolean isTfosError() {
    return result instanceof TfosException;
  }

  @Override
  public String toString() {
    return result.toString();
  }

  // InsertSuccessOrError implementation, they must be JSON enabled ///////////////////////////
  @Override
  @JsonProperty
  public UUID getEventId() {
    if (isError()) {
      return null;
    }
    return ((InsertResponseRecord) result).getEventId();
  }

  @Override
  @JsonProperty
  public Long getTimestamp() {
    if (isError()) {
      return null;
    }
    return ((InsertResponseRecord) result).getTimeStamp();
  }

  @Override
  @JsonProperty
  public String getErrorMessage() {
    if (result instanceof Throwable) {
      return ((Throwable) result).getMessage();
    }
    return null;
  }

  @Override
  @JsonProperty
  public String getServerErrorCode() {
    if (result instanceof TfosException) {
      return ((TfosException) result).getErrorCode();
    } else if (result instanceof Throwable) {
      return GenericError.APPLICATION_ERROR.getErrorCode();
    }
    return null;
  }

  @Override
  @JsonProperty
  public Integer getStatusCode() {
    if (result instanceof TfosException) {
      return ((TfosException) result).getStatus().getStatusCode();
    } else if (result instanceof Throwable) {
      return Status.INTERNAL_SERVER_ERROR.getStatusCode();
    }
    return null;
  }
}
