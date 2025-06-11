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
package io.isima.bios.exceptions;

import io.isima.bios.dto.bulk.InsertBulkErrorResponse;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Server side only exception to carry Insert bulk error data.
 *
 * <p>This exception is required to use CompletableFuture on server side to communicate an ingest
 * bulk error back to the data resource layer.
 */
public class InsertBulkFailedServerException extends TfosException {
  private final InsertBulkErrorResponse errorResponse;

  private final boolean partialFailure;

  public InsertBulkFailedServerException(
      BiosError overall,
      String additionalMessage,
      InsertBulkErrorResponse response,
      boolean isPartialFailure) {
    super(overall);
    this.errorResponse = response;
    this.partialFailure = isPartialFailure;
  }

  public Response toResponse() {
    if (errorResponse instanceof InsertBulkErrorResponse) {
      // currently error messages goes out as JSON even for protobuf calls
      return Response.status(info.getStatus())
          .entity(errorResponse)
          .type(MediaType.APPLICATION_JSON)
          .build();
    } else {
      // for now this is unexpected
      return Response.status(GenericError.APPLICATION_ERROR.getStatus())
          .type(MediaType.APPLICATION_JSON)
          .build();
    }
  }

  public InsertBulkErrorResponse getErrorResponse() {
    return errorResponse;
  }

  @Override
  public String toString() {
    if (errorResponse != null) {
      return errorResponse.toString();
    }
    return "";
  }

  public boolean isPartialFailure() {
    return partialFailure;
  }
}
