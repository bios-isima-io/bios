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

import io.isima.bios.dto.IngestResponse;
import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.models.Record;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.exceptions.IngestBulkFailedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BiosClientErrorHandler {

  // map of status code and TFOS error code
  static final Map<Integer, BiosClientError> codeConversionTable;

  static {
    codeConversionTable = new HashMap<>();
    codeConversionTable.put(400, BiosClientError.BAD_INPUT);
    codeConversionTable.put(401, BiosClientError.UNAUTHORIZED);
    codeConversionTable.put(403, BiosClientError.FORBIDDEN);
    codeConversionTable.put(404, BiosClientError.NOT_FOUND);
    codeConversionTable.put(408, BiosClientError.TIMEOUT);
    codeConversionTable.put(409, BiosClientError.RESOURCE_ALREADY_EXISTS);
    codeConversionTable.put(413, BiosClientError.REQUEST_TOO_LARGE);
    codeConversionTable.put(501, BiosClientError.NOT_IMPLEMENTED);
    codeConversionTable.put(502, BiosClientError.BAD_GATEWAY);
    codeConversionTable.put(503, BiosClientError.SERVICE_UNAVAILABLE);
    codeConversionTable.put(504, BiosClientError.TIMEOUT);
  }

  /**
   * Determines client error type by status code and server error code.
   *
   * @param statusCode The operation status code
   * @param serverErrorCode Error code returned by the server
   * @return Resolved client error
   */
  public static BiosClientError determineError(int statusCode, String serverErrorCode) {
    switch (statusCode) {
      case 400:
        return BiosClientError.BAD_INPUT;
      case 401:
        return BiosClientError.UNAUTHORIZED;
      case 403:
        return BiosClientError.FORBIDDEN;
      case 404:
        if ("ADMIN03".equalsIgnoreCase(serverErrorCode)) {
          return BiosClientError.NO_SUCH_TENANT;
        } else if ("ADMIN04".equalsIgnoreCase(serverErrorCode)) {
          return BiosClientError.NO_SUCH_STREAM;
        } else if (serverErrorCode == null) {
          return BiosClientError.SERVICE_UNDEPLOYED;
        } else {
          return BiosClientError.NOT_FOUND;
        }
      case 409:
        {
          if ("ADMIN08".equalsIgnoreCase(serverErrorCode)) {
            return BiosClientError.TENANT_ALREADY_EXISTS;
          } else if ("ADMIN09".equalsIgnoreCase(serverErrorCode)) {
            return BiosClientError.STREAM_ALREADY_EXISTS;
          } else {
            return BiosClientError.RESOURCE_ALREADY_EXISTS;
          }
        }
      case 503:
        if ("GENERIC10".equalsIgnoreCase(serverErrorCode)) {
          return BiosClientError.SERVER_IN_MAINTENANCE;
        }
        return BiosClientError.SERVICE_UNAVAILABLE;
      default:
        {
          BiosClientError error = codeConversionTable.get(statusCode);
          if (error != null) {
            return error;
          }
          return (statusCode / 100 == 4)
              ? BiosClientError.GENERIC_CLIENT_ERROR
              : BiosClientError.GENERIC_SERVER_ERROR;
        }
    }
  }

  /**
   * Determines whether an error is a temporary server failure.
   *
   * @param error TFOS client error code.
   * @return True if the error is a temporary server failure, otherwise false.
   */
  public static boolean isTemporaryServerFailure(BiosClientError error) {
    switch (error) {
      case SERVER_CONNECTION_FAILURE:
      case SERVER_CHANNEL_ERROR:
      case TIMEOUT:
      case GENERIC_SERVER_ERROR:
      case BAD_GATEWAY:
      case SERVICE_UNAVAILABLE:
      case SERVICE_UNDEPLOYED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Utility to generate a IngestBulkFailedException instance.
   *
   * @param error Error code
   * @param reason Reason string
   * @param responses Accumulated responses.
   * @param errors Accumulated errors.
   * @return Generated exception.
   */
  public static IngestBulkFailedException generateIngestBulkFailedException(
      BiosClientError error,
      String reason,
      Map<Integer, IngestResponse> responses,
      Map<Integer, BiosClientException> errors) {
    return new IngestBulkFailedException(
        error, bulkFailureMessage(responses.keySet(), reason), responses, errors);
  }

  /**
   * Utility to generate a InsertBulkFailedException instance.
   *
   * <p>Note: this is different from IngestBulkFailed exception to hide the name 'ingest" from the
   * new bios interface as well as to hide IngestResponse which is an implementation class that
   * might get removed when old tfos models are removed.
   *
   * @param error Error code
   * @param reason Reason string
   * @param responses Accumulated responses.
   * @param errors Accumulated errors.
   * @return Generated exception.
   */
  public static InsertBulkFailedException generateInsertBulkFailedException(
      BiosClientError error,
      String reason,
      Map<Integer, Record> responses,
      Map<Integer, BiosClientException> errors) {
    return new InsertBulkFailedException(
        error, bulkFailureMessage(responses.keySet(), reason), responses, errors);
  }

  private static String bulkFailureMessage(Set<Integer> errorSet, String reason) {
    StringBuilder message = new StringBuilder();
    if (reason != null) {
      message.append("reason=").append(reason).append(", ");
    }
    message.append("failed entries=");
    final int limit = 10;
    int count = 0;
    String delimiter = "";
    for (Integer index : errorSet) {
      if (count++ > limit) {
        break;
      }
      message.append(delimiter).append(index);
      delimiter = ", ";
    }
    if (errorSet.size() > limit) {
      message.append(", ...");
    }
    return message.toString();
  }
}
