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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.sdk.BiosClientConstants;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.errors.BiosClientErrorHandler;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.IOException;
import okhttp3.HttpUrl;
import okhttp3.Response;

public class OkHttp3ErrorHandler {
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Method to check a web response and throws TFOS client exception on error.
   *
   * @param response Web response
   * @throws BiosClientException When response has failure state
   */
  public static void checkError(Response response) throws BiosClientException {
    if (response == null) {
      throw new BiosClientException(
          BiosClientError.GENERIC_SERVER_ERROR, "Server did not send any response");
    }
    if (response.isSuccessful()) {
      // no error. do nothing
      return;
    }

    String endpoint = response.header(BiosClientConstants.X_UPSTREAM);
    if (endpoint == null) {
      final HttpUrl url = response.request().url();
      endpoint = String.format("%s:%d", url.host(), url.port());
    }

    try {
      final String responseBody = response.body().string();
      String message = null;
      String serverErrorCode = null;
      final String contentType = response.header(BiosClientConstants.CONTENT_TYPE);
      if (contentType != null
          && contentType.contains(BiosClientConstants.JSON_CONTENT)
          && responseBody != null
          && !responseBody.isEmpty()) {
        JsonNode node = mapper.readTree(responseBody);
        message = node.get(BiosClientConstants.MESSAGE).asText();
        serverErrorCode = node.get(BiosClientConstants.ERROR_CODE).asText();
      } else {
        message = response + "; responseBody=" + responseBody;
      }
      final BiosClientError errorCode =
          BiosClientErrorHandler.determineError(response.code(), serverErrorCode);
      throw new BiosClientException(errorCode, message, endpoint);
    } catch (IOException e) {
      throw new BiosClientException(BiosClientError.GENERIC_SERVER_ERROR, endpoint, e);
    } finally {
      response.body().close();
    }
  }
}
