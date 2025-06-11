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
package io.isima.bios.sdk.exceptions;

import io.isima.bios.sdk.errors.BiosClientError;
import java.io.Serializable;

public class BiosClientException extends Exception implements Serializable {

  private static final long serialVersionUID = 6470001770754945976L;

  private final BiosClientError code;

  private final String endpoint;

  /**
   * Constructor with error message.
   *
   * <p>{@link BiosClientError#GENERIC_CLIENT_ERROR} is set as error code by this constructor.
   * Default error message for this code is overwritten by the specified message.
   *
   * @param message Error message.
   */
  public BiosClientException(String message) {
    super(message);
    this.code = BiosClientError.GENERIC_CLIENT_ERROR;
    this.endpoint = null;
  }

  /**
   * Constructor with error code and message.
   *
   * <p>Default error message for the code is overwritten by the specified message.
   *
   * @param code Error code
   * @param message Error message that overwrites the default message for the specified code
   */
  public BiosClientException(final BiosClientError code, final String message) {
    super(message);
    this.code = code;
    this.endpoint = null;
  }

  /**
   * Constructor with error code, message, and endpoint info.
   *
   * @param code Error code
   * @param message Error message
   * @param endpoint Remote server endpoint
   */
  public BiosClientException(BiosClientError code, String message, String endpoint) {
    super(message);
    this.code = code;
    this.endpoint = endpoint;
  }

  /**
   * Constructor with error code, message, endpoint info, and cause.
   *
   * @param code Error code
   * @param message Error message
   * @param endpoint Remote server endpoint
   * @param cause The exception that causes this error
   */
  public BiosClientException(
      BiosClientError code, String message, String endpoint, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.endpoint = endpoint;
  }

  /**
   * Constructor with cause.
   *
   * <p>This constructor is meant be used to report any unexpected exception during client code
   * execution. {@link BiosClientError#GENERIC_CLIENT_ERROR} is set as the error code.
   *
   * @param message Error message
   * @param cause The throwable that caused this exception.
   */
  public BiosClientException(final String message, final Throwable cause) {
    super(message, cause);
    this.code = BiosClientError.GENERIC_CLIENT_ERROR;
    this.endpoint = null;
  }

  public BiosClientException(Throwable cause) {
    super(cause);
    this.code = BiosClientError.GENERIC_CLIENT_ERROR;
    this.endpoint = null;
  }

  public BiosClientException(BiosClientError code, String endpoint, Throwable cause) {
    super(cause);
    this.code = code;
    this.endpoint = endpoint;
  }

  /**
   * Method to return the error code.
   *
   * @return error code
   */
  public BiosClientError getCode() {
    return code;
  }

  /**
   * Returns server endpoint if any.
   *
   * @return Server endpoint. If the endpoint is not available, null is returned.
   */
  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder(getClass().getName())
            .append(": ")
            .append(code.name())
            .append("; errorCode=")
            .append(code.getErrorCode());
    if (endpoint != null) {
      sb.append(", endpoint=").append(endpoint);
    }
    if (getMessage() != null) {
      sb.append(", message='").append(getMessage()).append("'");
    }
    if (getCause() != null) {
      sb.append(", caused by: ").append(getCause().toString());
    }
    return sb.toString();
  }
}
