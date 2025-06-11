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
package io.isima.bios.sdk.csdk;

/**
 * Exception thrown by C-SDK native methods.
 *
 * <p>This exception is meant to be converted to TfosClientException.
 */
public class CSdkException extends Exception {
  private static final long serialVersionUID = -7695688128560988643L;

  private final int statusCode;

  /**
   * The constructor of the exception.
   *
   * @param statusCode C-SDK OpStatus as integer
   * @param message Error message
   */
  CSdkException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
