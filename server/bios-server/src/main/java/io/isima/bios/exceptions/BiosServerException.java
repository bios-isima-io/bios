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

import io.netty.handler.codec.http.HttpResponseStatus;

public class BiosServerException extends Exception {
  private static final long serialVersionUID = 6632491821492563107L;

  private final HttpResponseStatus status;

  public BiosServerException(HttpResponseStatus status) {
    this.status = status;
  }

  public BiosServerException(HttpResponseStatus status, String message) {
    super(message);
    this.status = status;
  }

  public BiosServerException(HttpResponseStatus status, String message, Throwable cause) {
    super(message, cause);
    this.status = status;
  }

  public HttpResponseStatus getStatus() {
    return status;
  }
}
