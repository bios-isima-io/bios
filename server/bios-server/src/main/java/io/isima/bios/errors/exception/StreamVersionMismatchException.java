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
package io.isima.bios.errors.exception;

import io.isima.bios.errors.AdminError;

/** Exception thrown when a data plane operation is called with an unavailable version. */
public class StreamVersionMismatchException extends StreamException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  public StreamVersionMismatchException() {
    super(AdminError.STREAM_VERSION_MISMATCH);
  }

  public StreamVersionMismatchException(String message) {
    super(AdminError.STREAM_VERSION_MISMATCH, message);
  }

  public StreamVersionMismatchException(String tenant, String stream, String message) {
    super(AdminError.STREAM_VERSION_MISMATCH, tenant + "." + stream + ": " + message);
  }
}
