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

/**
 * This exception is only for use by admin service handler.
 *
 * @author aj
 */
public class StreamAlreadyExistsException extends StreamException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  public StreamAlreadyExistsException() {
    super(AdminError.STREAM_ALREADY_EXISTS);
  }

  public StreamAlreadyExistsException(String message) {
    super(AdminError.STREAM_ALREADY_EXISTS, message);
  }

  public StreamAlreadyExistsException(String tenant, String stream) {
    super(AdminError.STREAM_ALREADY_EXISTS, tenant + "." + stream);
  }

  public StreamAlreadyExistsException(String tenant, String stream, String additionalMessage) {
    super(AdminError.STREAM_ALREADY_EXISTS, tenant + "." + stream + ": " + additionalMessage);
  }
}
