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

import io.isima.bios.errors.GenericError;

/**
 * Exception thrown to indicate that the server is unable to fulfill the request due to a server
 * misconfiguration.
 */
public class InvalidConfigurationException extends TfosException {

  private static final long serialVersionUID = 7255356423560979020L;

  public InvalidConfigurationException() {
    super(GenericError.INVALID_CONFIGURATION);
  }

  public InvalidConfigurationException(String message) {
    super(GenericError.INVALID_CONFIGURATION, message);
  }

  public InvalidConfigurationException(Throwable t) {
    super(GenericError.INVALID_CONFIGURATION, t.getMessage(), t);
  }

  public InvalidConfigurationException(String message, Throwable t) {
    super(GenericError.INVALID_CONFIGURATION, message, t);
  }
}
