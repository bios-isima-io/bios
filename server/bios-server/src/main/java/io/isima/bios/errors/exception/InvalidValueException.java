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

public class InvalidValueException extends TfosException {

  private static final long serialVersionUID = -3455489140842338407L;

  public InvalidValueException() {
    super(AdminError.INVALID_VALUE);
  }

  public InvalidValueException(String additionalMessage) {
    super(AdminError.INVALID_VALUE, additionalMessage);
  }

  public InvalidValueException(Throwable t) {
    super(AdminError.INVALID_VALUE, t.getMessage(), t);
  }

  public InvalidValueException(String message, Throwable t) {
    super(AdminError.INVALID_VALUE, message, t);
  }
}
