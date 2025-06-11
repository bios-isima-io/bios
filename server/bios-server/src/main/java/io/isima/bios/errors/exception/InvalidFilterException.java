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

import io.isima.bios.errors.EventExtractError;

public class InvalidFilterException extends TfosException {
  private static final long serialVersionUID = 6247745278396458143L;

  public InvalidFilterException() {
    super(EventExtractError.INVALID_FILTER);
  }

  public InvalidFilterException(String additionalMessage) {
    super(EventExtractError.INVALID_FILTER, additionalMessage);
  }

  public InvalidFilterException(String format, Object... params) {
    super(EventExtractError.INVALID_FILTER, String.format(format, params));
  }

  public InvalidFilterException(Throwable t) {
    super(EventExtractError.INVALID_FILTER, t);
  }
}
