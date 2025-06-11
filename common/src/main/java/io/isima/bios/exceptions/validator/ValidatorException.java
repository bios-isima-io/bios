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
package io.isima.bios.exceptions.validator;

/** Exception thrown to indicate that validation failed. */
public class ValidatorException extends Exception {
  private static final long serialVersionUID = 5150061474397300265L;

  private final ValidationError errorType;

  public ValidatorException(String message, ValidationError errorType) {
    super(message);
    this.errorType = errorType;
  }

  public ValidationError getErrorType() {
    return errorType;
  }
}
