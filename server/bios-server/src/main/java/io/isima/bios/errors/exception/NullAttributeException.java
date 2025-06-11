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

/** Exception used when an event has a null value and the process cannot handle the data. */
public class NullAttributeException extends RuntimeException {
  private static final long serialVersionUID = -7387571353123704858L;

  public NullAttributeException() {
    super();
  }

  public NullAttributeException(String message) {
    super(message);
  }
}
