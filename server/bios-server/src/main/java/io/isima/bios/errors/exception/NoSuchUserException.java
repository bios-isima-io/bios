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

import io.isima.bios.errors.AuthError;

/**
 * Exception thrown during a user operation when target user is not found.
 *
 * <p>Note: Do not use this exception for authentication failure since it may give some idea of
 * existing names to a malicious user.
 */
public class NoSuchUserException extends TfosException {
  private static final long serialVersionUID = 9098831628972041999L;

  /**
   * The constructor.
   *
   * @param scope User scope
   * @param username User name
   */
  public NoSuchUserException(String scope, String username) {
    super(AuthError.NO_SUCH_USER, String.format("scope=%s username=%s", scope, username));
  }

  public NoSuchUserException(String message) {
    super(AuthError.NO_SUCH_USER, message);
  }
}
