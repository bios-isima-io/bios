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
import io.isima.bios.models.UserContext;

/**
 * Exception thrown to indicate that requested resource is out of the scope of the user's
 * permission.
 */
public class InvalidAccessRequestException extends UserAccessControlException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  public InvalidAccessRequestException() {
    super(AuthError.INVALID_ACCESS_REQUEST);
  }

  public InvalidAccessRequestException(UserContext userContext, String message) {
    super(AuthError.INVALID_ACCESS_REQUEST, message, userContext);
  }
}
