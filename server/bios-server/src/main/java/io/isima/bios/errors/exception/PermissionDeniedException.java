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

/** Exception thrown when a user does not have permission to do a certain operation. */
public class PermissionDeniedException extends UserAccessControlException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  public PermissionDeniedException() {
    super(AuthError.PERMISSION_DENIED);
  }

  public PermissionDeniedException(UserContext userContext) {
    super(AuthError.PERMISSION_DENIED, userContext);
  }

  public PermissionDeniedException(UserContext userContext, String message) {
    super(AuthError.PERMISSION_DENIED, message, userContext);
  }
}
