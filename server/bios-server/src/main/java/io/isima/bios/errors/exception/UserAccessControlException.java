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

import io.isima.bios.errors.BiosError;
import io.isima.bios.models.UserContext;
import javax.ws.rs.core.Response.Status;

public class UserAccessControlException extends TfosException {
  private static final long serialVersionUID = -2420557974456671702L;
  private final UserContext userContext;

  public UserAccessControlException(BiosError info) {
    super(info);
    this.userContext = null;
    assert (info.getStatus() == Status.FORBIDDEN || info.getStatus() == Status.UNAUTHORIZED);
  }

  public UserAccessControlException(BiosError info, String message, UserContext userContext) {
    super(info, message);
    this.userContext = userContext;
    assert (info.getStatus() == Status.FORBIDDEN || info.getStatus() == Status.UNAUTHORIZED);
  }

  public UserAccessControlException(BiosError info, UserContext userContext) {
    super(info);
    this.userContext = userContext;
    assert (info.getStatus() == Status.FORBIDDEN || info.getStatus() == Status.UNAUTHORIZED);
  }
}
