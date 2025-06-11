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

/** This exception class is used when user authentication fails. */
public class AuthenticationException extends UserAccessControlException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  private final String user;
  private final String scope;
  private final String appName;

  /**
   * Constructor of the class.
   *
   * @param info Error information that indicates the reason of authentication failure.
   * @param user User name
   * @param scope User scope
   * @param appName Application name
   */
  public AuthenticationException(BiosError info, String user, String scope, String appName) {
    super(info);
    this.user = user;
    this.scope = scope;
    this.appName = appName;
  }

  /**
   * Another constructor of the class.
   *
   * @param info Error information that indicates the reason of authentication failure.
   */
  public AuthenticationException(BiosError info) {
    super(info);
    this.user = null;
    this.scope = null;
    this.appName = null;
  }

  /**
   * Another constructor of the class.
   *
   * @param info Error information that indicates the reason of authentication failure.
   * @param userContext User Context
   */
  public AuthenticationException(BiosError info, UserContext userContext) {
    this(info, null, userContext);
  }

  public AuthenticationException(BiosError info, String message, UserContext userContext) {
    super(info, message, userContext);
    this.user = userContext.getSubject();
    this.scope = userContext.getScope();
    this.appName = userContext.getAppName();
  }

  /** This class does not give detail information of error for security reason. */
  @Override
  public String getErrorMessage() {
    return "Authentication failed";
  }

  public String getUser() {
    return user;
  }

  public String getScope() {
    return scope;
  }

  public String getAppName() {
    return appName;
  }

  @Override
  public String toString() {
    return String.format(
        "%s: %s user=%s scope=%s appName=%s",
        getErrorMessage(), info.getErrorMessage(), user, scope, appName);
  }
}
