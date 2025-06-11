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
package io.isima.bios.auth;

import io.isima.bios.dto.ChangePasswordRequest;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.AuthResponse;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** AuthV1 intermal module interface. */
public interface Auth {
  /**
   * Login user.
   *
   * @param credentials User credentials
   * @param clientVersion Client version
   * @return Authentication response.
   */
  CompletableFuture<AuthResponse> loginAsync(
      Credentials credentials, String clientVersion, ExecutionState state);

  /**
   * Renew the given authentication token and return the renewed token.
   *
   * @param authToken The authentication token. The value may be null, in which case the method
   *     throws UserAccessControlException.
   * @return Login information
   * @throws UserAccessControlException thrown to indicate that the specified token is invalid.
   */
  AuthResponse renewSession(SessionToken authToken) throws UserAccessControlException;

  /**
   * Get login information from the specified authentication token.
   *
   * @param authToken The authentication token. The value may be null, in which case the method
   *     throws UserAccessControlException.
   * @param detail Retrieve detail information from database if specified, otherwise the method
   *     returns only outline of the user info.
   * @return Login information
   * @throws UserAccessControlException thrown to indicate that the specified token is invalid.
   */
  AuthResponse getLoginInfo(SessionToken authToken, boolean detail)
      throws UserAccessControlException;

  /**
   * Reset password.
   *
   * @param userContext User context of the requester
   * @param changePasswordRequest The request
   * @throws TfosException thrown to indicate a user error happened
   * @throws ApplicationException thrown to indicate an unexpected error happened
   */
  CompletableFuture<Void> changePassword(
      UserContext userContext, ChangePasswordRequest changePasswordRequest, ExecutionState state);

  /**
   * Validate an authorization token.
   *
   * @param sessionToken Session token. The value may be null, in which case the method throws
   *     UserAccessControlException.
   * @param tenantName Target tenant name of the operation. System admin operation should specify
   *     "_system". Specify empty string ("") when the tenant name is unknown.
   * @param requiredRoles Required roles for the operation.
   * @return Validated user's context.
   * @throws UserAccessControlException thrown to indicate that the specified token is invalid.
   */
  UserContext validateSessionToken(
      SessionToken sessionToken, String tenantName, List<Permission> requiredRoles)
      throws UserAccessControlException;

  /**
   * Method to create a session token.
   *
   * @param currentTime Current time
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext instance
   * @return Session token
   */
  String createToken(long currentTime, long expirationTime, UserContext userContext);

  String createSignupToken(long currentTime, long expiry, Long orgId, String email);

  UserContext parseToken(SessionToken sessionToken) throws UserAccessControlException;

  UserContext parseSignupToken(String signUpToken) throws UserAccessControlException;

  /**
   * Method to hash the input.
   *
   * @param input Input data
   * @return Base64 encoded of hash value.
   */
  String hash(String input);
}
