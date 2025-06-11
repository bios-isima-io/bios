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
package io.isima.bios.auth.v1;

import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.LoginResponse;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Credentials;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.service.handler.ServiceHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface AuthV1 {
  public static final String SCOPE_DELIMITER = "/";

  public static final String SCOPE_PREFIX = "/tenants/";
  public static final String SCOPE_TENANT = "/tenants/%s/";

  // TODO(Naoki): The platform (root) scope should be deprecated; Use SCOPE_SYSTEM
  public static final String SCOPE_ROOT = "/";
  public static final String SCOPE_SYSTEM = "/tenants/_system/";

  /**
   * Method hash the input and then encodes the hash value.
   *
   * @param input Input data
   * @return Base64 encoded of hash value.
   */
  String hash(String input);

  /**
   * Method to login user with credentials.
   *
   * @param credentials Authentication credentials
   * @param expirationMillis Specifies expiration milliseconds. If 0 is specified, the system
   *     default is used.
   * @param clientVersion Client version
   * @return CompletableFuture of LoginResponse.
   */
  CompletableFuture<LoginResponse> login(
      Credentials credentials, long expirationMillis, String clientVersion, ExecutionState state);

  /**
   * Method to login user with credentials.
   *
   * @param domain Cookie domain, if to be set.
   * @param credentials Authentication credentials
   * @param expirationMillis Specifies expiration milliseconds. If 0 is specified, the system
   *     default is used.
   * @param clientVersion Client version
   * @return CompletableFuture of LoginResponse.
   */
  CompletableFuture<LoginResponse> login(
      String domain,
      Credentials credentials,
      long expirationMillis,
      String clientVersion,
      ExecutionState state);

  /**
   * Get user info by email.
   *
   * @param userContext User context
   * @return Login response as the user info
   */
  CompletableFuture<LoginResponse> getUserInfo(UserContext userContext, ExecutionState state);

  /**
   * Method parse the jwt token.
   *
   * @param sessionToken Session token
   * @return UserContext contains information about user.
   * @throws UserAccessControlException for invalid token
   */
  CompletableFuture<UserContext> parseToken(
      String sessionToken, boolean isCookie, ExecutionState state);

  default UserContext parseToken(SessionToken sessionToken) throws UserAccessControlException {
    final var token = sessionToken != null ? sessionToken.getToken() : null;
    final var isCookie = sessionToken != null && sessionToken.isCookie();
    try {
      return parseToken(
              token,
              isCookie,
              new GenericExecutionState("ParseToken", ServiceHandler.getExecutor()))
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof UserAccessControlException) {
        throw (UserAccessControlException) cause;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Method to create the auth token.
   *
   * @param currentTime Current time
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext instance
   * @return AuthV1 token
   */
  String createToken(long currentTime, long expirationTime, UserContext userContext);

  /**
   * Method to renew session token.
   *
   * @param sessionToken Java Web Token
   * @param expirationMillis Specifies expiration milliseconds. If 0 is specified, the system
   *     default is used.
   * @return Login response with new token
   * @throws AuthenticationException for invalid token
   */
  Object renewSessionToken(String sessionToken, long expirationMillis)
      throws UserAccessControlException;

  /**
   * Method to validate auth token for given scope and roles.
   *
   * @param sessionToken Session token using Java Web Token
   * @param isCookie Whether the session token is sent over cookie
   * @param scope Requested scope. Scope check is skipped when this parameter is blank
   * @param permission Required permission
   * @return UserContexts
   * @throws UserAccessControlException when the access is denied @IllegalArgumentException if any
   *     of the parameters is null
   */
  UserContext validateToken(
      String sessionToken, boolean isCookie, String scope, Permission permission)
      throws UserAccessControlException;

  /**
   * Method to validate auth token for given scope and roles.
   *
   * @param sessionToken AuthV1 token using Java Web Token
   * @param isCookie Whether the session token is sent over cookie
   * @param scope Requested scope. Scope check is skipped when this parameter is blank
   * @param permissions Required permissions
   * @return UserContext
   * @throws UserAccessControlException when the access is denied @IllegalArgumentException if any
   *     of the parameters is null
   */
  UserContext validateToken(
      String sessionToken, boolean isCookie, String scope, List<Permission> permissions)
      throws UserAccessControlException;

  /**
   * Method to validate auth token for given tenant and roles.
   *
   * <p>This method is meant to be used by BIOS API for receive a permission to access the specified
   * scope with the specified roles.
   *
   * <p>There are pre-existing similar APIs {@link #validateToken}. This version of the method is
   * stricter than that existing ones. The user's scope has to match exactly with the requested
   * scope to get permission, whereas the previous version allows access to subsequent scopes (such
   * as '/' user is allowed to access '/tenants/mytenant/').
   *
   * <p>The user gets permission to access to the requested scope when all of the following
   * conditions meet:
   *
   * <ul>
   *   <li>The specified allowed roles include at least one of the user's roles.
   *   <li>The user's scope matches the requested scope exactly.
   * </ul>
   *
   * @param sessionToken Session token using Java Web Token
   * @param requestedScope Requested scope/resource. Scope check is skipped when this parameter is
   *     blank
   * @param allowedRoles Roles that are allowed to access to the resource for the requester's
   *     purpose.
   * @return UserContext
   * @throws UserAccessControlException when the access is denied @IllegalArgumentException if any
   *     of the parameters is null
   */
  UserContext validateTokenStrictly(
      SessionToken sessionToken, String requestedScope, List<Permission> allowedRoles)
      throws UserAccessControlException;

  /**
   * Method to get auth token from auth header.
   *
   * @param authHeader Authentication header
   * @return Authentication token.
   */
  SessionToken getAuthToken(String authHeader);

  /**
   * Method parse sign-up token.
   *
   * @param signupToken User signup token
   * @return UserContext contains information about user.
   * @throws TfosException for invalid token
   */
  UserContext parseSignupToken(String signupToken) throws UserAccessControlException;

  /**
   * Method create user sign-up token.
   *
   * @param currentTime Current time in milliseconds
   * @param expirationTime Expiration time in milliseconds
   * @param orgId Organization Id, if available
   * @param email Email of User
   * @return Token to complete sign-up process.
   */
  String createSignupToken(long currentTime, long expirationTime, Long orgId, String email);
}
