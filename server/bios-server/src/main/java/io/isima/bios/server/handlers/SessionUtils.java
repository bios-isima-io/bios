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
package io.isima.bios.server.handlers;

import io.isima.bios.errors.exception.InvalidAccessRequestException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.SessionInfo;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.service.JwtTokenUtils;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Class to manage sessions. */
public class SessionUtils {

  public SessionUtils() {}

  /**
   * Get login information from the specified authentication token.
   *
   * @param authToken The authentication token. The value may be null, in which case the method
   *     throws UserAccessControlException.
   * @return Login information
   * @throws UserAccessControlException thrown to indicate that the specified token is invalid.
   */
  @Deprecated
  public SessionInfo getLoginInfo(String authToken) throws UserAccessControlException {
    return null;
  }

  /**
   * Validate an authorization token.
   *
   * @param sessionToken Session token. The value may be null, in which case the method throws
   *     UserAccessControlException.
   * @param tenantName Target tenant name of the operation. System admin operation should specify
   *     "_system". Specify empty string ("") when the tenant name is unknown.
   * @param allowedRoles Roles that are allowed to execute the operation.
   * @return Validated user's context.
   * @throws UserAccessControlException thrown to indicate that the specified token is invalid.
   */
  public void validateSessionToken(
      SessionToken sessionToken,
      String tenantName,
      List<Permission> allowedRoles,
      ExecutionState state,
      Consumer<UserContext> acceptor,
      Consumer<Throwable> errorHandler) {
    Objects.requireNonNull(tenantName, "'tenantName' must not be null");

    JwtTokenUtils.parseToken(
        sessionToken.getToken(),
        sessionToken.isCookie(),
        state,
        (userContext) ->
            ExecutionHelper.run(
                () -> {
                  state.setUserContext(userContext);
                  // Check if the user has any allowed role
                  if (!allowedRoles.isEmpty()
                      && !allowedRoles.stream().anyMatch(userContext.getPermissions()::contains)) {
                    throw new PermissionDeniedException(
                        userContext,
                        String.format(
                            "Operation not permitted for user; email=%s, roles=%s",
                            userContext.getSubject(),
                            userContext.getPermissions().stream()
                                .map((perm) -> perm.toBios())
                                .collect(Collectors.toList())));
                  }

                  // Check if the user scope covers the specified tenant
                  if (!tenantName.isBlank()) {
                    final var scope = String.format("/tenants/%s/", tenantName);
                    if (!scope.equalsIgnoreCase(userContext.getScope())) {
                      throw new InvalidAccessRequestException(
                          userContext, buildForbiddenMessage(scope));
                    }
                  }

                  acceptor.accept(userContext);
                }),
        errorHandler::accept);
  }

  public CompletableFuture<UserContext> validateSessionTokenAllowDelegate(
      SessionToken sessionToken,
      String tenantName,
      List<Permission> allowedRoles,
      ExecutionState state) {
    Objects.requireNonNull(tenantName, "'tenantName' must not be null");

    final var future = new CompletableFuture<UserContext>();

    JwtTokenUtils.parseToken(
        sessionToken.getToken(),
        sessionToken.isCookie(),
        state,
        (userContext) ->
            ExecutionHelper.run(
                () -> {
                  state.setUserContext(userContext);

                  // Check if the user has any allowed role
                  if (!allowedRoles.isEmpty()
                      && !allowedRoles.stream().anyMatch(userContext.getPermissions()::contains)) {
                    throw new PermissionDeniedException(
                        userContext,
                        String.format(
                            "Operation not permitted for user; email=%s, roles=%s",
                            userContext.getSubject(),
                            userContext.getPermissions().stream()
                                .map((perm) -> perm.toBios())
                                .collect(Collectors.toList())));
                  }

                  if (userContext.getPermissions().contains(Permission.APP_MASTER)
                      && (!userContext.getTenant().equalsIgnoreCase(tenantName)
                          || state.isDelegate())) {
                    userContext.setIsAppMaster(true);
                    future.complete(userContext);
                    return;
                  }

                  // Check if the user scope covers the specified tenant
                  if (!tenantName.isBlank()) {
                    final var scope = String.format("/tenants/%s/", tenantName);
                    if (!scope.equalsIgnoreCase(userContext.getScope())) {
                      throw new InvalidAccessRequestException(
                          userContext, buildForbiddenMessage(scope));
                    }
                  }

                  future.complete(userContext);
                }),
        future::completeExceptionally);
    return future;
  }

  /**
   * Utility to build an out of scope message.
   *
   * @param scope Requested scope
   * @return Out of scope message
   */
  protected String buildForbiddenMessage(String scope) {
    final String tenantPrefix = "/tenants/";
    final String resourceName;
    if ("/".equals(scope)) {
      resourceName = "root area";
    } else if (scope.startsWith(tenantPrefix) && scope.length() > tenantPrefix.length()) {
      String tenant = scope.substring(tenantPrefix.length());
      if (tenant.endsWith("/")) {
        tenant = tenant.substring(0, tenant.length() - 1);
      }
      resourceName = "tenant '" + tenant + "'";
    } else {
      resourceName = scope;
    }
    return "Access denied; resource=" + resourceName;
  }

  /**
   * Method to create a session token.
   *
   * @param currentTime Current time
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext instance
   * @return Session token
   */
  public String createToken(long currentTime, long expirationTime, UserContext userContext) {
    return JwtTokenUtils.createToken(currentTime, expirationTime, userContext);
  }
}
