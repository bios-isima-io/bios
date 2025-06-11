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

import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.auth.v1.impl.AuthV1Impl;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.Constants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.dto.ChangePasswordRequest;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.AuthResponse;
import io.isima.bios.models.LoginResponse;
import io.isima.bios.models.Role;
import io.isima.bios.models.SessionAttribute;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Credentials;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthV1 implementation.
 *
 * <p>This version of the implementation translates the requests and replies to/from the AuthV1
 * requests and replies.
 */
public class AuthImpl implements Auth {
  private static final Logger logger = LoggerFactory.getLogger(AuthImpl.class);
  private final AuthV1Impl authV1;
  private final SharedConfig sharedConfig;

  private static final Pattern APP_NAME_PATTERN;

  static {
    APP_NAME_PATTERN = Pattern.compile(ValidatorConstants.APP_NAME_PATTERN);
  }

  public AuthImpl(AuthV1Impl authV1, SharedConfig sharedConfig) {
    this.authV1 = authV1;
    this.sharedConfig = sharedConfig;
  }

  private void validateAppName(String appName) throws InvalidValueException {
    // Empty appName is OK.
    if ((appName == null) || (appName.length() == 0)) {
      return;
    }

    if (appName.length() > ValidatorConstants.MAX_NAME_LENGTH) {
      throw new InvalidValueException(
          "Length of appName may not exceed "
              + ValidatorConstants.MAX_NAME_LENGTH
              + "; given length is "
              + appName.length()
              + ": '"
              + appName
              + "'");
    }
    final var matcher = APP_NAME_PATTERN.matcher(appName);
    // The appName should have a match, and it should be equal to the full matching pattern.
    if (!matcher.find() || !matcher.group().equals(appName)) {
      throw new InvalidValueException(
          "appName string can only contain alphanumerics, underscores, and dashes"
              + "; given appName is '"
              + appName
              + "'");
    }
  }

  @Override
  public CompletableFuture<AuthResponse> loginAsync(
      io.isima.bios.models.Credentials credentials, String clientVersion, ExecutionState state) {
    Objects.requireNonNull(credentials, "credentials");
    Objects.requireNonNull(credentials.getEmail(), "credentials.email");
    Objects.requireNonNull(state, "state");

    final String userName = credentials.getEmail();
    final String scope = "";
    final var translatedCredentials =
        new Credentials(
            userName,
            credentials.getPassword(),
            scope,
            credentials.getAppName(),
            credentials.getAppType());

    return (TfosConfig.isTestMode()
            ? sharedConfig.getSessionExpirationMillisAsync(state).toCompletableFuture()
            : CompletableFuture.completedFuture(0L))
        .thenCompose(
            (expirationMillis) ->
                authV1
                    .login(translatedCredentials, expirationMillis, clientVersion, state)
                    .thenCompose(
                        (originalResp) -> {
                          try {
                            validateAppName(originalResp.getAppName());
                          } catch (InvalidValueException e) {
                            throw new CompletionException(e);
                          }
                          return translateLoginResponseAsync(
                              credentials.getEmail(), originalResp, state);
                        }));
  }

  @Override
  public AuthResponse renewSession(SessionToken sessionToken) throws UserAccessControlException {
    final var userContext = validateSessionToken(sessionToken, "", null);
    logger.info(
        "v1.1 session renewal request received for user={} scope={} appName={}",
        userContext.getSubject(),
        userContext.getScope(),
        userContext.getAppName());

    final long expirationMillis =
        TfosConfig.isTestMode() ? sharedConfig.getSessionExpirationMillis() : 0;
    final var originalResp = authV1.renewSessionToken(sessionToken.getToken(), expirationMillis);
    return translateLoginResponse(userContext.getSubject(), (LoginResponse) originalResp);
  }

  private AuthResponse translateLoginResponse(
      final String email, final LoginResponse originalResp) {
    assert !ExecutorManager.isInIoThread();
    try {
      return translateLoginResponseAsync(
              email,
              originalResp,
              new GenericExecutionState("translate", ExecutorManager.getSidelineExecutor()))
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private CompletableFuture<AuthResponse> translateLoginResponseAsync(
      final String email, final LoginResponse originalResp, ExecutionState state) {

    return sharedConfig
        .getTenantCachedAsync(
            Constants.LOOKUP_QOS_THRESHOLD_MILLIS,
            originalResp.getTenant(),
            Constants.DEFAULT_LOOKUP_QOS_THRESHOLD_MILLIS_VALUE,
            true,
            state)
        .thenApply(
            (lookupQosThresholdMillis) -> {
              final var response = new AuthResponse();
              response.setToken(originalResp.getToken());
              // email match is case-insensitive, we make the address lower case to simplify the
              // later
              // processes
              response.setEmail(email.toLowerCase());
              response.setUserName(originalResp.getUserName());
              response.setTenantName(originalResp.getTenant());
              response.setAppName(originalResp.getAppName());
              response.setAppType(originalResp.getAppType());
              response.setExpiry(originalResp.getExpiry());
              response.setDevInstance(originalResp.getDevInstance());
              response.setRoles(convertPermissions(originalResp.getPermissions()));
              List<SessionAttribute> sessionAttributes = new ArrayList<>();
              sessionAttributes.add(
                  new SessionAttribute(
                      Constants.LOOKUP_QOS_THRESHOLD_MILLIS, lookupQosThresholdMillis));
              response.setSessionAttributes(sessionAttributes);
              return response;
            });
  }

  @Override
  public AuthResponse getLoginInfo(SessionToken sessionToken, boolean detail)
      throws UserAccessControlException {
    final var userContext = validateSessionToken(sessionToken, "", null);

    final var response = new AuthResponse();
    response.setToken(sessionToken.getToken());
    final var originalTenantName = userContext.getTenant();
    final var tenantName =
        "/".equals(originalTenantName) ? BiosConstants.TENANT_SYSTEM : originalTenantName;
    response.setTenantName(tenantName);
    response.setExpiry(userContext.getSessionExpiry());
    response.setEmail(userContext.getSubject());
    response.setRoles(convertPermissions(userContext.getPermissions()));
    final String lookupQosThresholdMillis =
        sharedConfig.getTenantCached(
            Constants.LOOKUP_QOS_THRESHOLD_MILLIS,
            tenantName,
            Constants.DEFAULT_LOOKUP_QOS_THRESHOLD_MILLIS_VALUE,
            true);
    List<SessionAttribute> sessionAttributes = new ArrayList<>();
    sessionAttributes.add(
        new SessionAttribute(Constants.LOOKUP_QOS_THRESHOLD_MILLIS, lookupQosThresholdMillis));
    response.setSessionAttributes(sessionAttributes);
    if (!detail) {
      return response;
    }

    final LoginResponse tfosLoginResponse;
    try {
      final var state =
          new GenericExecutionState("getUserInfo", ExecutorManager.getSidelineExecutor());
      tfosLoginResponse = authV1.getUserInfo(userContext, state).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserAccessControlException) {
        throw (UserAccessControlException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
    response.setEmail(userContext.getSubject());
    response.setUserName(tfosLoginResponse.getUserName());
    response.setDevInstance(tfosLoginResponse.getDevInstance());
    return response;
  }

  @Override
  public CompletableFuture<Void> changePassword(
      UserContext userContext, ChangePasswordRequest request, ExecutionState state) {
    if (request.getNewPassword() == null) {
      throw new IllegalArgumentException("New password must be set");
    }
    final String email;
    try {
      email =
          validateEmailForChangingPassword(
              userContext, request.getEmail(), request.getCurrentPassword());
    } catch (PermissionDeniedException e) {
      return CompletableFuture.failedFuture(e);
    }
    return authV1.changePassword(
        email, request.getCurrentPassword(), request.getNewPassword(), userContext, state);
  }

  private String validateEmailForChangingPassword(
      UserContext userContext, String email, String currentPassword)
      throws PermissionDeniedException {
    if (email == null || email.equalsIgnoreCase(userContext.getSubject())) {
      // This is a reset request of the user's own password. The current password must be set
      if (currentPassword == null) {
        throw new PermissionDeniedException(userContext, "Current password must be set");
      }
      return userContext.getSubject();
    }
    // This is cross-user password reset. Check whether the user is tenant admin.
    if (!userContext.getPermissions().contains(Permission.ADMIN)
        && !userContext.getPermissions().contains(Permission.SUPERADMIN)) {
      throw new PermissionDeniedException(
          userContext, "TenantAdmin role is required to reset password of another user");
    }
    return email;
  }

  @Override
  public UserContext validateSessionToken(
      SessionToken sessionToken, String tenantName, List<Permission> allowedRoles)
      throws UserAccessControlException {
    Objects.requireNonNull(tenantName, "'tenantName' must not be null");
    final String scope;
    if (tenantName.isBlank()) {
      scope = "";
    } else {
      scope = String.format("/tenants/%s/", tenantName);
    }
    return authV1.validateTokenStrictly(sessionToken, scope, allowedRoles);
  }

  @Override
  public String createToken(long currentTime, long expirationTime, UserContext userContext) {
    Objects.requireNonNull(userContext, "'userContext' must not be null");
    return authV1.createToken(currentTime, expirationTime, userContext);
  }

  @Override
  public String createSignupToken(long currentTime, long expiry, Long orgId, String email) {
    return authV1.createSignupToken(currentTime, expiry, orgId, email);
  }

  @Override
  public UserContext parseToken(SessionToken sessionToken) throws UserAccessControlException {
    return authV1.parseToken(sessionToken);
  }

  @Override
  public UserContext parseSignupToken(String signUpToken) throws UserAccessControlException {
    return authV1.parseSignupToken(signUpToken);
  }

  @Override
  public String hash(String input) {
    return authV1.hash(input);
  }

  private List<Role> convertPermissions(List<Permission> permissions) {
    if (permissions == null) {
      return null;
    }
    return permissions.stream()
        .map((permission) -> permission.toBios())
        .collect(Collectors.toList());
  }
}
