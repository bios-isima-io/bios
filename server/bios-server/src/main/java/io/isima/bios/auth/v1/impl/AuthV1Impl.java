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
package io.isima.bios.auth.v1.impl;

import static io.isima.bios.service.JwtTokenUtils.ALLOWED_CLOCK_SKEW_SECONDS_DEFAULT;
import static io.isima.bios.service.JwtTokenUtils.ALLOWED_CLOCK_SKEW_SECONDS_KEY;

import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.auth.v1.AuthV1;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.Constants;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.AuthError;
import io.isima.bios.errors.SignupError;
import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.InvalidAccessRequestException;
import io.isima.bios.errors.exception.NoSuchUserException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.DevInstanceConfig;
import io.isima.bios.models.LoginResponse;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.SessionAttribute;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.auth.Group;
import io.isima.bios.models.auth.Organization;
import io.isima.bios.models.auth.User;
import io.isima.bios.models.v1.Credentials;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.auth.GroupRepository;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.service.JwtTokenUtils;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.ServiceHandler;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for AuthV1 APIs as a Singleton class.
 *
 * @author aj
 */
public class AuthV1Impl implements AuthV1 {
  private static Logger logger = LoggerFactory.getLogger(AuthV1Impl.class);

  protected final PasswordManager passwordManager = new PasswordManager();

  private final OrganizationRepository orgRepo;
  private final GroupRepository groupRepo;
  private final UserRepository userRepo;
  private final SharedConfig sharedConfig;

  /**
   * Default constructor.
   *
   * <p>Used for testing purpose
   */
  public AuthV1Impl() {
    this.orgRepo = null;
    this.userRepo = null;
    this.groupRepo = null;
    this.sharedConfig = null;
  }

  public AuthV1Impl(
      final OrganizationRepository orgRepo,
      final UserRepository userRepo,
      final GroupRepository groupRepo,
      SharedConfig sharedConfig) {
    this.orgRepo = orgRepo;
    this.userRepo = userRepo;
    this.groupRepo = groupRepo;
    this.sharedConfig = sharedConfig;
  }

  /**
   * Method handle user login.
   *
   * @param credentials user login information
   * @return CompletableFuture of LoginResponse.
   */
  @Override
  public CompletableFuture<LoginResponse> login(
      Credentials credentials, long expirationMillis, String clientVersion, ExecutionState state) {
    return login(null, credentials, expirationMillis, clientVersion, state);
  }

  /**
   * Method handle user login.
   *
   * @param domain host name or domain name of server
   * @param credentials user login information
   * @return CompletableFuture of LoginResponse.
   */
  @Override
  public CompletableFuture<LoginResponse> login(
      String domain,
      Credentials credentials,
      long expirationMillis,
      String clientVersion,
      ExecutionState state) {

    final CompletableFuture<LoginResponse> loginFut = new CompletableFuture<>();
    if (credentials == null) {
      AuthenticationException exception =
          new AuthenticationException(AuthError.INSUFFICIENT_CREDENTIALS);
      loginFut.completeExceptionally(exception);
      return loginFut;
    }

    // TODO(Naoki): Not sure whether we should reject null scope or handle it as a blank.
    //   I'll treat it as a blank for now to prevent NPE without changing the logic flow of
    //   this method. This may miss any potential bug in the upper layer though.
    final String scope = credentials.getScope() != null ? credentials.getScope() : "";
    final String email = credentials.getUsername();
    final String password = credentials.getPassword();
    final String appName = credentials.getAppName() != null ? credentials.getAppName() : "";
    if (email == null || password == null) {
      AuthenticationException exception =
          new AuthenticationException(AuthError.INSUFFICIENT_CREDENTIALS, email, scope, appName);
      loginFut.completeExceptionally(exception);
      return loginFut;
    }

    // These parameters are available among all asynchrnous handlers in this method.
    // This is ugly but we keep it as is for quicker migration to V2 code base.
    final List<Long> userGroupIds = new ArrayList<>();
    final Set<Integer> userPermissions = new HashSet<>();
    final UserContext userContext =
        state.getUserContext() != null ? state.getUserContext() : new UserContext();
    final var devInstance = new DevInstanceConfig();
    final var userName = new String[1];

    Function<Throwable, Void> errorFunc =
        (ex) -> {
          Throwable cause = ex.getCause();
          if (cause == null) {
            cause = ex;
          }
          if (cause instanceof AuthenticationException) {
            loginFut.completeExceptionally(cause);
          } else {
            ApplicationException exception =
                new ApplicationException(
                    "Failed to validate credentials of user " + email + " in scope " + scope,
                    cause);
            loginFut.completeExceptionally(exception);
          }
          return null;
        };

    Consumer<List<Group>> groupConsumer =
        (groups) -> {
          groups.forEach(
              group -> {
                userPermissions.addAll(Permission.parse(group.getPermissions()));
              });
          userContext.addPermissionIds(userPermissions);
          userContext.setClientVersion(clientVersion);

          // Resolve appType
          final var specifiedAppType = credentials.getAppType();
          final AppType appType;
          if (userContext.getPermissions().contains(Permission.INTERNAL)) {
            appType = AppType.INTERNAL;
          } else if (specifiedAppType == null) {
            appType = AppType.UNKNOWN;
          } else {
            appType = specifiedAppType;
          }
          userContext.setAppName(appName);
          userContext.setAppType(appType);

          loginFut.complete(
              createLoginResponse(userName[0], userContext, expirationMillis, devInstance));
        };

    Function<Organization, CompletableFuture<List<Group>>> orgByIdHandler =
        (org) -> {
          final var tenantName =
              SCOPE_ROOT.equals(org.getTenant()) ? BiosConstants.TENANT_SYSTEM : org.getTenant();
          userContext.setTenant(tenantName);
          if (StringUtils.isNotBlank(scope)) {
            userContext.setScope(scope.toLowerCase());
          } else if (org.getTenant().equals(SCOPE_ROOT)) {
            userContext.setScope(SCOPE_SYSTEM);
          } else {
            userContext.setScope(String.format(AuthV1.SCOPE_TENANT, org.getTenant()));
          }

          if (userGroupIds.isEmpty()) {
            CompletableFuture<List<Group>> fut = new CompletableFuture<>();
            fut.complete(Collections.emptyList());
            return fut;
          } else {
            return groupRepo.findByIds(org.getId(), userGroupIds, state);
          }
        };

    Function<User, CompletableFuture<Organization>> userHandler =
        (user) -> {
          if (user == null) {
            AuthenticationException exception =
                new AuthenticationException(AuthError.USERNAME_NOT_FOUND, email, scope, appName);
            CompletableFuture<Organization> fut = new CompletableFuture<>();
            fut.completeExceptionally(exception);
            return fut;
          }

          final var userStatus = user.getStatus();
          if (userStatus != MemberStatus.ACTIVE) {
            final AuthError errorType;
            switch (userStatus) {
              case SUSPENDED:
              case DELETED:
                errorType = AuthError.DISABLED_USER;
                break;
              case TO_BE_VERIFIED:
                errorType = AuthError.USER_ID_NOT_VERIFIED;
                break;
              default:
                return CompletableFuture.failedFuture(
                    new ApplicationException("Unexpected user status: " + userStatus));
            }
            AuthenticationException exception =
                new AuthenticationException(errorType, email, scope, appName);
            return CompletableFuture.failedFuture(exception);
          }

          if (!passwordManager.check(password, user.getPassword())) {
            AuthenticationException exception =
                new AuthenticationException(AuthError.INVALID_PASSWORD, email, scope, appName);
            CompletableFuture<Organization> fut = new CompletableFuture<>();
            fut.completeExceptionally(exception);
            return fut;
          }

          userContext.setUserId(user.getId());
          userContext.setOrgId(user.getOrgId());

          userContext.setSubject(user.getEmail());
          userName[0] = user.getName();

          userGroupIds.addAll(user.getGroupIds());
          userPermissions.addAll(Permission.parse(user.getPermissions()));

          populateDevInstance(user, devInstance);
          startNotebookServerForHubUser(
              devInstance.getDevInstanceName(), user.getEmail(), devInstance.getSshKey());

          if (StringUtils.isNotBlank(scope)) {
            Organization org = new Organization();
            org.setId(user.getOrgId());
            org.setTenant(getTenantNameFromScope(scope));
            return CompletableFuture.completedFuture(org);
          } else {
            return orgRepo.findById(user.getOrgId(), state);
          }
        };

    Function<Organization, CompletableFuture<User>> orgByTenantHandler =
        (org) -> {
          if (org == null) {
            AuthenticationException exception =
                new AuthenticationException(AuthError.INVALID_SCOPE, email, scope, appName);
            CompletableFuture<User> fut = new CompletableFuture<>();
            fut.completeExceptionally(exception);
            return fut;
          } else {
            return userRepo.findByEmail(org.getId(), email, state);
          }
        };

    if (StringUtils.isNotBlank(scope)) {
      final String tenantName = getTenantNameFromScope(scope);
      if (StringUtils.isBlank(tenantName)) {
        AuthenticationException exception =
            new AuthenticationException(AuthError.INVALID_SCOPE, email, scope, appName);
        loginFut.completeExceptionally(exception);
        return loginFut;
      }
      orgRepo
          .findByTenantName(tenantName, state)
          .thenCompose(orgByTenantHandler)
          .thenCompose(userHandler)
          .thenCompose(orgByIdHandler)
          .thenAccept(groupConsumer)
          .exceptionally(errorFunc);
    } else {
      userRepo
          .findByEmail(email, state)
          .thenCompose(userHandler)
          .thenCompose(orgByIdHandler)
          .thenAccept(groupConsumer)
          .exceptionally(errorFunc);
    }

    return loginFut;
  }

  private void populateDevInstance(User user, DevInstanceConfig devInstance) {
    devInstance.setDevInstanceName(user.getDevInstance());
    devInstance.setSshKey(user.getSshKey());
    devInstance.setSshUser(user.getSshUser());
    devInstance.setSshIp(user.getSshIp());
    Long port = user.getSshPort();
    devInstance.setSshPort(port != null ? port.intValue() : null);
  }

  private void startNotebookServerForHubUser(String instanceName, String email, String hubKey) {
    if (instanceName != null && instanceName.startsWith("hub_")) {
      logger.info("Starting notebook server for user: {}", email);
      try {
        BiosModules.getJupyterHubAdmin().startNotebookServerForUser(email, hubKey);
      } catch (TfosException e) {
        logger.error("Error occurred while starting notebook server for user: {}", email, e);
      }
    }
  }

  private String getTenantNameFromScope(final String scope) {
    if (StringUtils.isBlank(scope)) {
      throw new IllegalArgumentException("invalid scope for login");
    } else if (StringUtils.equals(scope, SCOPE_ROOT)) {
      return SCOPE_ROOT;
    } else {
      return StringUtils.substringBetween(scope, SCOPE_PREFIX, SCOPE_DELIMITER);
    }
  }

  private LoginResponse createLoginResponse(
      String userName,
      final UserContext userContext,
      long expirationMillis,
      DevInstanceConfig devInstance) {
    final long currentTime = System.currentTimeMillis();
    if (expirationMillis == 0) {
      expirationMillis = TfosConfig.authExpirationTimeMillis();
    }
    final long expirationTime = currentTime + expirationMillis;
    logger.debug("EXPIRY: {} ({})", expirationTime, new Date(expirationTime));

    final String token = createToken(currentTime, expirationTime, userContext);
    final String tenant = userContext.getTenant();
    final String appName = userContext.getAppName();
    final AppType appType = userContext.getAppType();

    LoginResponse loginResponse =
        new LoginResponse(
            token,
            tenant,
            userName,
            appName,
            appType,
            expirationTime,
            userContext.getPermissions(),
            expirationMillis,
            devInstance);
    return loginResponse;
  }

  @Override
  public CompletableFuture<LoginResponse> getUserInfo(
      UserContext userContext, ExecutionState state) {
    return userRepo
        .findByEmail(userContext.getSubject(), state)
        .thenApply(
            (user) -> {
              if (user == null) {
                throw new CompletionException(
                    new AuthenticationException(AuthError.USERNAME_NOT_FOUND));
              }
              final var devInstance = new DevInstanceConfig();
              populateDevInstance(user, devInstance);
              startNotebookServerForHubUser(
                  devInstance.getDevInstanceName(),
                  userContext.getSubject(),
                  devInstance.getSshKey());
              final var resp =
                  new LoginResponse(
                      null,
                      userContext.getTenant(),
                      user.getName(),
                      userContext.getAppName(),
                      userContext.getAppType(),
                      userContext.getSessionExpiry(),
                      userContext.getPermissions(),
                      0,
                      devInstance);
              return resp;
            });
  }

  /**
   * Method parse sign-up token.
   *
   * @param signupToken User signup token
   * @return UserContext contains information about user.
   * @throws TfosException for invalid token
   */
  @Override
  public UserContext parseSignupToken(String signupToken) throws UserAccessControlException {
    if (signupToken == null) {
      throw new AuthenticationException(SignupError.INVALID_REQUEST);
    }

    try {
      final long allowedClockSkewSeconds =
          SharedProperties.getCached(
              ALLOWED_CLOCK_SKEW_SECONDS_KEY, ALLOWED_CLOCK_SKEW_SECONDS_DEFAULT);
      String decoded = new String(Base64.getDecoder().decode(signupToken));
      Claims claims =
          Jwts.parser()
              .setSigningKey(TfosConfig.authTokenSecret())
              .setAllowedClockSkewSeconds(allowedClockSkewSeconds)
              .parseClaimsJws(decoded)
              .getBody();

      Date now = new Date();
      Date expirationTime = claims.getExpiration();

      if (expirationTime.compareTo(now) <= 0) {
        throw new AuthenticationException(SignupError.LINK_EXPIRED);
      }

      return JwtTokenUtils.createUserContext(claims);
    } catch (SignatureException
        | MalformedJwtException
        | ExpiredJwtException
        | IllegalArgumentException e) {
      logger.info(
          "Exception occurred while validating sign up JWT: token={} error={}",
          signupToken,
          e.toString());
      throw new AuthenticationException(SignupError.INVALID_TOKEN);
    }
  }

  /**
   * Method create user sign-up token.
   *
   * @param currentTime Current time in milliseconds
   * @param expirationTime Expiration time in milliseconds
   * @param orgId Organization Id, if available
   * @param email Email of User
   * @return Token to complete sign-up process.
   */
  @Override
  public String createSignupToken(long currentTime, long expirationTime, Long orgId, String email) {
    UserContext userContext = new UserContext();
    userContext.setOrgId(orgId);
    userContext.setSubject(email);
    return createToken(currentTime, expirationTime, userContext);
  }

  /**
   * Method renew auth token.
   *
   * @param sessionToken Authentication token
   * @param expirationMillis Specifies expiration milliseconds. If 0 is specified, the system
   *     default is used.
   * @return LoginResponse.
   * @throws UserAccessControlException for invalid token
   */
  @Override
  public Object renewSessionToken(String sessionToken, long expirationMillis)
      throws UserAccessControlException {
    final SessionToken authToken = getAuthToken(sessionToken);
    if (authToken != null) {
      final UserContext userContext = parseToken(authToken);

      if (expirationMillis == 0) {
        expirationMillis = TfosConfig.authExpirationTimeMillis();
      }
      long currentTime = System.currentTimeMillis();
      long expirationTime = currentTime + expirationMillis;

      final String token = createToken(currentTime, expirationTime, userContext);

      LoginResponse resp =
          new LoginResponse(
              token,
              userContext.getTenant(),
              null,
              userContext.getAppName(),
              userContext.getAppType(),
              expirationTime,
              userContext.getPermissions(),
              expirationMillis,
              null);

      final String lookupQosThresholdMillis =
          sharedConfig.getTenantCached(
              Constants.LOOKUP_QOS_THRESHOLD_MILLIS,
              userContext.getTenant(),
              Constants.DEFAULT_LOOKUP_QOS_THRESHOLD_MILLIS_VALUE,
              true);
      List<SessionAttribute> sessionAttributes = new ArrayList<>();
      sessionAttributes.add(
          new SessionAttribute(Constants.LOOKUP_QOS_THRESHOLD_MILLIS, lookupQosThresholdMillis));
      resp.setSessionAttributes(sessionAttributes);

      return resp;
    } else {
      throw new AuthenticationException(AuthError.PERMISSION_DENIED);
    }
  }

  /**
   * Method to validate auth token and permission.
   *
   * @param authToken Authentication token
   * @param isCookie Whether the token is sent via cookie
   * @param scope Requested scope
   * @param requiredPermission Required permission to allow access.
   * @throws UserAccessControlException when the access is denied.
   */
  @Override
  public UserContext validateToken(
      String authToken, boolean isCookie, String scope, Permission requiredPermission)
      throws UserAccessControlException {

    final UserContext userContext;
    try {
      userContext =
          parseToken(
                  authToken,
                  isCookie,
                  new GenericExecutionState(
                      "ParseToken", io.isima.bios.service.handler.ServiceHandler.getExecutor()))
              .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UserAccessControlException) {
        throw (UserAccessControlException) e.getCause();
      }
      throw new RuntimeException(e);
    }

    // Check if user have necessary permission
    if (!userContext.getPermissions().contains(requiredPermission)) {
      throw new PermissionDeniedException(
          userContext, requiredPermission.name() + " privilege is required for this operation");
    }

    if (!scope.isEmpty() && !scope.toLowerCase().startsWith(userContext.getScope())) {
      throw new InvalidAccessRequestException(userContext, buildForbiddenMessage(scope));
    }
    return userContext;
  }

  /**
   * Method to validate auth token and permissions.
   *
   * @param sessionToken Session token
   * @param scope Scope of user
   * @param allowedRoles Allowed permissions, check if user have any.
   * @return Validation result.
   */
  @Override
  public UserContext validateToken(
      String sessionToken, boolean isCookie, String scope, List<Permission> allowedRoles)
      throws UserAccessControlException {
    return validateTokenCore(sessionToken, isCookie, scope, allowedRoles, false);
  }

  @Override
  public UserContext validateTokenStrictly(
      SessionToken sessionToken, String scope, List<Permission> allowedRoles)
      throws UserAccessControlException {
    return validateTokenCore(
        sessionToken.getToken(), sessionToken.isCookie(), scope, allowedRoles, true);
  }

  /**
   * Method to get auth token from auth header.
   *
   * @param authHeader Authentication header
   * @return Authentication token.
   */
  @Override
  public SessionToken getAuthToken(String authHeader) {
    if (authHeader == null || authHeader.isEmpty()) {
      return null;
    }

    String authToken = null;
    String[] tokens = authHeader.split(" ");
    if (tokens.length == 2 && tokens[0] != null && tokens[0].equals(Keywords.BEARER)) {
      authToken = tokens[1];
    } else if (tokens.length == 1 && tokens[0] != null) {
      authToken = tokens[0];
    }

    return new SessionToken(authToken, false);
  }

  /** Resets password. */
  public CompletableFuture<Void> changePassword(
      String email,
      String currentPassword,
      String newPassword,
      UserContext requesterUserContext,
      ExecutionState state) {
    return userRepo
        .findByEmail(email, state)
        .thenCompose(
            (user) -> {
              if (user == null) {
                return CompletableFuture.failedFuture(
                    new NoSuchUserException("No such user: " + email));
              }
              if (!user.getOrgId().equals(requesterUserContext.getOrgId())
                  && !requesterUserContext.getPermissions().contains(Permission.SUPERADMIN)) {
                throw new CompletionException(
                    new PermissionDeniedException(
                        requesterUserContext, "Not allowed to change password of user " + email));
              }

              if (currentPassword != null
                  && !passwordManager.check(currentPassword, user.getPassword())) {
                throw new CompletionException(
                    new PermissionDeniedException(
                        requesterUserContext, "Current password does not match"));
              }

              final Date date = new Date();

              user.setUpdatedAt(date);
              user.setPassword(hash(newPassword));

              return userRepo.save(user, state);
            })
        .thenAccept(
            (updatedUser) -> {
              // to convert the completable future of string to void
            });
  }

  /**
   * Method create a session token.
   *
   * @param currentTime Current time in milliseconds
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext contains information about login user.
   * @return Authentication token.
   * @throws NullPointerException when the userContext is null.
   */
  @Override
  public String createToken(long currentTime, long expirationTime, UserContext userContext) {
    return JwtTokenUtils.createToken(currentTime, expirationTime, userContext);
  }

  /**
   * get password manager.
   *
   * @return the passwordManager
   */
  public PasswordManager getPasswordManager() {
    return passwordManager;
  }

  @Override
  public String hash(String input) {
    return passwordManager.encode(input);
  }

  /**
   * Method to parse the session token.
   *
   * @param sessionToken Session token
   * @param isCookie Whether the token is sent over cokie
   * @param state
   * @return UserContext contains information about user.
   * @throws AuthenticationException for invalid token
   */
  @Override
  public CompletableFuture<UserContext> parseToken(
      String sessionToken, boolean isCookie, ExecutionState state) {
    final var future = new CompletableFuture<UserContext>();
    JwtTokenUtils.parseToken(
        sessionToken,
        isCookie,
        new GenericExecutionState("parseToken", ServiceHandler.getExecutor()),
        future::complete,
        future::completeExceptionally);
    return future;
  }

  protected UserContext validateTokenCore(
      String sessionToken,
      boolean isCookie,
      String requestedScope,
      List<Permission> allowedRoles,
      boolean isStrict)
      throws UserAccessControlException {

    final UserContext userContext;
    try {
      userContext =
          parseToken(
                  sessionToken,
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

    if (userContext.getPermissions().contains(Permission.APP_MASTER)) {
      final var tenantName = requestedScope.replace("/tenants/", "").replace("/", "");
      if (!tenantName.isBlank() && !userContext.getTenant().equalsIgnoreCase(tenantName)) {
        userContext.setIsAppMaster(true);
      }
    }

    // Check if user have necessary permission
    if (!hasPermission(userContext, allowedRoles)) {
      throw new PermissionDeniedException(userContext);
    }

    if (userContext.getIsAppMaster() != Boolean.TRUE) {
      if (isStrict) {
        final var userScope = userContext.getScope();
        if (!requestedScope.isEmpty() && !requestedScope.equalsIgnoreCase(userScope)) {
          throw new InvalidAccessRequestException(
              userContext, buildForbiddenMessage(requestedScope));
        }
      } else if (!requestedScope.isEmpty()
          && !requestedScope.toLowerCase().startsWith(userContext.getScope().toLowerCase())) {
        throw new InvalidAccessRequestException(userContext, buildForbiddenMessage(requestedScope));
      }
    }

    return userContext;
  }

  /**
   * Utility to check whether a specified user has any of allowed roles.
   *
   * @param userContext User context
   * @param allowedRoles Allowed roles for the operation.
   * @return True if permitted, false otherwise.
   */
  private boolean hasPermission(UserContext userContext, List<Permission> allowedRoles) {
    if (allowedRoles == null || allowedRoles.isEmpty()) {
      // Any user is allowed.
      // The user must have valid session token, which has been checked already.
      return true;
    } else {
      return allowedRoles.stream().anyMatch(userContext.getPermissions()::contains);
    }
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
    if (AuthV1.SCOPE_ROOT.equals(scope)) {
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
}
