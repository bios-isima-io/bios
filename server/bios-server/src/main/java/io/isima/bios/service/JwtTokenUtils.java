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
package io.isima.bios.service;

import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.AuthError;
import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.SessionExpiredException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.utils.StringUtils;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JwtTokenUtils {
  private static final String TENANT = "tenant";
  private static final String SCOPE = "scope";
  private static final String USER = "user";
  private static final String ORG = "org";
  private static final String PERM = "perm";
  private static final String CLI_VER = "cli_ver";
  private static final String APP_NAME = "appname";
  private static final String APP_TYPE = "apptype";
  private static final String SOURCE = "source";
  private static final String DOMAIN = "domain";
  private static final String ADDITIONAL = "additional";

  private static final long MIN_RENEW_SESSION_LEAD_TIME = 10000;
  public static final String ALLOWED_CLOCK_SKEW_SECONDS_KEY = "prop.jwt.allowedClockSkewSeconds";
  public static final long ALLOWED_CLOCK_SKEW_SECONDS_DEFAULT = 5L;

  public static final String APP_SIGNATURE = "ed6f7414-9544-11ee-b9d1-0242ac120002";

  /** Generates a temporary session token for an internal fan-routing method call. */
  // TODO(Naoki): Eliminate this. User identifier should be necessary for audit logging
  public static SessionToken makeTemporarySessionToken(String tenantName, long timestamp) {
    return makeTemporarySessionToken(tenantName, timestamp, null, null);
  }

  /**
   * Generates a temporary session token for an internal fan-routing method call with email and user
   * ID.
   */
  public static SessionToken makeTemporarySessionToken(
      String tenantName, long timestamp, String email, Long userId) {
    final var userContext = new UserContext();
    userContext.setTenant(tenantName);
    userContext.setScope("/tenants/" + tenantName + "/");
    userContext.addPermissions(List.of(Permission.ADMIN));
    userContext.setSubject(email);
    userContext.setUserId(userId);
    final long expiry = timestamp + 240000; // the operation should be finished within 4 minutes
    userContext.setAppName("bios");
    userContext.setAppType(AppType.INTERNAL);
    final var tokenString = BiosModules.getAuth().createToken(timestamp, expiry, userContext);
    return new SessionToken(tokenString, false);
  }

  /**
   * Generates a temporary token with app master permission for setting up a new analytics domain
   */
  public static SessionToken makeTemporaryAppMasterToken(String masterTenant, String domain) {
    final var appMasterUserContext = new UserContext();
    final var currentTime = System.currentTimeMillis();
    final var expirationTime = currentTime + 300000; // 5 minutes
    appMasterUserContext.setTenant(masterTenant); // point to master tenant
    appMasterUserContext.setScope("/tenants/" + masterTenant + "/");
    appMasterUserContext.setSubject("app-master+wordpress@isima.io");
    appMasterUserContext.setDomain(domain);
    appMasterUserContext.addPermissions(List.of(Permission.APP_MASTER));
    appMasterUserContext.setAppName(domain + "_" + masterTenant);
    final var tokenString =
        BiosModules.getAuth().createToken(currentTime, expirationTime, appMasterUserContext);
    return new SessionToken(tokenString, false);
  }

  /**
   * Creates a session token with the default secret key.
   *
   * @param currentTime Current time in milliseconds
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext contains information about login user.
   * @throws NullPointerException when the userContext is null.
   */
  public static String createToken(long currentTime, long expirationTime, UserContext userContext) {
    return createToken(currentTime, expirationTime, userContext, TfosConfig.authTokenSecret());
  }

  /**
   * Creates a user token with an arbitrary secret key.
   *
   * @param currentTime Current time in milliseconds
   * @param expirationTime Expiration time in milliseconds
   * @param userContext UserContext contains information about login user.
   * @param secret Secret key
   * @throws NullPointerException when the userContext is null.
   */
  public static String createToken(
      long currentTime, long expirationTime, UserContext userContext, String secret) {
    Objects.requireNonNull(userContext);
    Date issuedAt = new Date(currentTime);
    Date expiration = new Date(expirationTime);

    JwtBuilder jwtBuilder =
        Jwts.builder()
            .setSubject(userContext.getSubject())
            .setIssuedAt(issuedAt)
            .setNotBefore(issuedAt)
            .setExpiration(expiration);

    if (isNotEmpty(userContext.getTenant())) {
      jwtBuilder.claim(TENANT, userContext.getTenant());
    }
    if (isNotEmpty(userContext.getScope())) {
      jwtBuilder.claim(SCOPE, userContext.getScope());
    }
    if (userContext.getUserId() != null) {
      jwtBuilder.claim(USER, userContext.getUserId());
    }
    if (userContext.getOrgId() != null) {
      jwtBuilder.claim(ORG, userContext.getOrgId());
    }
    if (!userContext.getPermissionIds().isEmpty()) {
      String permissions =
          String.join(
              ",",
              userContext.getPermissionIds().stream()
                  .map((value) -> value.toString())
                  .collect(Collectors.toList()));
      jwtBuilder.claim(PERM, permissions);
    }
    if (userContext.getClientVersion() != null) {
      jwtBuilder.claim(CLI_VER, userContext.getClientVersion());
    }
    if (userContext.getAppName() != null) {
      jwtBuilder.claim(APP_NAME, userContext.getAppName());
    }
    if (userContext.getAppType() != null) {
      jwtBuilder.claim(APP_TYPE, userContext.getAppType().name());
    }
    if (userContext.getDomain() != null) {
      jwtBuilder.claim(DOMAIN, userContext.getDomain());
    }
    if (userContext.getSource() != null) {
      jwtBuilder.claim(SOURCE, userContext.getSource());
    }
    if (userContext.getAdditionalUsers() != null) {
      jwtBuilder.claim(
          ADDITIONAL, userContext.getAdditionalUsers().stream().collect(Collectors.joining(",")));
    }

    String token = jwtBuilder.signWith(SignatureAlgorithm.HS512, secret).compact();
    return Base64.getEncoder().encodeToString(token.getBytes());
  }

  /**
   * Utility to check whether the given string is empty.
   *
   * @param input Input string
   * @return null -> false, "" -> false, otherwise true
   */
  private static boolean isNotEmpty(String input) {
    return input != null && !input.isEmpty();
  }

  /**
   * Method to parse the jwt token with default secret.
   *
   * <p>The method executes asynchronously because parsing requires reading shared properties.
   *
   * @param jwtToken Input JWT token.
   * @param isCookieAuth Flag to indicate whether the input token comes via a cookie. In case of
   *     true, the returned object includes renewal token when current token is close to expiry.
   * @param state Execution state used for fetching shared properties required for decoding.
   * @param acceptor Acceptor of the parsing output as a user context.
   * @param errorHandler The error handler.
   */
  public static void parseToken(
      String jwtToken,
      boolean isCookieAuth,
      ExecutionState state,
      Consumer<UserContext> acceptor,
      Consumer<Throwable> errorHandler) {
    parseToken(jwtToken, TfosConfig.authTokenSecret(), isCookieAuth, state, acceptor, errorHandler);
  }

  /**
   * Method to parse the jwt token with explicit secret.
   *
   * <p>The method executes asynchronously because parsing requires reading shared properties.
   *
   * @param jwtToken Input JWT token.
   * @param secret Token secret.
   * @param isCookieAuth Flag to indicate whether the input token comes via a cookie. In case of
   *     true, the returned object includes renewal token when current token is close to expiry.
   * @param state Execution state used for fetching shared properties required for decoding.
   * @param acceptor Acceptor of the parsing output as a user context.
   * @param errorHandler The error handler.
   */
  public static void parseToken(
      String jwtToken,
      String secret,
      boolean isCookieAuth,
      ExecutionState state,
      Consumer<UserContext> acceptor,
      Consumer<Throwable> errorHandler) {

    if (jwtToken == null) {
      logger.debug("token is not present");
      errorHandler.accept(new AuthenticationException(AuthError.MISSING_TOKEN));
      return;
    }

    decodeToken(
        jwtToken,
        secret,
        state,
        (claims) -> {
          // Check timestamps
          final Date nowDate = new Date();
          final long now = nowDate.getTime();
          final long expirationTime = claims.getExpiration().getTime();
          final var userContext = createUserContext(claims);
          if (now >= expirationTime) {
            String message =
                String.format(
                    "expiry=%s, valid duration=%s, subject=%s, tenant=%s, " + "appName=%s",
                    StringUtils.tsToIso8601Millis(claims.getExpiration().getTime()),
                    StringUtils.shortReadableDuration(
                        claims.getExpiration().getTime() - claims.getIssuedAt().getTime()),
                    userContext.getSubject(),
                    userContext.getTenant(),
                    userContext.getAppName());
            logger.info("Unauthorized: too late, token expired. {}", message);
            errorHandler.accept(
                new AuthenticationException(AuthError.SESSION_EXPIRED, message, userContext));
            return;
          }

          // If the session is close to the end, the server would renew the token
          final long sessionLifeTime = expirationTime - claims.getIssuedAt().getTime();
          final long leadTime =
              Math.max((long) (sessionLifeTime * 0.1), MIN_RENEW_SESSION_LEAD_TIME);

          if (isCookieAuth && now + leadTime >= expirationTime) {
            final long newExpiration = now + sessionLifeTime;
            logger.info(
                "Renewing session token; tenant={}, user={}, appName={}, issuedAt={}, "
                    + "expiry={}",
                userContext.getTenant(),
                userContext.getSubject(),
                userContext.getAppName(),
                nowDate,
                new Date(newExpiration));
            final var newToken = createToken(now, newExpiration, userContext);
            userContext.setNewSessionToken(newToken);
          }

          acceptor.accept(userContext);
        },
        errorHandler::accept);
  }

  public static CompletableFuture<Claims> decodeToken(
      String jwtToken, String signingKey, ExecutionState state) {
    final var future = new CompletableFuture<Claims>();
    decodeToken(jwtToken, signingKey, state, future::complete, future::completeExceptionally);
    return future;
  }

  private static void decodeToken(
      String jwtToken,
      String signingKey,
      ExecutionState state,
      Consumer<Claims> acceptor,
      Consumer<Throwable> errorHandler) {
    SharedProperties.getCachedAsync(
        ALLOWED_CLOCK_SKEW_SECONDS_KEY,
        ALLOWED_CLOCK_SKEW_SECONDS_DEFAULT,
        state,
        (allowedClockSkewSeconds) -> {
          try {
            String decoded = new String(Base64.getDecoder().decode(jwtToken));
            final var claims =
                Jwts.parser()
                    .setSigningKey(signingKey)
                    .setAllowedClockSkewSeconds(allowedClockSkewSeconds)
                    .parseClaimsJws(decoded)
                    .getBody();
            logger.trace(
                "expiry: {}, current: {}, claims: {}",
                claims.getExpiration(),
                System.currentTimeMillis(),
                claims);
            acceptor.accept(claims);
          } catch (ExpiredJwtException e) {
            final var claims = e.getClaims();
            final UserContext userContext = createUserContext(claims);
            var expiry = claims.getExpiration();
            logger.debug(
                "JWT token expired: subject={}, tenant={}, userId={}, appName={} "
                    + "expiry={} ({}), token={}, error={}",
                userContext.getSubject(),
                userContext.getTenant(),
                userContext.getUserId(),
                userContext.getAppName(),
                expiry,
                expiry.getTime(),
                jwtToken,
                e.toString());
            String message =
                String.format(
                    "expiry=%s, valid duration=%s, subject=%s, tenant=%s, " + "appName=%s",
                    StringUtils.tsToIso8601Millis(claims.getExpiration().getTime()),
                    StringUtils.shortReadableDuration(
                        claims.getExpiration().getTime() - claims.getIssuedAt().getTime()),
                    userContext.getSubject(),
                    userContext.getTenant(),
                    userContext.getAppName());
            final var error = new SessionExpiredException(message, userContext);
            errorHandler.accept(error);
          } catch (SignatureException | MalformedJwtException | IllegalArgumentException e) {
            logger.info(
                "Exception occurred while validating JWT: token={}, error={}",
                jwtToken,
                e.toString());
            errorHandler.accept(new AuthenticationException(AuthError.INVALID_TOKEN));
          }
        },
        errorHandler::accept);
  }

  public static UserContext createUserContext(Claims claims) {
    final UserContext userContext = new UserContext();

    userContext.setSubject(claims.getSubject());

    if (claims.containsKey(TENANT)) {
      userContext.setTenant(claims.get(TENANT, String.class));
    }

    if (claims.containsKey(SCOPE)) {
      userContext.setScope(claims.get(SCOPE, String.class));
    }

    if (claims.containsKey(USER)) {
      userContext.setUserId(claims.get(USER, Long.class));
    }

    if (claims.containsKey(ORG)) {
      userContext.setOrgId(claims.get(ORG, Long.class));
    }

    if (claims.containsKey(PERM)) {
      String perm = claims.get(PERM, String.class);
      String[] tokens = perm.split(",");
      List<Integer> permissions = new ArrayList<>(tokens.length);
      for (String token : tokens) {
        permissions.add(Integer.parseInt(token));
      }
      userContext.setPermissions(permissions);
    }

    if (claims.containsKey(CLI_VER)) {
      userContext.setClientVersion(claims.get(CLI_VER, String.class));
    }

    if (claims.containsKey(APP_NAME)) {
      userContext.setAppName(claims.get(APP_NAME, String.class));
    }

    if (claims.containsKey(APP_TYPE)) {
      userContext.setAppType(AppType.valueOf(claims.get(APP_TYPE, String.class)));
    }

    if (claims.getExpiration() != null) {
      userContext.setSessionExpiry(claims.getExpiration().getTime());
    }

    if (claims.containsKey(DOMAIN)) {
      userContext.setDomain(claims.get(DOMAIN, String.class));
    }

    if (claims.containsKey(SOURCE)) {
      userContext.setSource(claims.get(SOURCE, String.class));
    }

    if (claims.containsKey(ADDITIONAL)) {
      final String[] elements = claims.get(ADDITIONAL, String.class).split(",");
      userContext.setAdditionalUsers(List.of(elements));
    }

    return userContext;
  }

  /** Create a token for an application. */
  public static String createAppToken(
      String appName, String source, String tenantName, String domain, String email) {
    final var now = new Date();
    final var jwtBuilder =
        Jwts.builder()
            .setSubject(Objects.requireNonNull(email))
            .setIssuedAt(now)
            .setNotBefore(now)
            .claim(APP_NAME, appName);
    if (source != null) {
      jwtBuilder.claim(SOURCE, source);
    }
    if (tenantName != null) {
      jwtBuilder.claim(TENANT, tenantName);
    }
    if (domain != null) {
      jwtBuilder.claim(DOMAIN, domain);
    }
    final var token = jwtBuilder.signWith(SignatureAlgorithm.HS512, APP_SIGNATURE).compact();
    final var out = Base64.getEncoder().encode(token.getBytes());
    return new String(out);
  }
}
