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
package io.isima.bios.service.handler;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.audit.AuditConstants;
import io.isima.bios.audit.AuditInfo;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.audit.OperationStatus;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.ChangePasswordRequest;
import io.isima.bios.errors.AuthError;
import io.isima.bios.errors.exception.AuthenticationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AuthResponse;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.recorder.ControlRequestType;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthServiceHandler extends ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(AuthServiceHandler.class);

  public AuthServiceHandler(
      Auth auth,
      AuditManager auditManager,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics) {
    super(auditManager, auth, admin, dataEngine, metrics);
  }

  public CompletableFuture<AuthResponse> login(
      Credentials credentials, String clientVersion, ExecutionState state) {
    return loginAsync(credentials, clientVersion, state)
        .thenApply(
            (response) -> {
              final var userContext = state.getUserContext();
              userContext.setTenant(response.getTenantName());
              userContext.setSubject(response.getEmail());
              userContext.setAppName(credentials.getAppName());
              userContext.setAppType(credentials.getAppType());
              userContext.setNewSessionToken(response.getToken());
              state.setUserContext(userContext);
              return response;
            });
  }

  private CompletableFuture<AuthResponse> loginAsync(
      Credentials credentials, String clientVersion, ExecutionState state) {
    // setup the audit log item
    final AuditInfo auditInfo =
        auditManager.begin(AuditConstants.NA, AuditOperation.LOGIN, credentials.toString());
    auditInfo.setUser(credentials.getEmail());

    final var userContext = new UserContext();
    userContext.setSubject(credentials.getEmail());
    userContext.setAppName(credentials.getAppName());
    userContext.setAppType(credentials.getAppType());
    state.setUserContext(userContext);

    return auth.loginAsync(credentials, clientVersion, state)
        .thenApply(
            (loginResponse) -> {
              String response = null;
              OperationStatus operationStatus = OperationStatus.FAILURE;
              try {
                // We check whether the request specifies appType INTERNAL after successful
                // authentication.
                // We want to avoid giving any info to an unauthorized user.
                // TODO(Naoki): This is a rough edge.  Is there any good way to hide
                // AppType.INTERNAL
                //  from the user interface?
                if (credentials.getAppType() == AppType.INTERNAL) {
                  throw new InvalidRequestException(
                      "AppType INTERNAL cannot be specified externally");
                }

                // Attach a metrics recorder
                if (state.getMetricsTracer() != null) {
                  final var recorder =
                      metrics.getRecorder(
                          loginResponse.getTenantName(),
                          "",
                          credentials.getAppName(),
                          credentials.getAppType(),
                          ControlRequestType.LOGIN);
                  state.getMetricsTracer().attachRecorder(recorder);
                  state.getMetricsTracer().clearBytesWritten();
                }

                // prepare for audit log
                operationStatus = OperationStatus.SUCCESS;
                auditInfo.setTenant(loginResponse.getTenantName());
                // TODO(Naoki): Fill permissions
                // auditInfo.setPermissionATM();
                response = loginResponse.toString();

                logger.debug(
                    "Login completed; email={}, tenant={}",
                    credentials.getEmail(),
                    loginResponse.getTenantName());
                return loginResponse;
              } catch (TfosException e) {
                response = e.getMessage();
                operationStatus = OperationStatus.FAILURE;
                throw new CompletionException(e);
              } finally {
                auditManager.commit(auditInfo, operationStatus.name(), response);
              }
            });
  }

  public CompletableFuture<Void> checkUser(
      Credentials credentials, String clientVersion, GenericExecutionState state) {
    final var userContext = new UserContext();
    userContext.setSubject(credentials.getEmail());
    userContext.setAppName(credentials.getAppName());
    userContext.setAppType(credentials.getAppType());
    state.setUserContext(userContext);

    credentials.setPassword(" ");

    return auth.loginAsync(credentials, clientVersion, state)
        .thenAccept((none) -> {})
        .exceptionally(
            (t) -> {
              final var cause = t instanceof CompletionException ? t.getCause() : t;
              if (cause instanceof AuthenticationException) {
                final var error = (AuthenticationException) cause;
                if (error.getErrorCode().equals(AuthError.USER_ID_NOT_VERIFIED.getErrorCode())) {
                  throw new CompletionException(error);
                }
              }
              return null;
            });
  }

  public CompletableFuture<AuthResponse> renewSession(
      SessionToken sessionToken, ExecutionState state) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final var response = auth.renewSession(sessionToken);
            // Attach a metrics recorder
            if (state.getMetricsTracer() != null) {
              final var recorder =
                  metrics.getRecorder(
                      response.getTenantName(),
                      "",
                      response.getAppName(),
                      response.getAppType(),
                      ControlRequestType.RENEW_SESSION);
              state.getMetricsTracer().attachRecorder(recorder);
              state.getMetricsTracer().clearBytesWritten();
            }
            return response;
          } catch (TfosException e) {
            throw new CompletionException(e);
          }
        },
        getExecutor());
  }

  public CompletableFuture<AuthResponse> getUserInfo(
      SessionToken sessionToken, String clientVersion, ExecutionState state) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final var response = getLoginInfo(sessionToken, true);
            // Attach a metrics recorder
            if (state.getMetricsTracer() != null) {
              final var recorder =
                  metrics.getRecorder(
                      response.getTenantName(),
                      "",
                      response.getAppName(),
                      response.getAppType(),
                      ControlRequestType.RENEW_SESSION);
              state.getMetricsTracer().attachRecorder(recorder);
              state.getMetricsTracer().clearBytesWritten();
            }
            return response;
          } catch (TfosException | ApplicationException e) {
            throw new CompletionException(e);
          }
        },
        getExecutor());
  }

  public AuthResponse getLoginInfo(SessionToken authToken, boolean detail)
      throws TfosException, ApplicationException {
    return auth.getLoginInfo(authToken, detail);
  }

  public CompletableFuture<Void> changePassword(
      SessionToken sessionToken,
      ChangePasswordRequest changePasswordRequest,
      ExecutionState state) {

    final AuditInfo auditInfo =
        auditManager.begin(
            AuditConstants.NA, AuditOperation.CHANGE_PASSWORD, changePasswordRequest.toString());
    try {
      final UserContext userContext = auth.validateSessionToken(sessionToken, "", AllowedRoles.ANY);
      auditInfo.setUserContext(userContext);

      // Attach a metrics recorder
      if (state.getMetricsTracer() != null) {
        final var recorder =
            metrics.getRecorder(
                userContext.getTenant(),
                "",
                userContext.getAppName(),
                userContext.getAppType(),
                ControlRequestType.CHANGE_PASSWORD);
        state.getMetricsTracer().attachRecorder(recorder);
        state.getMetricsTracer().clearBytesWritten();
      }

      return auth.changePassword(userContext, changePasswordRequest, state)
          .thenAccept(
              (none) -> {
                auditManager.commit(auditInfo, OperationStatus.SUCCESS.name(), "");
                logger.info("Audit: {}", auditInfo);
              })
          .exceptionally(
              (t) -> {
                final boolean isCompletionException = (t instanceof CompletionException);
                final var cause = isCompletionException ? t.getCause() : t;
                final String response = cause.getMessage();
                auditManager.commit(auditInfo, OperationStatus.FAILURE.getName(), response);
                logger.info("Audit: {}", auditInfo);
                throw isCompletionException
                    ? (CompletionException) t
                    : new CompletionException(cause);
              });
    } catch (TfosException e) {
      final String response = e.getMessage();
      auditManager.commit(auditInfo, OperationStatus.FAILURE.getName(), response);
      logger.info("Audit: {}", auditInfo);
      return CompletableFuture.failedFuture(e);
    }
  }

  public CompletableFuture<Void> logout(SessionToken sessionToken, ExecutionState state) {

    try {
      final AuditInfo auditInfo = auditManager.begin(AuditConstants.NA, AuditOperation.LOGOUT, "");
      final UserContext userContext = auth.validateSessionToken(sessionToken, "", AllowedRoles.ANY);
      auditInfo.setUserContext(userContext);

      // Attach a metrics recorder
      if (state.getMetricsTracer() != null) {
        final var recorder =
            metrics.getRecorder(
                userContext.getTenant(),
                "",
                userContext.getAppName(),
                userContext.getAppType(),
                ControlRequestType.CHANGE_PASSWORD);
        state.getMetricsTracer().attachRecorder(recorder);
        state.getMetricsTracer().clearBytesWritten();
      }

      auditManager.commit(auditInfo, OperationStatus.SUCCESS.name(), "");
      logger.info("User {} logged out", userContext.getSubject());
    } catch (TfosException e) {
      // ignore quietly
    }

    // for now we do nothing
    return CompletableFuture.completedFuture(null);
  }
}
