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
package io.isima.bios.server.services;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.OK;

import io.isima.bios.dto.ChangePasswordRequest;
import io.isima.bios.dto.PasswordSpec;
import io.isima.bios.dto.SignupRequest;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.Credentials;
import io.isima.bios.models.LoginResponse;
import io.isima.bios.models.SessionInfo;
import io.isima.bios.models.SessionInfoBuilder;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.handler.AuthServiceHandler;
import io.isima.bios.service.handler.SignupServiceHandler;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthBiosService extends BiosService {

  private final AuthServiceHandler handler = BiosModules.getAuthServiceHandler();
  private final SignupServiceHandler signupHandler = BiosModules.getSignUpServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "Login",
            POST,
            BiosServicePath.PATH_AUTH_LOGIN,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            Credentials.class,
            SessionInfo.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(ExecutorManager.getSidelineExecutor());
          }

          @Override
          protected CompletableFuture<SessionInfo> handle(
              Credentials credentials,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(login");
            final var clientVersionSrc = getClientVersion(headers);
            // LoginResponse did not have JsonIgnoreProperties annotation, then enhanced properties
            // would cause error in Java SDK. In order to workaround it, we check version and
            // remove new properties for old SDKs. This should be solved in future by adding
            // JsonIgnoreProperties to DTOs.
            final BiosVersion clientVersion = makeBiosVersion(clientVersionSrc);

            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .login(credentials, clientVersionSrc, state)
                        .thenApply(
                            (response) -> {
                              final var appName =
                                  state.getUserContext() != null
                                      ? state.getUserContext().getAppName()
                                      : null;
                              final var appType =
                                  state.getUserContext() != null
                                      ? state.getUserContext().getAppType()
                                      : null;
                              logger.info(
                                  "User logged in; email={}, appName={}, appType={}",
                                  credentials.getEmail(),
                                  appName != null ? appName : "",
                                  appType != null ? appType : "");
                              return SessionInfoBuilder.getBuilder(response, clientVersion).build();
                            }));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "Logout",
            POST,
            BiosServicePath.PATH_AUTH_LOGOUT,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            Void.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            state.addHistory("(logout");
            final var cookie = makeCookie("");
            cookie.setMaxAge(0);
            responseHeaders.add(
                HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .logout(sessionToken, state)
                        .thenRunAsync(() -> state.addHistory(")"), state.getExecutor())
                        .exceptionally(
                            (t) -> {
                              logger.warn("Error on logout: {}", t.toString());
                              return null;
                            }));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetUserInfo",
            GET,
            BiosServicePath.PATH_AUTH_INFO,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            SessionInfo.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<SessionInfo> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(getUserInfo");
            final var clientVersionSrc = getClientVersion(headers);
            final var sessionToken = getSessionToken(headers);
            final BiosVersion clientVersion = makeBiosVersion(clientVersionSrc);
            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .getUserInfo(sessionToken, clientVersionSrc, state)
                        .thenApply(
                            (loginResponse) -> {
                              state.addHistory(")");
                              return SessionInfoBuilder.getBuilder(loginResponse, clientVersion)
                                  .build();
                            }));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "RenewSession",
            GET,
            BiosServicePath.PATH_AUTH_RENEW,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            SessionInfo.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<SessionInfo> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(renewSession");
            final var clientVersionSrc = getClientVersion(headers);
            final var sessionToken = getSessionToken(headers);
            final BiosVersion clientVersion = makeBiosVersion(clientVersionSrc);
            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .renewSession(sessionToken, state)
                        .thenApply(
                            (loginResponse) -> {
                              state.addHistory(")");
                              return SessionInfoBuilder.getBuilder(loginResponse, clientVersion)
                                  .build();
                            }));
          }
        });

    // We have to keep this until all SDK is upgraded to 1.1.2 or later.
    // SDKs before version 1.1.2 use this URL to renew session.
    serviceRouter.addHandler(
        new RestHandler<>(
            "RenewSessionDeprecated",
            GET,
            BiosServicePath.PATH_AUTH_RENEW_DEPRECATED,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            LoginResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<LoginResponse> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(renewSession");
            final var sessionToken = getSessionToken(headers);
            return handler.executeInSideline(
                state,
                () -> {
                  final long startTime = System.currentTimeMillis();
                  return handler
                      .renewSession(sessionToken, state)
                      .thenApply(
                          (authResponse) -> {
                            state.addHistory(")");
                            // conventional C-SDK (before version 1.1.2) uses following parameters
                            // to renew a session:
                            //   - token
                            //   - expiry
                            //   - sessionTimeoutMillis
                            //   - sessionAttributes
                            //
                            final long sessionTimeoutMillis = authResponse.getExpiry() - startTime;
                            final var loginResponse =
                                new LoginResponse(
                                    authResponse.getToken(),
                                    authResponse.getTenantName(),
                                    authResponse.getUserName(),
                                    authResponse.getAppName(),
                                    authResponse.getAppType(),
                                    authResponse.getExpiry(),
                                    List.of(),
                                    sessionTimeoutMillis,
                                    authResponse.getDevInstance());
                            loginResponse.setSessionAttributes(authResponse.getSessionAttributes());
                            return loginResponse;
                          });
                });
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ChangePassword",
            POST,
            BiosServicePath.PATH_AUTH_CHANGE_PASSWORD,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ChangePasswordRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              ChangePasswordRequest changePasswordRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(changePassword");
            final var sessionToken = getSessionToken(headers);

            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .changePassword(sessionToken, changePasswordRequest, state)
                        .thenRun(() -> state.addHistory(")")));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ChangePassword",
            POST,
            BiosServicePath.PATH_AUTH_CHANGE_PASSWORD_DEPRECATED,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ChangePasswordRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              ChangePasswordRequest changePasswordRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(changePassword");
            final var sessionToken = getSessionToken(headers);

            return handler.executeInSideline(
                state,
                () ->
                    handler
                        .changePassword(sessionToken, changePasswordRequest, state)
                        .thenRun(() -> state.addHistory(")")));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "InitiatePasswordReset",
            POST,
            BiosServicePath.PATH_AUTH_FORGOT_PASSWORD,
            ACCEPTED,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            SignupRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              SignupRequest params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(initiatePasswordReset");

            return signupHandler.executeInSideline(
                state, () -> signupHandler.initiatePasswordReset(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ForgotPasswordDeprecated",
            POST,
            BiosServicePath.PATH_AUTH_FORGOT_PASSWORD_DEPRECATED,
            ACCEPTED,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            SignupRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              SignupRequest params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(resetPassword");

            return signupHandler.executeInSideline(
                state, () -> signupHandler.initiatePasswordReset(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ResetPassword",
            POST,
            BiosServicePath.PATH_RESET_PASSWORD,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            PasswordSpec.class,
            String.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<String> handle(
              PasswordSpec params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(resetPassword");

            return signupHandler.executeInSideline(
                state, () -> signupHandler.resetPassword(params, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ResetPasswordDeprecated",
            POST,
            BiosServicePath.PATH_RESET_PASSWORD_DEPRECATED,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            PasswordSpec.class,
            String.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<String> handle(
              PasswordSpec params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(resetPassword");

            return signupHandler.executeInSideline(
                state, () -> signupHandler.resetPassword(params, state));
          }
        });
  }

  private BiosVersion makeBiosVersion(String clientVersionSrc) {
    try {
      return new BiosVersion(clientVersionSrc);
    } catch (RuntimeException e) { // ugly but ok
      return new BiosVersion(null);
    }
  }
}
