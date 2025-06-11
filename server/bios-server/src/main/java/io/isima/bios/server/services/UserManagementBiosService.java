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

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.PATCH;
import static io.netty.handler.codec.http.HttpMethod.POST;

import io.isima.bios.dto.GetUsersResponse;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.UserConfig;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.UserManagementServiceHandler;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserManagementBiosService extends BiosService {
  public static final String ROOT = "/bios/v1/users";

  private final UserManagementServiceHandler handler =
      BiosModules.getUserManagementServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "Get Users",
            GET,
            ROOT,
            HttpResponseStatus.OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            GetUsersResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<GetUsersResponse> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(getUsers");
            final var sessionToken = getSessionToken(headers);
            final var optionalEmails =
                (queryParams == null) ? null : queryParams.get(Keywords.EMAIL);

            return handler.getUsers(sessionToken, optionalEmails, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateUser",
            POST,
            ROOT,
            HttpResponseStatus.OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            UserConfig.class,
            UserConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<UserConfig> handle(
              UserConfig userConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(getUsers");
            final var sessionToken = getSessionToken(headers);

            return handler.executeInSideline(
                state, () -> handler.createUser(sessionToken, userConfig, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ModifyUser",
            PATCH,
            ROOT + "/{userId}",
            HttpResponseStatus.OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            UserConfig.class,
            UserConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<UserConfig> handle(
              UserConfig userConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(getUsers");
            final var sessionToken = getSessionToken(headers);
            final Long userId = Long.parseLong(pathParams.get(Keywords.USER_ID));

            return handler.executeInSideline(
                state, () -> handler.modifyUser(sessionToken, userId, userConfig, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteUser",
            DELETE,
            ROOT,
            HttpResponseStatus.OK,
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
            state.addHistory("(getUsers");
            final var sessionToken = getSessionToken(headers);
            final var optionalEmail =
                (queryParams == null) ? null : queryParams.get(Keywords.EMAIL);
            final Long optionalUserId = getLong(queryParams, Keywords.USER_ID, null);

            return handler.executeInSideline(
                state,
                () -> handler.deleteUser(sessionToken, optionalEmail, optionalUserId, state));
          }
        });
  }
}
