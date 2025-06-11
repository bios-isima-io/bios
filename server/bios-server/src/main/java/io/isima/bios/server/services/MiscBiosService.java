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

import static io.isima.bios.service.Keywords.TENANT_NAME;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.common.TfosConfig;
import io.isima.bios.common.TfosUtils;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.dto.TestMethodParams;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.TestServiceHandler;
import io.netty.handler.codec.Headers;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class MiscBiosService extends BiosService {

  public final TestServiceHandler testHandler;
  private final RestHandler<Void, Void, GenericExecutionState> optionsHandler;

  public MiscBiosService() {

    testHandler = BiosModules.getTestServiceHandler();

    /**
     * Handles OPTIONS requests.
     *
     * <p>This is a special handler that can be invoked from any path, so is globally available.
     */
    optionsHandler =
        new RestHandler<>(
            "EntireServerOptions",
            OPTIONS,
            "", // TODO(BIOS-4998): This should be an asterisk
            HttpResponseStatus.NO_CONTENT,
            MediaType.NONE,
            MediaType.NONE,
            Void.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              Void request,
              Headers<CharSequence, CharSequence, ?> requestHeaders,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            state.addHistory("(options");
            // We keep using the same thread from the caller assuming it's in an i/o thread.

            // Allowed methods have been populated by handler resolver. We copy them to the
            // response.
            // TODO(BIOS-4998): Collect all supported methods and use them for target '*'.
            // See https://www.rfc-editor.org/rfc/rfc7231#page-31
            final var methods = requestHeaders.get(Keywords.X_BIOS_ALLOWED_METHODS);
            if (requestHeaders.contains(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD)
                && requestHeaders.contains(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS)) {
              // It's a preflight request
              responseHeaders.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, methods);
              responseHeaders.add(
                  HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, Bios2Config.corsAllowHeaders());
              responseHeaders.add(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, Bios2Config.corsMaxAge());
            } else {
              responseHeaders.add(HttpHeaderNames.ALLOW, methods);
            }
            state.addHistory(")");
            return CompletableFuture.completedFuture(null);
          }
        };
  }

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "Version",
            GET,
            BiosServicePath.PATH_VERSION,
            OK,
            MediaType.NONE,
            MediaType.TEXT_PLAIN,
            Void.class,
            String.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<String> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            return CompletableFuture.completedFuture(TfosUtils.version());
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "Analytics",
            POST,
            BiosServicePath.PATH_ANALYTICS,
            OK,
            MediaType.APPLICATION_JSON_DECODE_TO_STRING,
            MediaType.NONE,
            String.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              String data,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var tenantName = getString(pathParams, TENANT_NAME, null);
            if (tenantName == null) {
              return CompletableFuture.failedFuture(
                  new InvalidRequestException("Path parameter tenantName must be set"));
            }
            return CompletableFuture.completedFuture(null);
          }
        });

    if (TfosConfig.isTestMode()) {
      registerTestService(serviceRouter);
    }
  }

  private void registerTestService(ServiceRouter serviceRouter) {

    /**
     * Check the data types of the JSON reader on the server side.
     *
     * <p>The method receives list of object (any type) values; Returns data types of corresponding
     * decoded values.
     *
     * @param values Input values.
     * @return List of strings that tells the data types of the JSON reader for the input.
     */
    serviceRouter.addHandler(
        new RestHandler<>(
            "CheckJsonType",
            POST,
            BiosServicePath.PATH_TEST_JSON_TYPE,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            List.class,
            List.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<List> handle(
              List values,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            return testHandler.checkJsonReaderTypesAsync(sessionToken, values, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "Delay",
            POST,
            BiosServicePath.PATH_TEST_DELAY,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            TestMethodParams.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              TestMethodParams params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            return testHandler.delayAsync(sessionToken, params, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "Log",
            POST,
            BiosServicePath.PATH_TEST_LOG,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            TestMethodParams.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              TestMethodParams params,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            return testHandler.logMessage(sessionToken, params, state);
          }
        });
  }

  public RestHandler<Void, Void, GenericExecutionState> getOptionsHandler() {
    return optionsHandler;
  }
}
