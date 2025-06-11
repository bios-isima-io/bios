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
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.dto.MaintainContextRequest;
import io.isima.bios.dto.MaintainKeyspacesRequest;
import io.isima.bios.dto.MaintainTablesRequest;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppsInfo;
import io.isima.bios.models.ContextMaintenanceResult;
import io.isima.bios.models.EndpointType;
import io.isima.bios.models.TenantKeyspaces;
import io.isima.bios.models.TenantTables;
import io.isima.bios.models.UpdateEndpointsRequest;
import io.isima.bios.models.UpstreamConfig;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.netty.handler.codec.Headers;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemAdminBiosService extends BiosService {
  public static final Logger logger = LoggerFactory.getLogger(SystemAdminBiosService.class);
  public static final String DEFAULT_ENDPOINT_TYPE = "all";

  private final AdminServiceHandler v1Handler = BiosModules.getAdminServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "GetProperty",
            GET,
            BiosServicePath.PATH_PROPERTIES,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
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
            final var sessionToken = getSessionToken(headers);
            final String key = pathParams.get(Keywords.KEY);
            return v1Handler.getProperty(sessionToken, key, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "SetProperty",
            PUT,
            BiosServicePath.PATH_PROPERTIES,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
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
              String value,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String key = pathParams.get(Keywords.KEY);
            return v1Handler.setProperty(sessionToken, key, value, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ListEndpoints",
            GET,
            BiosServicePath.PATH_ENDPOINTS,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            List.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<List> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String paramEndpointType =
                queryParams.getOrDefault(Keywords.ENDPOINT_TYPE, DEFAULT_ENDPOINT_TYPE);

            EndpointType endpointType;
            try {
              endpointType = EndpointType.valueOf(paramEndpointType.toUpperCase());
            } catch (IllegalArgumentException e) {
              logger.warn(
                  "GET {}: Unknown query parameter {} value {} -- using default {}",
                  BiosServicePath.PATH_ENDPOINTS,
                  Keywords.ENDPOINT_TYPE,
                  paramEndpointType,
                  EndpointType.ALL.name());
              endpointType = EndpointType.ALL;
            }

            return v1Handler.getEndpoints(sessionToken, endpointType, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "WriteEndpoint",
            POST,
            BiosServicePath.PATH_ENDPOINTS,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            UpdateEndpointsRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              UpdateEndpointsRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.updateEndpoints(sessionToken, request, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetUpstreamConfig",
            GET,
            BiosServicePath.PATH_UPSTREAM,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            UpstreamConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(ExecutorManager.getSidelineExecutor());
          }

          @Override
          protected CompletableFuture<UpstreamConfig> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.executeInSideline(
                state, () -> v1Handler.getUpstreamConfig(sessionToken, state));
          }
        });

    // We have to keep this until all SDK is upgraded to 1.0.51 or later
    serviceRouter.addHandler(
        new RestHandler<>(
            "GetUpstreamConfigDeprecated",
            GET,
            BiosServicePath.PATH_UPSTREAM_DEPRECATED,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            UpstreamConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(ExecutorManager.getSidelineExecutor());
          }

          @Override
          protected CompletableFuture<UpstreamConfig> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.executeInSideline(
                state, () -> v1Handler.getUpstreamConfig(sessionToken, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "RegisterBiosApps",
            POST,
            BiosServicePath.PATH_APPS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            AppsInfo.class,
            AppsInfo.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<AppsInfo> handle(
              AppsInfo request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.registerBiosApps(sessionToken, request);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetRegisteredBiosApps",
            GET,
            BiosServicePath.PATH_APPS_FOR_TENANT,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            AppsInfo.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<AppsInfo> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);

            return v1Handler.executeInSideline(
                state, () -> v1Handler.getRegisteredBiosAppsInfo(sessionToken, tenantName));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeregisteredBiosApps",
            DELETE,
            BiosServicePath.PATH_APPS_FOR_TENANT,
            NO_CONTENT,
            MediaType.NONE,
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
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);

            return v1Handler.executeInSideline(
                state, () -> v1Handler.deregisterBiosApps(sessionToken, tenantName));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "MaintainKeyspaces",
            POST,
            BiosServicePath.PATH_KEYSPACES,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            MaintainKeyspacesRequest.class,
            TenantKeyspaces.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantKeyspaces> handle(
              MaintainKeyspacesRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.executeInSideline(
                state,
                () -> v1Handler.maintainKeyspaces(sessionToken, state.getMetricsTracer(), request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "MaintainTables",
            POST,
            BiosServicePath.PATH_TABLES,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            MaintainTablesRequest.class,
            TenantTables.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantTables> handle(
              MaintainTablesRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.executeInSideline(
                state,
                () ->
                    v1Handler.maintainTables(
                        sessionToken,
                        state.getMetricsTracer(),
                        request.getAction(),
                        request.getKeyspace(),
                        request.getLimit(),
                        request.getTables()));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "MaintainContext",
            POST,
            BiosServicePath.PATH_SYSADMIN_CONTEXT,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            MaintainContextRequest.class,
            ContextMaintenanceResult.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ContextMaintenanceResult> handle(
              MaintainContextRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);

            return v1Handler.executeInSideline(
                state,
                () ->
                    v1Handler.maintainContext(
                        sessionToken,
                        state.getMetricsTracer(),
                        request.getAction(),
                        request.getTenantName(),
                        request.getContextName(),
                        request.getGcGraceSeconds(),
                        request.getBatchSize()));
          }
        });
  }
}
