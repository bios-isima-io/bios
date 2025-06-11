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

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.isima.bios.server.services.BiosServicePath.FULL_PATH_INFERRED_TAGS;
import static io.isima.bios.server.services.BiosServicePath.PATH_APPENDIX;
import static io.isima.bios.server.services.BiosServicePath.PATH_APPENDIXES;
import static io.isima.bios.server.services.BiosServicePath.PATH_APP_TENANTS;
import static io.isima.bios.server.services.BiosServicePath.PATH_AVAILABLE_METRICS;
import static io.isima.bios.server.services.BiosServicePath.PATH_CONTEXT;
import static io.isima.bios.server.services.BiosServicePath.PATH_CONTEXTS;
import static io.isima.bios.server.services.BiosServicePath.PATH_DISCOVER;
import static io.isima.bios.server.services.BiosServicePath.PATH_EXPORT;
import static io.isima.bios.server.services.BiosServicePath.PATH_EXPORTS;
import static io.isima.bios.server.services.BiosServicePath.PATH_EXPORT_START;
import static io.isima.bios.server.services.BiosServicePath.PATH_EXPORT_STOP;
import static io.isima.bios.server.services.BiosServicePath.PATH_SIGNAL;
import static io.isima.bios.server.services.BiosServicePath.PATH_SIGNALS;
import static io.isima.bios.server.services.BiosServicePath.PATH_SUPPORTED_TAGS;
import static io.isima.bios.server.services.BiosServicePath.PATH_TEACH_BIOS;
import static io.isima.bios.server.services.BiosServicePath.PATH_TEACH_BIOS_DEPRECATED;
import static io.isima.bios.server.services.BiosServicePath.PATH_TENANT;
import static io.isima.bios.server.services.BiosServicePath.PATH_TENANTS;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.PATCH;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.dto.teachbios.LearningData;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AvailableMetrics;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TagsMetadata;
import io.isima.bios.models.TeachBiosResponse;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantAppendixSpec;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UpdateInferredTagsRequest;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.ExportServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.utils.StringUtils;
import io.netty.handler.codec.Headers;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminBiosService extends BiosService {
  public static final Logger logger = LoggerFactory.getLogger(AdminBiosService.class);

  private final EventExecutorGroup adminExecutor =
      new DefaultEventExecutorGroup(
          Bios2Config.adminNumThreads(),
          BiosModules.getExecutorManager().createAdminThreadFactory());

  private final AdminServiceHandler v1AdminHandler = BiosModules.getAdminServiceHandler();
  private final ExportServiceHandler v1ExportHandler = BiosModules.getExportServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateTenant",
            POST,
            PATH_TENANTS,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            TenantConfig.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              TenantConfig tenantConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final Optional<FanRouter<TenantConfig, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);
            final boolean registerApps =
                queryParams != null && Boolean.parseBoolean(queryParams.get("registerApps"));

            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.createTenant(
                        sessionToken,
                        tenantConfig,
                        phase,
                        timestamp,
                        fanRouter,
                        registerApps,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetTenantNames",
            GET,
            PATH_TENANTS,
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
            final var tenantNames = parseQueryParameterList(queryParams, Keywords.NAMES);

            return v1AdminHandler.executeInSideline(
                state, () -> v1AdminHandler.getTenantNames(sessionToken, tenantNames, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetTenant",
            GET,
            PATH_TENANT,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            TenantConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantConfig> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final boolean detail = getBoolean(queryParams, Keywords.DETAIL);
            final boolean includeInferredTags =
                getBoolean(queryParams, Keywords.INCLUDE_INFERRED_TAGS);
            final boolean includeInternal = getBoolean(queryParams, Keywords.INCLUDE_INTERNAL);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.getTenant(
                        sessionToken,
                        tenantName,
                        detail,
                        includeInternal,
                        includeInferredTags,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteTenant",
            DELETE,
            PATH_TENANT,
            NO_CONTENT,
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
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final Optional<FanRouter<Void, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.deleteTenant(
                        sessionToken, tenantName, phase, timestamp, fanRouter, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetAppTenantNames",
            GET,
            PATH_APP_TENANTS,
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
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);

            return v1AdminHandler.executeInSideline(
                state, () -> v1AdminHandler.getAppTenantNames(sessionToken, tenantName, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateSignal",
            POST,
            PATH_SIGNALS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            SignalConfig.class,
            SignalConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<SignalConfig> handle(
              SignalConfig signalConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final Optional<FanRouter<SignalConfig, SignalConfig>> fanRouter =
                createFanRouter(headers, phase, sessionToken, SignalConfig.class);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.createSignal(
                        sessionToken,
                        tenantName,
                        signalConfig,
                        phase,
                        timestamp,
                        fanRouter,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetSignals",
            GET,
            PATH_SIGNALS,
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
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final List<String> signalNames = parseQueryParameterList(queryParams, Keywords.NAMES);
            final boolean detail = getBoolean(queryParams, Keywords.DETAIL);
            final boolean includeInternal = getBoolean(queryParams, Keywords.INCLUDE_INTERNAL);
            final boolean includeInferredTags =
                getBoolean(queryParams, Keywords.INCLUDE_INFERRED_TAGS);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.getSignals(
                        sessionToken,
                        tenantName,
                        signalNames,
                        detail,
                        includeInternal,
                        includeInferredTags,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateSignal",
            POST,
            PATH_SIGNAL,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            SignalConfig.class,
            SignalConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<SignalConfig> handle(
              SignalConfig signalConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final Optional<FanRouter<SignalConfig, SignalConfig>> fanRouter =
                createFanRouter(headers, phase, sessionToken, SignalConfig.class);
            final var skipAuditValidation =
                getBoolean(headers, Keywords.X_BIOS_SKIP_AUDIT_VALIDATION, false);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String signalName = pathParams.get(Keywords.SIGNAL_NAME);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.updateSignal(
                        sessionToken,
                        tenantName,
                        signalName,
                        signalConfig,
                        phase,
                        timestamp,
                        fanRouter,
                        skipAuditValidation,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteSignal",
            DELETE,
            PATH_SIGNAL,
            NO_CONTENT,
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
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final Optional<FanRouter<Void, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String signalName = pathParams.get(Keywords.SIGNAL_NAME);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.deleteSignal(
                        sessionToken, tenantName, signalName, phase, timestamp, fanRouter, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetAvailableMetrics",
            GET,
            PATH_AVAILABLE_METRICS,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            AvailableMetrics.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<AvailableMetrics> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            return v1AdminHandler.executeInSideline(
                state, () -> v1AdminHandler.getAvailableMetrics(sessionToken, tenantName, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateContext",
            POST,
            PATH_CONTEXTS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ContextConfig.class,
            ContextConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(adminExecutor.next());
          }

          @Override
          protected CompletableFuture<ContextConfig> handle(
              ContextConfig contextConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final Optional<FanRouter<SignalConfig, SignalConfig>> createSignalFanRouter;

            // Force setting auditEnabled until we remove the property
            contextConfig.setAuditEnabled(getBoolean(headers, Keywords.X_BIOS_AUDIT_ENABLED, true));

            final String auditSignalName;
            if (contextConfig.getAuditEnabled() == Boolean.TRUE) {
              final var contextName = contextConfig.getName();
              auditSignalName = makeContextAuditSignalName(contextName);
              createSignalFanRouter =
                  createFanRouter(
                      POST,
                      () -> generatePath(PATH_SIGNALS, tenantName),
                      phase,
                      sessionToken,
                      SignalConfig.class);
            } else {
              auditSignalName = null;
              createSignalFanRouter = Optional.empty();
            }

            final Optional<FanRouter<ContextConfig, ContextConfig>> fanRouter =
                createFanRouter(headers, phase, sessionToken, ContextConfig.class);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.createContext(
                        sessionToken,
                        tenantName,
                        contextConfig,
                        phase,
                        timestamp,
                        fanRouter,
                        auditSignalName,
                        createSignalFanRouter,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetContexts",
            GET,
            PATH_CONTEXTS,
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
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final List<String> contextNames = parseQueryParameterList(queryParams, Keywords.NAMES);
            final boolean detail = getBoolean(queryParams, Keywords.DETAIL);
            final boolean includeInternal = getBoolean(queryParams, Keywords.INCLUDE_INTERNAL);
            final boolean includeInferredTags =
                getBoolean(queryParams, Keywords.INCLUDE_INFERRED_TAGS);
            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.getContexts(
                        sessionToken,
                        tenantName,
                        contextNames,
                        detail,
                        includeInternal,
                        includeInferredTags,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateContext",
            POST,
            PATH_CONTEXT,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ContextConfig.class,
            ContextConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ContextConfig> handle(
              ContextConfig contextConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String contextName = pathParams.get(Keywords.CONTEXT_NAME);

            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);

            // Force setting auditEnabled until we remove the property
            if (contextConfig.getAuditEnabled() != Boolean.TRUE) {
              contextConfig.setAuditEnabled(Boolean.TRUE);
            }

            // Creating and updating signal fan routers are used for managing audit signals.
            // Whether and which one they are used is unknown at this point, we create both
            // and pass them to the handler.
            final String auditSignalName = makeContextAuditSignalName(contextName);
            final Optional<FanRouter<SignalConfig, SignalConfig>> createSignalFanRouter =
                createFanRouter(
                    POST,
                    () -> generatePath(PATH_SIGNALS, tenantName),
                    phase,
                    sessionToken,
                    SignalConfig.class);

            final Optional<FanRouter<SignalConfig, SignalConfig>> updateSignalFanRouter =
                createFanRouter(
                    POST,
                    () -> generatePath(PATH_SIGNAL, tenantName, auditSignalName),
                    phase,
                    sessionToken,
                    SignalConfig.class);
            updateSignalFanRouter.ifPresent(
                (fr) -> fr.addParam(Keywords.X_BIOS_SKIP_AUDIT_VALIDATION, "true"));

            final Optional<FanRouter<ContextConfig, ContextConfig>> fanRouter =
                createFanRouter(headers, phase, sessionToken, ContextConfig.class);

            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.updateContext(
                        sessionToken,
                        tenantName,
                        contextName,
                        contextConfig,
                        phase,
                        timestamp,
                        fanRouter,
                        auditSignalName,
                        createSignalFanRouter,
                        updateSignalFanRouter,
                        state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteContext",
            DELETE,
            PATH_CONTEXT,
            NO_CONTENT,
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
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var phase = getRequestPhase(headers);
            final var timestamp = determineTimestamp(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);

            final String contextName = pathParams.get(Keywords.CONTEXT_NAME);
            final Optional<FanRouter<Void, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);

            state.setDelegate(getBoolean(queryParams, "delegate"));
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.deleteContext(
                        sessionToken, tenantName, contextName, phase, timestamp, fanRouter, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetSupportedTags",
            GET,
            PATH_SUPPORTED_TAGS,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            TagsMetadata.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TagsMetadata> handle(
              Void na,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            return v1AdminHandler.executeInSideline(
                state, () -> v1AdminHandler.getSupportedTags(sessionToken));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateInferredTags",
            PATCH,
            FULL_PATH_INFERRED_TAGS,
            NO_CONTENT,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            UpdateInferredTagsRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              UpdateInferredTagsRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            return v1AdminHandler.executeInSideline(
                state, () -> v1AdminHandler.updateInferredTags(sessionToken, request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateExportDestination",
            POST,
            PATH_EXPORTS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ExportDestinationConfig.class,
            ExportDestinationConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ExportDestinationConfig> handle(
              ExportDestinationConfig request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.createExportDestination(
                        sessionToken, state.getMetricsTracer(), tenantName, request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetExportDestination",
            GET,
            PATH_EXPORT,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            ExportDestinationConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ExportDestinationConfig> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String exportDestinationId = pathParams.get(Keywords.STORAGE_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.getExportDestination(
                        sessionToken, state.getMetricsTracer(), tenantName, exportDestinationId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateExportDestination",
            PUT,
            PATH_EXPORT,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            ExportDestinationConfig.class,
            ExportDestinationConfig.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ExportDestinationConfig> handle(
              ExportDestinationConfig request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String exportDestinationId = pathParams.get(Keywords.STORAGE_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.updateExportDestination(
                        sessionToken,
                        state.getMetricsTracer(),
                        tenantName,
                        exportDestinationId,
                        request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteExportDestination",
            DELETE,
            PATH_EXPORT,
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
            final String exportDestinationId = pathParams.get(Keywords.STORAGE_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.deleteExportConfig(
                        sessionToken, state.getMetricsTracer(), tenantName, exportDestinationId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DataExportStart",
            POST,
            PATH_EXPORT_START,
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
            final String exportDestinationId = pathParams.get(Keywords.STORAGE_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.startExport(
                        sessionToken, state.getMetricsTracer(), tenantName, exportDestinationId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DataExportStop",
            POST,
            PATH_EXPORT_STOP,
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
            final String exportDestinationId = pathParams.get(Keywords.STORAGE_NAME);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1ExportHandler.stopExport(
                        sessionToken, state.getMetricsTracer(), tenantName, exportDestinationId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "CreateTenantAppendix",
            POST,
            PATH_APPENDIXES,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            TenantAppendixSpec.class,
            TenantAppendixSpec.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantAppendixSpec> handle(
              TenantAppendixSpec request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String categoryName = pathParams.get(Keywords.CATEGORY);
            final var category = parseCategory(categoryName);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.createAppendix(
                        sessionToken, state.getMetricsTracer(), tenantName, category, request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetTenantAppendix",
            GET,
            PATH_APPENDIX,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            TenantAppendixSpec.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantAppendixSpec> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String categoryName = pathParams.get(Keywords.CATEGORY);
            final var category = parseCategory(categoryName);
            final String entryId = pathParams.get(Keywords.ENTRY_ID);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.getAppendix(
                        sessionToken, state.getMetricsTracer(), tenantName, category, entryId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateTenantAppendix",
            POST,
            PATH_APPENDIX,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            TenantAppendixSpec.class,
            TenantAppendixSpec.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<TenantAppendixSpec> handle(
              TenantAppendixSpec request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String categoryName = pathParams.get(Keywords.CATEGORY);
            final var category = parseCategory(categoryName);
            final String entryId = pathParams.get(Keywords.ENTRY_ID);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.updateAppendix(
                        sessionToken,
                        state.getMetricsTracer(),
                        tenantName,
                        category,
                        entryId,
                        request));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteTenantAppendix",
            DELETE,
            PATH_APPENDIX,
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
            final String categoryName = pathParams.get(Keywords.CATEGORY);
            final var category = parseCategory(categoryName);
            final String entryId = pathParams.get(Keywords.ENTRY_ID);
            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.deleteAppendix(
                        sessionToken, state.getMetricsTracer(), tenantName, category, entryId));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DiscoverImportSubjects",
            GET,
            PATH_DISCOVER,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            ImportSourceSchema.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ImportSourceSchema> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String importSourceId = pathParams.get(Keywords.IMPORT_SOURCE_ID);
            final Integer timeoutSeconds = getInteger(queryParams, Keywords.TIMEOUT, 60);

            return v1AdminHandler.executeInSideline(
                state,
                () ->
                    v1AdminHandler.discoverImportSource(
                        sessionToken,
                        state.getMetricsTracer(),
                        tenantName,
                        importSourceId,
                        timeoutSeconds));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "TeachBios",
            POST,
            PATH_TEACH_BIOS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            LearningData.class,
            TeachBiosResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(ExecutorManager.getSidelineExecutor());
          }

          @Override
          protected CompletableFuture<TeachBiosResponse> handle(
              LearningData learningData,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get("tenantName");

            return v1AdminHandler.executeInSideline(
                state,
                () -> v1AdminHandler.teachBios(sessionToken, tenantName, learningData, state));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "TeachBiosDeprecated",
            POST,
            PATH_TEACH_BIOS_DEPRECATED,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            LearningData.class,
            TeachBiosResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(ExecutorManager.getSidelineExecutor());
          }

          @Override
          protected CompletableFuture<TeachBiosResponse> handle(
              LearningData learningData,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get("tenantName");

            return v1AdminHandler.executeInSideline(
                state,
                () -> v1AdminHandler.teachBios(sessionToken, tenantName, learningData, state));
          }
        });
  }

  /** Utility to make a context's audit signal name. */
  String makeContextAuditSignalName(String contextName) {
    return StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, contextName);
  }

  private String generatePath(String pathTemplate, String... params) {
    return StringUtils.generatePath(pathTemplate, params);
  }

  private TenantAppendixCategory parseCategory(String categoryName) {
    try {
      return TenantAppendixCategory.valueOf(Objects.requireNonNull(categoryName.toUpperCase()));
    } catch (IllegalArgumentException e) {
      throw new CompletionException(
          new InvalidRequestException("Unknown appendix category: " + categoryName));
    }
  }
}
