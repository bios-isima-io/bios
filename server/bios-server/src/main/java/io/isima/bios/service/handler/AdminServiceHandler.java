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

import static io.isima.bios.admin.prerequisites.ContextAudit.makeAuditSignalConfig;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.isima.bios.admin.v1.AdminOption.DRY_RUN;
import static io.isima.bios.admin.v1.AdminOption.SKIP_AUDIT_VALIDATION;
import static io.isima.bios.auth.AllowedRoles.APP_MASTER;
import static io.isima.bios.auth.AllowedRoles.CONTEXT_READ;
import static io.isima.bios.auth.AllowedRoles.SIGNAL_DATA_READ;
import static io.isima.bios.auth.AllowedRoles.SIGNAL_READ;
import static io.isima.bios.auth.AllowedRoles.STREAM_WRITE;
import static io.isima.bios.auth.AllowedRoles.SYSADMIN;
import static io.isima.bios.auth.AllowedRoles.TENANT_WRITE;
import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;
import static io.isima.bios.models.RequestPhase.FINAL;
import static io.isima.bios.models.RequestPhase.INITIAL;
import static java.lang.Boolean.TRUE;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.TenantAppendix;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminOption;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.apps.AppsChangeNotifier;
import io.isima.bios.apps.AppsManager;
import io.isima.bios.audit.AuditConstants;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.bi.teachbios.TeachBios;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.storage.cassandra.CassandraDataStoreUtils;
import io.isima.bios.dto.MaintainKeyspacesRequest;
import io.isima.bios.dto.teachbios.LearningData;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.AdminChangeRequestToSameException;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.StreamAlreadyExistsException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.integrations.SourceSchemaFinder;
import io.isima.bios.integrations.validator.ImportFlowSpecValidator;
import io.isima.bios.integrations.validator.ImportSourceConfigValidator;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.AppsInfo;
import io.isima.bios.models.AvailableMetrics;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextMaintenanceAction;
import io.isima.bios.models.ContextMaintenanceResult;
import io.isima.bios.models.EndpointType;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.MaintenanceAction;
import io.isima.bios.models.NodeType;
import io.isima.bios.models.Operation;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TagsMetadata;
import io.isima.bios.models.TeachBiosResponse;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantAppendixSpec;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.TenantKeyspaces;
import io.isima.bios.models.TenantTables;
import io.isima.bios.models.UpdateEndpointsRequest;
import io.isima.bios.models.UpdateInferredTagsRequest;
import io.isima.bios.models.UpstreamConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.recorder.ControlRequestType;
import io.isima.bios.server.handlers.SessionUtils;
import io.isima.bios.utils.Utils;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminServiceHandler extends ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceHandler.class);

  private final Admin admin;
  private final TenantAppendix tenantAppendix;
  private final SharedConfig sharedConfig;
  private final AppsManager appsManager;
  private final AppsChangeNotifier appsChangeNotifier;
  private final TeachBios teachBios;

  private final SessionUtils sessionUtils = new SessionUtils();

  /** The constructor. */
  public AdminServiceHandler(
      AuditManager auditManager,
      Admin admin,
      AdminInternal tfosAdmin,
      TenantAppendix tenantAppendix,
      Auth auth,
      DataEngine dataEngine,
      OperationMetrics metrics,
      SharedConfig sharedConfig,
      AppsManager appsManager,
      AppsChangeNotifier appsChangeNotifier,
      TeachBios teachBios) {
    super(auditManager, auth, tfosAdmin, dataEngine, metrics);
    this.admin = admin;
    this.tenantAppendix = tenantAppendix;
    this.sharedConfig = sharedConfig;
    this.appsManager = appsManager;
    this.appsChangeNotifier = appsChangeNotifier;
    this.teachBios = teachBios;
  }

  public CompletableFuture<Void> createTenant(
      SessionToken sessionToken,
      TenantConfig tenant,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<TenantConfig, Void>> fanRouter,
      boolean registerApps,
      GenericExecutionState state) {

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.CREATE_TENANT_FINAL
            : ControlRequestType.CREATE_TENANT;

    final var handler =
        new FanRoutingServiceHandler<>(
            sessionToken,
            state.getMetricsTracer(),
            TENANT_SYSTEM,
            "",
            requestType,
            SYSADMIN,
            tenant,
            phase,
            timestamp,
            fanRouter,
            state) {

          @Override
          protected Void handleService(
              UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
              throws TfosException, ApplicationException {

            admin.createTenant(tenant, currentPhase, currentTimestamp, false);
            return null;
          }
        };

    return handler
        .enableAuditLog(AuditOperation.ADD_TENANT, AuditConstants.NA, tenant.getName(), true)
        .executeAsync()
        .thenComposeAsync(
            (none) -> {
              if (registerApps) {
                final var tenantName = tenant.getName();
                try {
                  final var appsInfo = registerBiosApps(tenantName, new AppsInfo());
                  final var keyspace =
                      CassandraDataStoreUtils.generateKeyspaceName(tenantName, timestamp);
                  logger.info(
                      "Registered biOS Apps service; tenant={}, appsInfo={}, keyspace={}",
                      tenantName,
                      appsInfo,
                      keyspace);
                  return appsManager.deploy(appsInfo, keyspace);
                } catch (TfosException | ApplicationException e) {
                  throw new CompletionException(e);
                }
              } else {
                return CompletableFuture.completedFuture(null);
              }
            });
  }

  /**
   * Handles GetTenantNames request.
   *
   * @param sessionToken The session token
   * @param tenantNames Names of the tenants to get
   * @param state Execution state
   * @return CompletableFuture for the tenant configuration.
   */
  public CompletableFuture<List> getTenantNames(
      SessionToken sessionToken, List<String> tenantNames, GenericExecutionState state) {

    final var handler =
        new AsyncServiceHandler<List>(
            sessionToken,
            state.getMetricsTracer(),
            TENANT_SYSTEM,
            "",
            ControlRequestType.GET_TENANT_NAMES,
            SYSADMIN,
            state) {

          @Override
          protected List<TenantConfig> handleService(UserContext userContext) throws TfosException {
            return admin.getTenants(tenantNames, Set.of());
          }
        };

    return handler
        .enableAuditLog(AuditOperation.GET_TENANT_LIST, AuditConstants.NA, tenantNames, false)
        .executeAsync();
  }

  /**
   * Handles GetTenant request.
   *
   * @param sessionToken The session token
   * @param tenantName Name of the tenant to get
   * @param detail If true, the operation returns full contents of sub contents, i.e, signals,
   *     contexts, and users. If false, the operation returns only names of the sub contents.
   * @param includeInternal If true, the operation includes internal sub contents in the reply.
   *     Internal sub content names begin with underscore.
   * @param state Execution state
   * @return CompletableFuture for the tenant configuration object.
   */
  public CompletableFuture<TenantConfig> getTenant(
      SessionToken sessionToken,
      String tenantName,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      GenericExecutionState state) {

    return new AsyncServiceHandler<TenantConfig>(
        sessionToken,
        state.getMetricsTracer(),
        tenantName,
        "",
        ControlRequestType.GET_TENANT_CONFIG,
        AllowedRoles.SIGNAL_READ,
        state) {

      @Override
      protected TenantConfig handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return admin.getTenant(
            tenantName, detail, includeInternal, includeInferredTags, userContext.getPermissions());
      }
    }.enableAuditLog(AuditOperation.GET_TENANT, AuditConstants.NA, AuditConstants.NA, false)
        .executeAsync();
  }

  /**
   * Handles DeleteTenant request.
   *
   * @param sessionToken Session token
   * @param tenantName Name of the tenant to delete
   * @param phase The request phase
   * @param timestamp The request timestamp
   * @param fanRouter Optional fan router used for the initial phase operation.
   * @param state Execution state
   */
  public CompletableFuture<Void> deleteTenant(
      SessionToken sessionToken,
      String tenantName,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<Void, Void>> fanRouter,
      GenericExecutionState state) {

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.DELETE_TENANT_FINAL
            : ControlRequestType.DELETE_TENANT;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        state.getMetricsTracer(),
        TENANT_SYSTEM,
        "",
        requestType,
        SYSADMIN,
        null,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected Void handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        admin.deleteTenant(tenantName, currentPhase, currentTimestamp);
        return null;
      }
    }.enableAuditLog(AuditOperation.DELETE_TENANT, AuditConstants.NA, tenantName, true)
        .executeAsync();
  }

  /** Handles CreateSignal request. */
  public CompletableFuture<SignalConfig> createSignal(
      SessionToken sessionToken,
      String tenantName,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<SignalConfig, SignalConfig>> fanRouter,
      ExecutionState state) {

    final var metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.CREATE_SIGNAL_FINAL
            : ControlRequestType.CREATE_SIGNAL;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        requestType,
        STREAM_WRITE,
        signalConfig,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected SignalConfig handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        final String targetTenant = getTargetTenant(tenantName, userContext);

        final var signalName = signalConfig.getName();
        final Set<String> internalAttributes;
        if (signalName.startsWith(CONTEXT_AUDIT_SIGNAL_PREFIX)) {
          internalAttributes = Set.of(CONTEXT_AUDIT_ATTRIBUTE_OPERATION);
        } else {
          internalAttributes = Set.of();
        }
        return admin.createSignalAllowingInternalAttributes(
            targetTenant, signalConfig, currentPhase, currentTimestamp, internalAttributes);
      }
    }.enableAuditLog(AuditOperation.ADD_SIGNAL_CONFIG, signalConfig.getName(), signalConfig, true)
        .executeAsync();
  }

  /** Handles GetSignals request. */
  public CompletableFuture<List> getSignals(
      SessionToken sessionToken,
      String tenantName,
      List<String> signalNames,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      GenericExecutionState state) {

    return new AsyncServiceHandler<List>(
        sessionToken,
        state.getMetricsTracer(),
        tenantName,
        "",
        ControlRequestType.GET_SIGNALS,
        SIGNAL_READ,
        state) {

      @Override
      protected List<SignalConfig> handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final String targetTenant = getTargetTenant(tenantName, userContext);
        final var signals =
            admin.getSignals(
                targetTenant, detail, includeInternal, includeInferredTags, signalNames);
        return signals;
      }
    }.enableAuditLog(AuditOperation.GET_SIGNAL_LIST, AuditConstants.NA, signalNames, false)
        .executeAsync();
  }

  /** Handles UpdateSignal request. */
  public CompletableFuture<SignalConfig> updateSignal(
      SessionToken sessionToken,
      String tenantName,
      String signalName,
      SignalConfig signalConfig,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<SignalConfig, SignalConfig>> fanRouter,
      boolean skipAuditValidation,
      ExecutionState state) {

    final var metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.UPDATE_SIGNAL_FINAL
            : ControlRequestType.UPDATE_SIGNAL;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        signalName,
        requestType,
        STREAM_WRITE,
        signalConfig,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected SignalConfig handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {

        final var targetTenant = getTargetTenant(tenantName, userContext);

        final var signalName = signalConfig.getName();
        final Set<String> internalAttributes;
        if (signalName.startsWith(CONTEXT_AUDIT_SIGNAL_PREFIX)) {
          internalAttributes = Set.of(CONTEXT_AUDIT_ATTRIBUTE_OPERATION);
        } else {
          internalAttributes = Set.of();
        }

        admin.updateSignalAllowingInternalAttributes(
            targetTenant,
            signalName,
            signalConfig,
            currentPhase,
            currentTimestamp,
            internalAttributes,
            skipAuditValidation ? Set.of(SKIP_AUDIT_VALIDATION) : Set.of());

        // We'll take the signal config from the cache later. Enrichment fill-in values of proper
        // types are available only after the final phase is done. It's premature at this point.
        return null;
      }
    }.enableAuditLog(AuditOperation.UPDATE_SIGNAL_CONFIG, signalName, signalConfig, true)
        .executeAsync()
        .thenApply(
            (none) ->
                ExecutionHelper.supply(
                    () ->
                        admin
                            .getSignals(tenantName, true, true, false, List.of(signalName))
                            .get(0)));
  }

  /** Handles DeleteSignal request. */
  public CompletableFuture<Void> deleteSignal(
      SessionToken sessionToken,
      String tenantName,
      String signalName,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<Void, Void>> fanRouter,
      GenericExecutionState state) {

    final var metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.DELETE_SIGNAL_FINAL
            : ControlRequestType.DELETE_SIGNAL;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        requestType,
        STREAM_WRITE,
        null,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected Void handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        final String targetTenant = getTargetTenant(tenantName, userContext);
        admin.deleteSignal(targetTenant, signalName, currentPhase, currentTimestamp);
        return null;
      }
    }.enableAuditLog(AuditOperation.DELETE_SIGNAL_CONFIG, signalName, null, true).executeAsync();
  }

  /** Returns the metrics available for each attribute of each signal. */
  public CompletableFuture<AvailableMetrics> getAvailableMetrics(
      SessionToken sessionToken, String tenantName, GenericExecutionState state) {
    return new AsyncServiceHandler<AvailableMetrics>(
        sessionToken,
        state.getMetricsTracer(),
        tenantName,
        "",
        ControlRequestType.GET_AVAILABLE_METRICS,
        SIGNAL_READ,
        state) {

      @Override
      protected AvailableMetrics handleService(UserContext userContext) throws TfosException {
        final String targetTenant = getTargetTenant(tenantName, userContext);
        return admin.getAvailableMetrics(targetTenant);
      }
    }.executeAsync();
  }

  public CompletableFuture<ContextConfig> createContext(
      SessionToken sessionToken,
      String tenantName,
      ContextConfig contextConfig,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<ContextConfig, ContextConfig>> fanRouter,
      String auditSignalName,
      Optional<FanRouter<SignalConfig, SignalConfig>> createSignalFanRouter,
      GenericExecutionState state) {

    final var metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.CREATE_CONTEXT_FINAL
            : ControlRequestType.CREATE_CONTEXT;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        requestType,
        STREAM_WRITE,
        contextConfig,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected ContextConfig handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        final var targetTenant = getTargetTenant(tenantName, userContext);
        if (currentPhase == INITIAL && contextConfig.getAuditEnabled() == TRUE) {
          final var auditSignal = makeAuditSignalConfig(contextConfig, auditSignalName, null);
          try {
            callHandlerMethod(
                createSignal(
                    sessionToken,
                    tenantName,
                    auditSignal,
                    currentPhase,
                    currentTimestamp,
                    createSignalFanRouter,
                    state));
          } catch (StreamAlreadyExistsException e) {
            // This is fine, the AdminInternal component will validate the existing signal later.
          }
        }
        return admin.createContext(targetTenant, contextConfig, currentPhase, currentTimestamp);
      }
    }.enableAuditLog(
            AuditOperation.ADD_CONTEXT_CONFIG, contextConfig.getName(), contextConfig, true)
        .executeAsync();
  }

  public CompletableFuture<List> getContexts(
      SessionToken sessionToken,
      String tenantName,
      List<String> contextNames,
      boolean detail,
      boolean includeInternal,
      boolean includeInferredTags,
      GenericExecutionState state) {

    return new AsyncServiceHandler<List>(
        sessionToken,
        state.getMetricsTracer(),
        tenantName,
        "",
        ControlRequestType.GET_CONTEXTS,
        CONTEXT_READ,
        state) {
      @Override
      protected List<ContextConfig> handleService(UserContext userContext) throws TfosException {
        return admin.getContexts(
            tenantName, detail, includeInternal, includeInferredTags, contextNames);
      }
    }.enableAuditLog(AuditOperation.GET_CONTEXT_LIST, AuditConstants.NA, contextNames, false)
        .executeAsync();
  }

  public CompletableFuture<ContextConfig> updateContext(
      SessionToken sessionToken,
      String tenantName,
      String contextName,
      ContextConfig contextConfig,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<ContextConfig, ContextConfig>> fanRouter,
      String specifiedAuditSignalName,
      Optional<FanRouter<SignalConfig, SignalConfig>> createSignalFanRouter,
      Optional<FanRouter<SignalConfig, SignalConfig>> updateSignalFanRouter,
      ExecutionState state) {

    final var metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.UPDATE_CONTEXT_FINAL
            : ControlRequestType.UPDATE_CONTEXT;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        contextName,
        requestType,
        STREAM_WRITE,
        contextConfig,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected ContextConfig handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        final var targetTenant = getTargetTenant(tenantName, userContext);

        if (currentPhase == INITIAL) {
          // Create or modify audit signal if audit is enabled
          SignalConfig origAuditSignal = getSignal(tenantName, specifiedAuditSignalName);
          // Use the original cases if possible
          final String auditSignalName =
              origAuditSignal != null ? origAuditSignal.getName() : specifiedAuditSignalName;
          final var auditSignal =
              makeAuditSignalConfig(contextConfig, auditSignalName, origAuditSignal);

          final boolean auditEnabled = TRUE.equals(contextConfig.getAuditEnabled());

          if (auditSignal != null || auditEnabled) {
            // We validate the request but do not actually change the context yet to abort on
            // any violation before creating / modifying audit signal
            admin.updateContext(
                targetTenant,
                contextName,
                contextConfig,
                currentPhase,
                currentTimestamp,
                Set.of(SKIP_AUDIT_VALIDATION, DRY_RUN));
          }

          // Adding or modifying the audit signal happens before the context updates.
          // They become inconsistent temporarily at this step. We'll skip audit-and-context
          // consistency check to avoid operation failure.
          final boolean skipAuditValidation = true;

          if (origAuditSignal != null) {
            try {
              callHandlerMethod(
                  updateSignal(
                      sessionToken,
                      tenantName,
                      specifiedAuditSignalName,
                      auditSignal,
                      currentPhase,
                      timestamp,
                      updateSignalFanRouter,
                      skipAuditValidation,
                      state));
            } catch (AdminChangeRequestToSameException e) {
              // This is fine, will ignore silently
            }
          } else if (auditEnabled) {
            callHandlerMethod(
                createSignal(
                    sessionToken,
                    tenantName,
                    auditSignal,
                    currentPhase,
                    timestamp,
                    createSignalFanRouter,
                    state));
          }
        }

        return admin.updateContext(
            targetTenant, contextName, contextConfig, currentPhase, currentTimestamp, Set.of());
      }
    }.enableAuditLog(AuditOperation.UPDATE_CONTEXT_CONFIG, contextName, contextConfig, true)
        .executeAsync();
  }

  public CompletableFuture<Void> deleteContext(
      SessionToken sessionToken,
      String tenantName,
      String contextName,
      RequestPhase phase,
      Long timestamp,
      Optional<FanRouter<Void, Void>> fanRouter,
      GenericExecutionState state) {

    final OperationMetricsTracer metricsTracer = state.getMetricsTracer();

    final var requestType =
        (phase == FINAL)
            ? ControlRequestType.DELETE_CONTEXT_FINAL
            : ControlRequestType.DELETE_CONTEXT;

    return new FanRoutingServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        requestType,
        STREAM_WRITE,
        null,
        phase,
        timestamp,
        fanRouter,
        state) {

      @Override
      protected Void handleService(
          UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
          throws TfosException, ApplicationException {
        final var targetTenant = getTargetTenant(tenantName, userContext);
        admin.deleteContext(targetTenant, contextName, currentPhase, currentTimestamp);
        return null;
      }
    }.enableAuditLog(AuditOperation.DELETE_CONTEXT_CONFIG, contextName, null, true).executeAsync();
  }

  /**
   * Creates a tenant appendix.
   *
   * <p>Allowed roles change by the appendix cateogry:
   *
   * <dl>
   *   <dt>IMPORT_SOURCES, IMPORT_DESTINATIONS
   *   <dd>AdminInternal
   *   <dt>IMPORT_FLOW_SPEC, IMPORT_DATA_MAPPINGS, IMPORT_DATA_PROCESSORS
   *   <dd>Machine Learning Engineers
   * </dl>
   *
   * @param sessionToken Session token
   * @param tenantName Tenant name
   * @param category Appendix category
   * @param appendixSpec Appendix specifier
   * @return Completable future for the created appendix
   */
  public CompletableFuture<TenantAppendixSpec> createAppendix(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      TenantAppendixCategory category,
      TenantAppendixSpec<?> appendixSpec) {

    return new AsyncServiceHandler<TenantAppendixSpec>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.CREATE_APPENDIX,
        resolveAllowedRolesForAppendix(category, false),
        null) {

      @Override
      protected TenantAppendixSpec<?> handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, true, true, false, List.of());
        final EntityId tenantId = new EntityId(tenantConfig);

        // TODO(Naoki): Cloning the appendixSpec is better to avoid modifying the source object
        final String entryId =
            appendixSpec.getEntryId() != null
                ? appendixSpec.getEntryId()
                : UUID.randomUUID().toString();
        appendixSpec.setEntryId(entryId);
        appendixSpec.commitEntryId();

        validateSpec(tenantConfig, category, appendixSpec);

        tenantAppendix.createEntry(tenantId, category, entryId, appendixSpec.getContent());
        if (category == TenantAppendixCategory.IMPORT_FLOW_SPECS) {
          appsChangeNotifier.notifyFlowChange(
              tenantConfig, (ImportFlowConfig) appendixSpec.getContent());
        } else if (category == TenantAppendixCategory.IMPORT_SOURCES) {
          appsChangeNotifier.notifySourceChange(
              tenantConfig, (ImportSourceConfig) appendixSpec.getContent());
        }
        return appendixSpec;
      }
    }.executeAsync();
  }

  /**
   * Gets a tenant appendix.
   *
   * <p>Allowed roles change by the appendix cateogry:
   *
   * <dl>
   *   <dt>IMPORT_SOURCES, IMPORT_DESTINATIONS, IMPORT_FLOW_SPEC, IMPORT_DATA_MAPPINGS,
   *       IMPORT_DATA_PROCESSORS
   *   <dd>Signal readers
   * </dl>
   *
   * @param sessionToken Session token
   * @param tenantName Tenant name
   * @param entryId Entry ID
   * @return Completable future for the retrieved appendix
   */
  public CompletableFuture<TenantAppendixSpec> getAppendix(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      TenantAppendixCategory category,
      String entryId) {

    return new AsyncServiceHandler<TenantAppendixSpec>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.GET_APPENDIX,
        resolveAllowedRolesForAppendix(category, true),
        null) {

      @Override
      protected TenantAppendixSpec<?> handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
        final EntityId tenantId = new EntityId(tenantConfig);

        final var entry =
            tenantAppendix.getEntry(tenantId, category, entryId, category.getConfigClass());

        return category.supplyAppendixSpec(entryId, entry);
      }
    }.executeAsync();
  }

  /**
   * Modifies a tenant appendix.
   *
   * <p>Allowed roles change by the appendix cateogry:
   *
   * <dl>
   *   <dt>IMPORT_SOURCES, IMPORT_DESTINATIONS
   *   <dd>AdminInternal
   *   <dt>IMPORT_FLOW_SPEC, IMPORT_DATA_MAPPINGS, IMPORT_DATA_PROCESSORS
   *   <dd>Machine Learning Engineers
   * </dl>
   *
   * @param sessionToken Session token
   * @param tenantName Tenant name
   * @param category Appendix category
   * @param entryId Entry ID
   * @param appendixSpec Appendix specifier
   * @return Completable future for the modified appendix
   */
  public CompletableFuture<TenantAppendixSpec> updateAppendix(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      TenantAppendixCategory category,
      String entryId,
      TenantAppendixSpec<?> appendixSpec) {

    return new AsyncServiceHandler<TenantAppendixSpec>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.UPDATE_APPENDIX,
        resolveAllowedRolesForAppendix(category, false),
        null) {

      @Override
      protected TenantAppendixSpec<?> handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, true, true, false, List.of());
        final EntityId tenantId = new EntityId(tenantConfig);

        // TODO(Naoki): Cloning the appendixSpec is better to avoid modifying the source object
        appendixSpec.setEntryId(entryId);
        appendixSpec.commitEntryId();

        validateSpec(tenantConfig, category, appendixSpec);

        tenantAppendix.updateEntry(tenantId, category, entryId, appendixSpec.getContent());
        if (category == TenantAppendixCategory.IMPORT_FLOW_SPECS) {
          appsChangeNotifier.notifyFlowChange(
              tenantConfig, (ImportFlowConfig) appendixSpec.getContent());
        } else if (category == TenantAppendixCategory.IMPORT_SOURCES) {
          appsChangeNotifier.notifySourceChange(
              tenantConfig, (ImportSourceConfig) appendixSpec.getContent());
        }
        return appendixSpec;
      }
    }.executeAsync();
  }

  /**
   * Deletes a tenant appendix.
   *
   * <p>Allowed roles change by the appendix cateogry:
   *
   * <dl>
   *   <dt>IMPORT_SOURCES, IMPORT_DESTINATIONS
   *   <dd>AdminInternal
   *   <dt>IMPORT_FLOW_SPEC, IMPORT_DATA_MAPPINGS, IMPORT_DATA_PROCESSORS
   *   <dd>Machine Learning Engineers
   * </dl>
   *
   * @param sessionToken Session token
   * @param tenantName Tenant name
   * @param category Appendix category
   * @param entryId Entry ID
   * @return Completable future for the completion
   */
  public CompletableFuture<Void> deleteAppendix(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      TenantAppendixCategory category,
      String entryId) {

    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.DELETE_APPENDIX,
        resolveAllowedRolesForAppendix(category, false),
        null) {

      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, true, false, false, List.of());
        final EntityId tenantId = new EntityId(tenantConfig);

        ImportFlowConfig flowSpec = null;
        if (category == TenantAppendixCategory.IMPORT_FLOW_SPECS) {
          flowSpec = tenantAppendix.getEntry(tenantId, category, entryId, ImportFlowConfig.class);
        }
        ImportSourceConfig sourceConfig = null;
        if (category == TenantAppendixCategory.IMPORT_SOURCES) {
          sourceConfig =
              tenantAppendix.getEntry(tenantId, category, entryId, ImportSourceConfig.class);
        }
        tenantAppendix.deleteEntry(tenantId, category, entryId);
        if (category == TenantAppendixCategory.IMPORT_FLOW_SPECS) {
          appsChangeNotifier.notifyFlowChange(tenantConfig, flowSpec);
        } else if (category == TenantAppendixCategory.IMPORT_SOURCES) {
          appsChangeNotifier.notifySourceChange(tenantConfig, sourceConfig);
        }
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<ImportSourceSchema> discoverImportSource(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String importSourceId,
      Integer timeoutSeconds) {
    Objects.requireNonNull(timeoutSeconds);
    return new AsyncServiceHandler<ImportSourceSchema>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.DISCOVER_IMPORT_SOURCE,
        SIGNAL_DATA_READ,
        null) {
      @Override
      protected ImportSourceSchema handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
        final EntityId tenantId = new EntityId(tenantConfig);
        final var category = TenantAppendixCategory.IMPORT_SOURCES;

        final var entry =
            tenantAppendix.getEntry(tenantId, category, importSourceId, category.getConfigClass());

        final ImportSourceConfig config =
            (ImportSourceConfig) category.supplyAppendixSpec(importSourceId, entry).getContent();
        logger.info("source={}", config);
        return SourceSchemaFinder.newFinder(tenantName, config).find(timeoutSeconds);
      }
    }.executeAsync();
  }

  public CompletableFuture<TeachBiosResponse> teachBios(
      SessionToken sessionToken,
      String tenantName,
      LearningData learningData,
      GenericExecutionState state) {
    return new AsyncServiceHandler<TeachBiosResponse>(
        sessionToken,
        state.getMetricsTracer(),
        tenantName,
        "",
        ControlRequestType.TEACH_BIOS,
        STREAM_WRITE,
        state) {
      @Override
      protected TeachBiosResponse handleService(UserContext userContext) throws TfosException {
        return teachBios.learn(learningData);
      }
    }.executeAsync();
  }

  /**
   * Gets a property and return it as a base64-encoded string.
   *
   * <p>The method returns an empty string if the property is not found.
   *
   * @param sessionToken The session token
   * @param key Property key
   * @param state Execution state
   * @return Completable future for the property value encoded by base64
   */
  public CompletableFuture<String> getProperty(
      SessionToken sessionToken, String key, GenericExecutionState state) {
    return new AsyncServiceHandler<String>(
        sessionToken,
        state.getMetricsTracer(),
        "",
        "",
        ControlRequestType.GET_PROPERTY,
        AllowedRoles.SYSADMIN,
        state) {
      @Override
      protected String handleService(UserContext userContext) {
        final String rawValue = sharedConfig.getProperty(key);
        return Base64.getEncoder().encodeToString(rawValue.getBytes());
      }
    }.executeAsync();
  }

  /**
   * Sets a property.
   *
   * @param sessionToken The session token
   * @param key Property key
   * @param value Base64-encoded property value
   * @param state Execution state
   * @return Completable future for the completion.
   */
  public CompletableFuture<Void> setProperty(
      SessionToken sessionToken, String key, String value, GenericExecutionState state) {
    return new AsyncServiceHandler<Void>(
        sessionToken,
        state.getMetricsTracer(),
        "",
        "",
        ControlRequestType.SET_PROPERTY,
        AllowedRoles.SYSADMIN,
        state) {
      @Override
      protected Void handleService(UserContext userContext) throws ApplicationException {
        try {
          final String decodedValue = new String(Base64.getDecoder().decode(value));
          sharedConfig.setProperty(key, decodedValue);
        } catch (IllegalArgumentException e) {
          throw new CompletionException(
              new InvalidValueException("Invalid setProperty payload: " + e.getMessage()));
        }
        return null;
      }
    }.executeAsync();
  }

  /**
   * Updates attributes' inferred tags based on the inference engine's findings. This is an internal
   * system endpoint used for fan-routing inferred tag updates from the server executing inference
   * to all other servers.
   */
  public CompletableFuture<Void> updateInferredTags(
      SessionToken sessionToken, UpdateInferredTagsRequest request) {
    return new AsyncServiceHandler<Void>(
        sessionToken, null, "", "", null, AllowedRoles.SYSADMIN, null) {
      @Override
      protected Void handleService(UserContext userContext) throws ApplicationException {
        admin.updateInferredTags(
            request.getTenant(), request.getStream(), FINAL, request.getAttributes());
        return null;
      }
    }.executeAsync();
  }

  /** Returns all the tags metadata supported by biOS. */
  public CompletableFuture<TagsMetadata> getSupportedTags(SessionToken sessionToken) {
    return new AsyncServiceHandler<TagsMetadata>(
        sessionToken, null, "", "", null, AllowedRoles.SIGNAL_READ, null) {
      @Override
      protected TagsMetadata handleService(UserContext userContext) {
        return admin.getSupportedTags();
      }
    }.executeAsync();
  }

  /**
   * Registers a bios-apps service.
   *
   * @param sessionToken Authentication parameters
   * @param appsInfo Apps information. Tenant name and bios-apps host are required. The control and
   *     webhook port are optional. If the port numbers are not specified in appsInfo, the server
   *     assigns next available port numbers and include them in the returning AppsInfo.
   * @return Registered apps info
   */
  public CompletableFuture<AppsInfo> registerBiosApps(
      SessionToken sessionToken, AppsInfo appsInfo) {
    return new AsyncServiceHandler<AppsInfo>(
        sessionToken, null, "", "", null, AllowedRoles.SYSADMIN, null) {
      @Override
      protected AppsInfo handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final String tenantName = appsInfo.getTenantName();
        return registerBiosApps(tenantName, appsInfo);
      }
    }.executeAsync();
  }

  public CompletableFuture<AppsInfo> getRegisteredBiosAppsInfo(
      SessionToken sessionToken, String tenantName) {
    return new AsyncServiceHandler<AppsInfo>(
        sessionToken, null, "", "", null, AllowedRoles.SYSADMIN, null) {
      @Override
      protected AppsInfo handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
        final var tenantId = new EntityId(tenantConfig);
        return appsManager.getAppsInfo(tenantId);
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> deregisterBiosApps(SessionToken sessionToken, String tenantName) {
    return new AsyncServiceHandler<Void>(
        sessionToken, null, "", "", null, AllowedRoles.SYSADMIN, null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
        final var tenantId = new EntityId(tenantConfig);
        appsManager.deregister(tenantId);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<List> getAppTenantNames(
      SessionToken sessionToken, String tenantName, GenericExecutionState state) {

    final var handler =
        new AsyncServiceHandler<List>(
            sessionToken,
            state.getMetricsTracer(),
            tenantName,
            "",
            ControlRequestType.GET_APP_TENANT_NAMES,
            APP_MASTER,
            state) {

          @Override
          protected List<String> handleService(UserContext userContext) throws TfosException {
            final var tenants = admin.getTenants(null, Set.of(AdminOption.DETAIL));
            return tenants.stream()
                .filter((tenant) -> tenantName.equalsIgnoreCase(tenant.getAppMaster()))
                .map((tenant) -> tenant.getName())
                .collect(Collectors.toList());
          }
        };

    return handler.executeAsync();
  }

  // Utilities /////////////////////////////////////////////////////////////////////////////

  private String getTargetTenant(String tenantName, UserContext userContext) {
    if (tenantName != null && !tenantName.isBlank()) {
      return tenantName;
    }
    return userContext.getTenant();
  }

  /**
   * Gets a signal from tenant and signal name.
   *
   * <p>This method does not throw an exception when the target signal does not exist. Null is
   * returned instead.
   *
   * @param tenantName target tenant
   * @param signalName target signal
   * @return Found signal or null
   * @throws NoSuchTenantException thrown to indicate that the target tenant does not exist
   * @throws InvalidRequestException thrown to indicate that the request is invalid
   */
  private SignalConfig getSignal(String tenantName, String signalName)
      throws NoSuchTenantException, InvalidRequestException {
    try {
      return admin.getSignals(tenantName, true, false, true, List.of(signalName)).get(0);
    } catch (NoSuchStreamException e) {
      return null;
    }
  }

  /** Resolves allowed roles for an appendix operation. */
  private List<Permission> resolveAllowedRolesForAppendix(
      TenantAppendixCategory category, boolean isReadOnly) {
    switch (category) {
      case IMPORT_SOURCES:
      case IMPORT_DESTINATIONS:
        return isReadOnly ? SIGNAL_READ : TENANT_WRITE;
      case IMPORT_FLOW_SPECS:
      case IMPORT_DATA_PROCESSORS:
        return isReadOnly ? SIGNAL_READ : STREAM_WRITE;
      default:
        throw new UnsupportedOperationException("Unsupported category " + category);
    }
  }

  private void validateSpec(
      TenantConfig tenantConfig,
      TenantAppendixCategory category,
      TenantAppendixSpec<?> appendixSpec)
      throws TfosException {
    try {
      validateSpecCore(tenantConfig, category, appendixSpec);
    } catch (ValidatorException e) {
      switch (e.getErrorType()) {
        case CONSTRAINT_VIOLATION:
          throw new ConstraintViolationException(e);
        case INVALID_VALUE:
          throw new InvalidValueException(e);
        case NOT_IMPLEMENTED:
          throw new NotImplementedException(e);
        default:
          throw new TfosException(GenericError.INVALID_REQUEST, e.getMessage(), e);
      }
    }
  }

  private void validateSpecCore(
      TenantConfig tenantConfig,
      TenantAppendixCategory category,
      TenantAppendixSpec<?> appendixSpec)
      throws ValidatorException {
    switch (category) {
      case IMPORT_SOURCES:
        ImportSourceConfigValidator.newValidator((ImportSourceConfig) appendixSpec.getContent())
            .validate();
        break;
      case IMPORT_FLOW_SPECS:
        ImportFlowSpecValidator.newValidator(
                tenantConfig, (ImportFlowConfig) appendixSpec.getContent())
            .validate();
      default:
        // validator is not implemented yet
    }
  }

  /**
   * Registers a BiosApps service.
   *
   * @param tenantName Name of the tenant to register an apps service
   * @param appsInfo Apps info to request. Specify an empty object to let server allocate resources.
   * @return Registered biosApps info.
   * @throws TfosException when a user error happens.
   * @throws ApplicationException thrown to indicate that an unexpected error happened
   */
  private AppsInfo registerBiosApps(String tenantName, AppsInfo appsInfo)
      throws TfosException, ApplicationException {
    final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
    final var tenantId = new EntityId(tenantConfig);
    return appsManager.register(tenantId, appsInfo);
  }

  public CompletableFuture<TenantKeyspaces> maintainKeyspaces(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      MaintainKeyspacesRequest request) {
    return new AsyncServiceHandler<TenantKeyspaces>(
        sessionToken,
        metricsTracer,
        "",
        "",
        ControlRequestType.MAINTAIN_KEYSPACES,
        SYSADMIN,
        null) {

      @Override
      protected TenantKeyspaces handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        boolean includeDropped =
            request.getIncludeDroppedKeyspaces() != null && request.getIncludeDroppedKeyspaces();
        return dataEngine.maintainTenantKeyspaces(
            request.getAction(),
            includeDropped,
            Optional.ofNullable(request.getLimit()),
            Optional.ofNullable(request.getKeyspaces()),
            Optional.ofNullable(request.getTenants()));
      }
    }.executeAsync();
  }

  public CompletableFuture<TenantTables> maintainTables(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      MaintenanceAction action,
      String keyspace,
      Integer limit,
      List<String> tables) {
    return new AsyncServiceHandler<TenantTables>(
        sessionToken, metricsTracer, "", "", ControlRequestType.MAINTAIN_TABLES, SYSADMIN, null) {

      @Override
      protected TenantTables handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return dataEngine.maintainTenantTables(action, keyspace, limit, tables);
      }
    }.executeAsync();
  }

  public CompletableFuture<ContextMaintenanceResult> maintainContext(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      ContextMaintenanceAction action,
      String tenantName,
      String contextName,
      Integer gcGraceSeconds,
      Integer batchSize) {
    return new AsyncServiceHandler<ContextMaintenanceResult>(
        sessionToken, metricsTracer, "", "", ControlRequestType.MAINTAIN_CONTEXT, SYSADMIN, null) {

      @Override
      protected ContextMaintenanceResult handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return dataEngine.maintainContext(
            action, tenantName, contextName, gcGraceSeconds, batchSize);
      }
    }.executeAsync();
  }

  public CompletableFuture<List> getEndpoints(
      SessionToken sessionToken, EndpointType endpointType, GenericExecutionState state) {
    return new AsyncServiceHandler<List>(
        sessionToken,
        state.getMetricsTracer(),
        "",
        "",
        ControlRequestType.GET_ENDPOINTS,
        AllowedRoles.SYSADMIN,
        state) {
      @Override
      protected List handleService(UserContext userContext) throws NotImplementedException {
        switch (endpointType) {
          case ALL:
            return sharedConfig.getEndpoints();
          case CONTEXT:
            return sharedConfig.getContextEndpoints();
          default:
            throw new NotImplementedException("Endpoint type " + endpointType.name());
        }
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> updateEndpoints(
      SessionToken sessionToken, UpdateEndpointsRequest request, GenericExecutionState state) {
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(
            (none) -> updateEndpoints(sessionToken, state.getMetricsTracer(), request),
            getExecutor());
  }

  public CompletableFuture<Void> updateEndpoints(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      UpdateEndpointsRequest request) {

    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        "",
        "",
        ControlRequestType.UPDATE_ENDPOINTS,
        AllowedRoles.SYSADMIN,
        null) {
      @Override
      protected Void handleService(UserContext userContext) throws ApplicationException {
        final String endpoint = request.getEndpoint();
        final NodeType nodeType = request.getNodeType();

        switch (request.getOperation()) {
          case ADD:
            final String nodeName = Utils.getNodeName();
            logger.info(
                "Registering node {} type {} with endpoint {}", nodeName, nodeType, endpoint);
            sharedConfig.registerNode(endpoint, nodeName, nodeType);
            break;
          case REMOVE:
            logger.info("Unregistering node, endpoint {}", endpoint);
            sharedConfig.unregisterNode(endpoint);
            break;
          default:
            // nothing to do
        }
        return null;
      }
    }.enableAuditLog(
            request.getOperation() == Operation.ADD
                ? AuditOperation.ADD_ENDPOINTS
                : AuditOperation.REMOVE_ENDPOINT,
            AuditConstants.NA,
            "",
            true)
        .executeAsync();
  }

  public CompletableFuture<UpstreamConfig> getUpstreamConfig(
      SessionToken sessionToken, GenericExecutionState state) {

    final var future = new CompletableFuture<UpstreamConfig>();
    sessionUtils.validateSessionToken(
        sessionToken,
        "",
        AllowedRoles.ANY,
        state,
        (userContext) -> {
          state.setUserContext(userContext);
          sharedConfig
              .getUpstreamConfigAsync(state)
              .thenAccept(future::complete)
              .exceptionally(
                  (t) -> {
                    future.completeExceptionally(t);
                    return null;
                  });
        },
        future::completeExceptionally);
    return future;
  }
}
