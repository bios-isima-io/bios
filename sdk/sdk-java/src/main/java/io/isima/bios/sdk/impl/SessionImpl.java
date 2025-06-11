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
package io.isima.bios.sdk.impl;

import static io.isima.bios.sdk.metrics.MetricsController.begin;

import io.isima.bios.codec.proto.isql.BasicBuilderValidator;
import io.isima.bios.codec.proto.isql.ProtoMetricBuilder;
import io.isima.bios.codec.proto.isql.ProtoOrderByBuilder;
import io.isima.bios.codec.proto.isql.ProtoSelectQueryBuilder;
import io.isima.bios.codec.proto.isql.ProtoSlidingWindowBuilder;
import io.isima.bios.codec.proto.isql.ProtoTumblingWindowBuilder;
import io.isima.bios.codec.proto.wrappers.ProtoSelectQuery;
import io.isima.bios.codec.proto.wrappers.ProtoSelectRequestWriter;
import io.isima.bios.dto.ContextResponseWrapper;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.MultiGetSpec;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.bulk.InsertBulkSuccessResponse;
import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AtomicOperationSpec;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.GenericMetric;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.InsertResponse;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.MultiSelectRequest;
import io.isima.bios.models.Record;
import io.isima.bios.models.SessionInfo;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.VoidISqlResponse;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.sdk.GetStreamOption;
import io.isima.bios.sdk.ISql;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.csdk.BiosClient;
import io.isima.bios.sdk.csdk.CSdkOperationId;
import io.isima.bios.sdk.csdk.InsertBulkExecutor;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.exceptions.IngestBulkFailedException;
import io.isima.bios.sdk.metrics.MetricsController.MetricsHandle;
import io.isima.bios.sdk.metrics.MetricsController.OperationType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class SessionImpl extends BiosClient implements Session {

  private static final int MAXIMUM_NUM_BULK_INSERT_EVENTS = 100000;

  /** Client caches the stream specific configuration on a need basis. */
  private final Map<String, SignalAttachment> signalAttachments;

  private final TenantAppendixOperator tenantAppendixOperator;

  private SessionInfo sessionInfo;

  public SessionImpl() {
    super();

    signalAttachments = new ConcurrentHashMap<>();
    tenantAppendixOperator = new TenantAppendixOperator(this);
  }

  public void start(
      String host,
      int port,
      String cafile,
      String email,
      String password,
      String appName,
      AppType appType)
      throws BiosClientException {
    BiosClientUtils.wait(
        startAsync(host, port, cafile, email, password, appName, appType).toCompletableFuture());
  }

  public CompletionStage<Void> startAsync(
      String host,
      int port,
      String cafile,
      String email,
      String password,
      String appName,
      AppType appType) {
    return connectAsync(host, port, cafile)
        .thenCompose((none) -> loginAsync(email, password, appName, appType));
  }

  private CompletableFuture<Void> loginAsync(
      String email, String password, String appName, AppType appType) {
    try {
      return makeLoginCaller(email, password, appName, appType)
          .invokeAsync()
          .thenAccept(
              (sessionInfo) -> {
                this.sessionInfo = sessionInfo;
                setTenantName(sessionInfo.getTenantName());
              });
    } catch (BiosClientException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public ISqlResponse execute(Statement statement) throws BiosClientException {
    return BiosClientUtils.wait(runExecuteAsync(statement, false));
  }

  @Override
  public CompletionStage<ISqlResponse> executeAsync(Statement statement) {
    return runExecuteAsync(statement, false);
  }

  @Override
  public List<ISqlResponse> multiExecute(Statement... statements) throws BiosClientException {
    return BiosClientUtils.wait(multiExecuteAsync(statements).toCompletableFuture());
  }

  @Override
  public CompletionStage<List<ISqlResponse>> multiExecuteAsync(Statement... statements) {
    try {
      Objects.requireNonNull(statements);
      if (statements.length == 0) {
        throw new BiosClientException(
            BiosClientError.INVALID_REQUEST, "At least one statement must be specified");
      }
      checkLoggedIn();
      if (statements[0] instanceof ISql.SignalSelect) {
        final var selectStatements = new ISql.SignalSelect[statements.length];
        for (int i = 0; i < statements.length; ++i) {
          if (!(statements[i] instanceof ISql.SignalSelect)) {
            throw new BiosClientException(
                BiosClientError.INVALID_REQUEST, "All statements should be of the same type");
          }
          selectStatements[i] = (ISql.SignalSelect) statements[i];
        }
        return signalSelectCore(selectStatements);
      } else if (statements[0] instanceof ISql.ContextSelect) {
        final var selectStatements = new ISql.ContextSelect[statements.length];
        for (int i = 0; i < statements.length; ++i) {
          if (!(statements[i] instanceof ISql.ContextSelect)) {
            throw new BiosClientException(
                BiosClientError.INVALID_REQUEST, "All statements should be of the same type");
          }
          selectStatements[i] = (ISql.ContextSelect) statements[i];
        }
        return contextSelectCore(selectStatements);
      } else {
        throw new BiosClientException(
            BiosClientError.INVALID_REQUEST,
            String.format(
                "Statement unsupported for multiExecute (%s)", statements[0].getStatementType()));
      }
    } catch (BiosClientException e) {
      return CompletableFuture.failedStage(e);
    }
  }

  @Override
  public TenantConfig getTenant(boolean detail, boolean includeInternal)
      throws BiosClientException {
    return getTenant(sessionInfo.getTenantName(), detail, includeInternal);
  }

  @Override
  public TenantConfig getTenant(String tenantName, boolean detail, boolean includeInternal)
      throws BiosClientException {
    Objects.requireNonNull(tenantName);
    checkLoggedIn();
    String[] resources = new String[] {TENANT_NAME_KEY, tenantName};
    String[] options =
        new String[] {
          DETAIL_KEY, String.valueOf(detail),
          INCLUDE_INTERNAL_KEY, String.valueOf(includeInternal)
        };
    return makeGenericMethodCaller(
            CSdkOperationId.GET_TENANT_BIOS, null, resources, options, null, TenantConfig.class)
        .invoke();
  }

  @Override
  public SignalConfig createSignal(SignalConfig signalConfig) throws BiosClientException {
    final var resources = new String[2];
    resources[0] = "tenantName";
    resources[1] = sessionInfo.getTenantName();
    return makeSimpleMethodCaller(
            CSdkOperationId.CREATE_SIGNAL,
            null,
            Objects.requireNonNull(signalConfig),
            SignalConfig.class)
        .invoke();
  }

  @Override
  public List<SignalConfig> getSignals(
      Collection<String> names, Collection<GetStreamOption> options) throws BiosClientException {
    final String[] methodOptions = makeGetStreamsOptions(names, options);

    return List.of(
        makeGenericMethodCaller(
                CSdkOperationId.GET_SIGNALS, null, null, methodOptions, null, SignalConfig[].class)
            .invoke());
  }

  @Override
  public SignalConfig getSignal(String signalName, Collection<GetStreamOption> options)
      throws BiosClientException {
    final var methodOptions = new HashSet<>(options);
    methodOptions.add(GetStreamOption.DETAIL);
    methodOptions.add(GetStreamOption.INCLUDE_INTERNAL);
    final var signals = getSignals(List.of(signalName), methodOptions);
    return signals.get(0);
  }

  @Override
  public SignalConfig updateSignal(String signalName, SignalConfig signalConfig)
      throws BiosClientException {
    return makeSimpleMethodCaller(
            CSdkOperationId.UPDATE_SIGNAL,
            signalName,
            Objects.requireNonNull(signalConfig),
            SignalConfig.class)
        .invoke();
  }

  @Override
  public void deleteSignal(String signalName) throws BiosClientException {
    makeSimpleMethodCaller(CSdkOperationId.DELETE_SIGNAL, signalName, null, Void.class).invoke();
  }

  @Override
  public ContextConfig createContext(ContextConfig contextConfig) throws BiosClientException {
    final var resources = new String[2];
    resources[0] = "tenantName";
    resources[1] = sessionInfo.getTenantName();
    return makeSimpleMethodCaller(
            CSdkOperationId.CREATE_CONTEXT,
            null,
            Objects.requireNonNull(contextConfig),
            ContextConfig.class)
        .invoke();
  }

  @Override
  public List<ContextConfig> getContexts(
      Collection<String> names, Collection<GetStreamOption> options) throws BiosClientException {
    final String[] methodOptions = makeGetStreamsOptions(names, options);

    return List.of(
        makeGenericMethodCaller(
                CSdkOperationId.GET_CONTEXTS,
                null,
                null,
                methodOptions,
                null,
                ContextConfig[].class)
            .invoke());
  }

  @Override
  public ContextConfig getContext(String contextName, Collection<GetStreamOption> options)
      throws BiosClientException {
    final var methodOptions = new HashSet<>(options);
    methodOptions.add(GetStreamOption.DETAIL);
    methodOptions.add(GetStreamOption.INCLUDE_INTERNAL);
    return getContexts(List.of(contextName), methodOptions).get(0);
  }

  @Override
  public ContextConfig updateContext(String contextName, ContextConfig contextConfig)
      throws BiosClientException {
    return makeSimpleMethodCaller(
            CSdkOperationId.UPDATE_CONTEXT,
            contextName,
            Objects.requireNonNull(contextConfig),
            ContextConfig.class)
        .invoke();
  }

  @Override
  public void deleteContext(String contextName) throws BiosClientException {
    makeSimpleMethodCaller(CSdkOperationId.DELETE_CONTEXT, contextName, null, Void.class).invoke();
  }

  @Override
  public ImportSourceConfig createImportSource(ImportSourceConfig importSourceConfig)
      throws BiosClientException {
    return tenantAppendixOperator.create(
        TenantAppendixCategory.IMPORT_SOURCES,
        importSourceConfig.getImportSourceId(),
        importSourceConfig);
  }

  @Override
  public ImportSourceConfig getImportSource(String importSourceId) throws BiosClientException {
    return tenantAppendixOperator.get(TenantAppendixCategory.IMPORT_SOURCES, importSourceId);
  }

  @Override
  public ImportSourceConfig updateImportSource(
      String importSourceId, ImportSourceConfig importSourceConfig) throws BiosClientException {
    return tenantAppendixOperator.update(
        TenantAppendixCategory.IMPORT_SOURCES, importSourceId, importSourceConfig);
  }

  @Override
  public void deleteImportSource(String importSourceId) throws BiosClientException {
    tenantAppendixOperator.delete(TenantAppendixCategory.IMPORT_SOURCES, importSourceId);
  }

  @Override
  public ImportDestinationConfig createImportDestination(
      ImportDestinationConfig importDestinationConfig) throws BiosClientException {
    return tenantAppendixOperator.create(
        TenantAppendixCategory.IMPORT_DESTINATIONS,
        importDestinationConfig.getImportDestinationId(),
        importDestinationConfig);
  }

  @Override
  public ImportDestinationConfig getImportDestination(String importDestinationId)
      throws BiosClientException {
    return tenantAppendixOperator.get(
        TenantAppendixCategory.IMPORT_DESTINATIONS, importDestinationId);
  }

  @Override
  public ImportDestinationConfig updateImportDestination(
      String importDestinationId, ImportDestinationConfig importDestinationConfig)
      throws BiosClientException {
    return tenantAppendixOperator.update(
        TenantAppendixCategory.IMPORT_DESTINATIONS, importDestinationId, importDestinationConfig);
  }

  @Override
  public void deleteImportDestination(String importDestinationId) throws BiosClientException {
    tenantAppendixOperator.delete(TenantAppendixCategory.IMPORT_DESTINATIONS, importDestinationId);
  }

  @Override
  public ImportFlowConfig createImportFlowSpec(ImportFlowConfig importFlowSpec)
      throws BiosClientException {
    return tenantAppendixOperator.create(
        TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowSpec.getImportFlowId(), importFlowSpec);
  }

  @Override
  public ImportFlowConfig getImportFlowSpec(String importFlowId) throws BiosClientException {
    return tenantAppendixOperator.get(TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowId);
  }

  @Override
  public ImportFlowConfig updateImportFlowSpec(String importFlowId, ImportFlowConfig importFlowSpec)
      throws BiosClientException {
    return tenantAppendixOperator.update(
        TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowId, importFlowSpec);
  }

  @Override
  public void deleteImportFlowSpec(String importFlowId) throws BiosClientException {
    tenantAppendixOperator.delete(TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowId);
  }

  @Override
  public void createImportDataProcessor(ImportDataProcessorConfig processorConfig)
      throws BiosClientException {
    tenantAppendixOperator.create(
        TenantAppendixCategory.IMPORT_DATA_PROCESSORS,
        processorConfig.getProcessorName(),
        processorConfig);
  }

  @Override
  public ImportDataProcessorConfig getImportDataProcessor(String processorName)
      throws BiosClientException {
    return tenantAppendixOperator.get(TenantAppendixCategory.IMPORT_DATA_PROCESSORS, processorName);
  }

  @Override
  public void updateImportDataProcessor(
      String processorName, ImportDataProcessorConfig processorConfig) throws BiosClientException {
    tenantAppendixOperator.update(
        TenantAppendixCategory.IMPORT_DATA_PROCESSORS, processorName, processorConfig);
  }

  @Override
  public void deleteImportDataProcessor(String processorName) throws BiosClientException {
    tenantAppendixOperator.delete(TenantAppendixCategory.IMPORT_DATA_PROCESSORS, processorName);
  }

  @Override
  public ExportDestinationConfig createExportDestination(
      ExportDestinationConfig exportDestinationConfig) throws BiosClientException {
    return makeGenericMethodCaller(
            CSdkOperationId.CREATE_EXPORT_DESTINATION,
            null,
            null,
            null,
            exportDestinationConfig,
            ExportDestinationConfig.class)
        .invoke();
  }

  @Override
  public ExportDestinationConfig getExportDestination(String exportDestinationId)
      throws BiosClientException {
    final var resources =
        new String[] {
          "storageName", Objects.requireNonNull(exportDestinationId),
        };
    return makeGenericMethodCaller(
            CSdkOperationId.GET_EXPORT_DESTINATION,
            null,
            resources,
            null,
            null,
            ExportDestinationConfig.class)
        .invoke();
  }

  @Override
  public ExportDestinationConfig updateExportDestination(
      String exportDestinationId, ExportDestinationConfig exportDestinationConfig)
      throws BiosClientException {
    final var resources =
        new String[] {
          "storageName", Objects.requireNonNull(exportDestinationId),
        };
    return makeGenericMethodCaller(
            CSdkOperationId.UPDATE_EXPORT_DESTINATION,
            null,
            resources,
            null,
            exportDestinationConfig,
            ExportDestinationConfig.class)
        .invoke();
  }

  @Override
  public void deleteExportDestination(String exportDestinationId) throws BiosClientException {
    final var resources =
        new String[] {
          "storageName", Objects.requireNonNull(exportDestinationId),
        };
    makeGenericMethodCaller(
            CSdkOperationId.DELETE_EXPORT_DESTINATION,
            null,
            resources,
            null,
            null,
            ExportDestinationConfig.class)
        .invoke();
  }

  @Override
  public void close() {
    super.close();
  }

  private CompletableFuture<ISqlResponse> runExecuteAsync(Statement statement, boolean isAtomic) {
    try {
      Objects.requireNonNull(statement);
      checkLoggedIn();
      final var atomicOperationSpec =
          isAtomic ? new AtomicOperationSpec(UUID.randomUUID(), 1) : null;
      final var type = statement.getStatementType();
      switch (type) {
        case INSERT:
          return insert((ISql.Insert) statement, atomicOperationSpec);
        case SELECT:
          return signalSelect((ISql.SignalSelect) statement);
        case UPSERT:
          return upsert((ISql.ContextUpsert) statement);
        case CONTEXT_SELECT:
          final var selectStatement = (ISql.ContextSelect) statement;
          if (selectStatement.isExtendedRequest()) {
            return contextSelect(selectStatement.getExRequest());
          } else {
            return getContextEntries(selectStatement);
          }
        case CONTEXT_UPDATE:
          return updateContextEntry((ISql.ContextUpdate) statement);
        case DELETE:
          return deleteContextEntry((ISql.ContextDelete) statement);
        case ATOMIC_MUTATION:
          {
            return mutateAtomically((ISql.AtomicMutation) statement);
          }
        default:
          throw new BiosClientException(
              BiosClientError.INVALID_REQUEST,
              "Execution of statement type " + type + " is not supported");
      }
    } catch (BiosClientException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<ISqlResponse> mutateAtomically(ISql.AtomicMutation statement)
      throws BiosClientException {
    if (statement.getStatements().size() > 1) {
      throw new BiosClientException(
          BiosClientError.NOT_IMPLEMENTED,
          "Coming soon: Multiple statements to rollback on failure");
    }
    final var childStatement = statement.getStatements().get(0);
    final var type = childStatement.getStatementType();
    switch (type) {
      case INSERT:
        return runExecuteAsync(childStatement, true);
      case ATOMIC_MUTATION:
        throw new BiosClientException(
            BiosClientError.BAD_INPUT, "rollbackOnFailure must not be nested");
      default:
        throw new BiosClientException(
            BiosClientError.INVALID_REQUEST,
            "Rolling back option is not supported for statement type " + type);
    }
  }

  private MultiSelectRequest createSignalSelectRequest(MultiSelectRequest request) {
    ProtoSelectRequestWriter requestWriter = new ProtoSelectRequestWriter();
    for (var query : request.queries()) {
      requestWriter.addQuery((ProtoSelectQuery) query);
    }
    return requestWriter;
  }

  private CompletableFuture<ISqlResponse> signalSelect(ISql.SignalSelect statement) {
    return signalSelectCore(statement).thenApply((responses) -> responses.get(0));
  }

  private CompletableFuture<List<ISqlResponse>> signalSelectCore(ISql.SignalSelect... statements) {

    MetricsHandle handle = begin(OperationType.SELECT, false, super::timestamp);
    try {
      MultiSelectRequest request = createSignalSelectRequest(toSelectRequest(statements));

      int queryIdx = 0;
      for (var query : request.queries()) {
        String signalName = extractFrom(query);
        SignalAttachment attachment = getOrCreateSignalAppendix(signalName);
        attachment.validateQuery(query, queryIdx);
      }

      final var apiCaller =
          makeSelectCaller(request, handle.timeStampBuffer())
              .addTimestampBuffer(handle.timeStampBuffer());
      if (handle.timeStampBuffer() != null) {
        apiCaller.addPostProcessor(
            (resp, completed) -> {
              recordOperationCompletion(handle, request, completed);
              return resp;
            });
      }
      recordAsyncCompletion(handle, request);
      return apiCaller
          .invokeAsync()
          .thenApply(
              (response) -> {
                recordSyncCompletion(handle, request);
                return response.getSelectResponses().stream().collect(Collectors.toList());
              });
    } catch (BiosClientException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<ISqlResponse> upsert(ISql.ContextUpsert statement)
      throws BiosClientException {
    return makeUpsertContextCaller(statement)
        .invokeAsync()
        .thenApply((none) -> VoidISqlResponse.INSTANCE);
  }

  private CompletableFuture<ISqlResponse> updateContextEntry(ISql.ContextUpdate statement)
      throws BiosClientException {
    return makeContextUpdateCaller(statement)
        .invokeAsync()
        .thenApply((none) -> VoidISqlResponse.INSTANCE);
  }

  private CompletableFuture<ISqlResponse> getContextEntries(ISql.ContextSelect statement)
      throws BiosClientException {
    return makeGetContextEntriesCaller(statement)
        .invokeAsync()
        .thenApply(ContextResponseWrapper::new);
  }

  private CompletableFuture<ISqlResponse> contextSelect(SelectContextRequest request)
      throws BiosClientException {
    return makeSelectContextEntriesCaller(request)
        .invokeAsync()
        .thenApply(ContextResponseWrapper::new);
  }

  private CompletableFuture<List<ISqlResponse>> contextSelectCore(
      ISql.ContextSelect... statements) {

    try {
      final var request = new MultiGetSpec();
      request.setContextNames(new ArrayList<>());
      request.setQueries(new ArrayList<>());
      for (var statement : statements) {
        request.getContextNames().add(statement.getContextName());
        final var spec = new MultiContextEntriesSpec();
        spec.setContentRepresentation(ContentRepresentation.UNTYPED);
        spec.setPrimaryKeys(convertCompositeKeys(statement.getPrimaryKeyValues()));
        spec.setAttributes(statement.getSelectColumns());
        request.getQueries().add(spec);
      }

      final var apiCaller = makeMultiGetContextCaller(request);
      return apiCaller
          .invokeAsync()
          .thenApply(
              (response) ->
                  response.getEntryLists().stream()
                      .map(ContextResponseWrapper::new)
                      .collect(Collectors.toList()));
    } catch (BiosClientException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<ISqlResponse> deleteContextEntry(ISql.ContextDelete statement)
      throws BiosClientException {
    return makeDeleteContextEntryCaller(statement)
        .invokeAsync()
        .thenApply((none) -> VoidISqlResponse.INSTANCE);
  }

  private CompletableFuture<ISqlResponse> insert(
      ISql.Insert insertStatement, AtomicOperationSpec atomicOperationSpec)
      throws BiosClientException {
    MetricsHandle metricsHandle = begin(OperationType.INSERT, true, super::timestamp);
    final var signalAppendix = getOrCreateSignalAppendix(insertStatement.getSignalName());

    final CompletableFuture<InsertResponse> future;
    if (insertStatement.isBulk()) {
      future = insertBulkCore(insertStatement, atomicOperationSpec, signalAppendix, metricsHandle);
    } else {
      future =
          insertCore(insertStatement, atomicOperationSpec, signalAppendix, metricsHandle)
              .thenApply((record) -> new InsertResponse(List.of(record)));
    }
    return future.thenApply(
        (response) -> {
          if (metricsHandle.timeStampBuffer() != null) {
            signalAppendix.getMetricsController().endSync(metricsHandle, timestamp());
          }
          return response;
        });
  }

  private CompletableFuture<InsertResponse> insertBulkCore(
      ISql.Insert insertRequest,
      AtomicOperationSpec atomicOperationSpec,
      SignalAttachment signalAttachment,
      MetricsHandle handle)
      throws BiosClientException {
    if (insertRequest.getCsvs().size() > MAXIMUM_NUM_BULK_INSERT_EVENTS) {
      throw new IngestBulkFailedException(
          BiosClientError.BAD_INPUT,
          "Number of events in a bulk insert may not exceed " + MAXIMUM_NUM_BULK_INSERT_EVENTS);
    }
    final InsertBulkExecutor<? extends InsertBulkSuccessResponse> insertBulkExecutor =
        new InsertBulkExecutor<>(
            insertRequest.getCsvs(),
            atomicOperationSpec,
            sessionId,
            insertRequest.getSignalName(),
            signalAttachment.getSchemaVersion(),
            this);

    return insertBulkExecutor
        .invokeAsync()
        .thenApply(
            (records) -> {
              if (handle.timeStampBuffer() != null) {
                signalAttachment.getMetricsController().end(handle, timestamp());
              }
              return new InsertResponse(records);
            })
        .exceptionally(
            (t) -> {
              final var cause = t instanceof CompletionException ? t.getCause() : t;
              if (cause instanceof InsertBulkFailedException) {
                throw new CompletionException(cause);
              } else {
                throw new CompletionException(
                    new InsertBulkFailedException(
                        BiosClientError.GENERIC_CLIENT_ERROR, cause.toString()));
              }
            });
  }

  private CompletableFuture<Record> insertCore(
      ISql.Insert statement,
      AtomicOperationSpec atomicOperationSpec,
      SignalAttachment signalAttachment,
      MetricsHandle handle) {
    final var apiCaller =
        makeInsertCaller(
            statement,
            atomicOperationSpec,
            signalAttachment.getSchemaVersion(),
            handle.timeStampBuffer());
    if (handle.timeStampBuffer() != null) {
      apiCaller.addPostProcessor(
          (resp, completed) -> {
            signalAttachment.getMetricsController().end(handle, completed);
            return resp;
          });
    }
    final var future = apiCaller.invokeAsync();
    if (handle.timeStampBuffer() != null) {
      signalAttachment.getMetricsController().endAsync(handle, timestamp());
    }
    return future;
  }

  private String extractFrom(SelectStatement query) throws BiosClientException {
    String from = query.getFrom();
    if (from == null || from.isBlank()) {
      throw new BiosClientException(
          BiosClientError.BAD_INPUT, "From must be specified in select query");
    }
    return from;
  }

  private SignalAttachment getOrCreateSignalAppendix(String signalName) throws BiosClientException {
    try {
      final var attachment =
          signalAttachments.computeIfAbsent(
              signalName,
              (name) -> {
                try {
                  return createSignalAttachment(name);
                } catch (BiosClientException e) {
                  throw new RuntimeException(e);
                }
              });
      return attachment;
    } catch (RuntimeException e) {
      if (e.getCause() instanceof BiosClientException) {
        throw (BiosClientException) e.getCause();
      }
      throw e;
    }
  }

  // TODO(Naoki): Provide following in future:
  //  - validator
  //  - schema version
  //  But we also should reconsider whether these controls on the language layer is a right thing.
  //  C-SDK already has a feature to resend the ingest request with new signal version
  //  if it gets a version mismatch error.
  //  Also, query validation on the language layer would cause repeating development of the same
  //  loging for each language.
  private SignalAttachment createSignalAttachment(String signalName) throws BiosClientException {
    String tenantName = sessionInfo.getTenantName();
    final QueryValidator queryValidator = null;
    final long signalSchemaVersion = 0;
    return new SignalAttachment(tenantName, signalName, signalSchemaVersion, queryValidator);
  }

  private void recordSyncCompletion(MetricsHandle handle, MultiSelectRequest request) {
    if (handle.timeStampBuffer() == null) {
      return;
    }
    for (var query : request.queries()) {
      SignalAttachment attachment = signalAttachments.get(query.getFrom());
      if (attachment == null) {
        continue;
      }
      attachment.getMetricsController().endSync(handle, timestamp());
    }
  }

  private void recordOperationCompletion(
      MetricsHandle handle, MultiSelectRequest request, long completion) {
    for (var query : request.queries()) {
      SignalAttachment attachment = signalAttachments.get(query.getFrom());
      if (attachment == null) {
        continue;
      }
      attachment.getMetricsController().end(handle, completion);
    }
  }

  private void recordAsyncCompletion(MetricsHandle handle, MultiSelectRequest request) {
    if (handle.timeStampBuffer() == null) {
      return;
    }
    for (var query : request.queries()) {
      SignalAttachment attachment = signalAttachments.get(query.getFrom());
      if (attachment == null) {
        continue;
      }
      attachment.getMetricsController().endAsync(handle, timestamp());
    }
  }

  private void checkLoggedIn() throws BiosClientException {
    if (sessionInfo == null) {
      throw new BiosClientException(BiosClientError.UNAUTHORIZED, "Not yet authenticated");
    }
  }

  private SelectStatement toSelectStatement(ISql.SignalSelect select) {
    List<String> attributes = new ArrayList<>();
    List<Metric.MetricFinalSpecifier> metrics = new ArrayList<>();
    final var columns = select.getSelectColumns();

    getMetricsAndAttributes(columns, attributes, metrics, null);

    if (attributes.isEmpty()) {
      attributes = null;
    }
    if (metrics.isEmpty()) {
      metrics = null;
    }
    var selectBuilder =
        new ProtoSelectQueryBuilder(new BasicBuilderValidator(), attributes, metrics);

    selectBuilder.from(select.getSignalName());
    if (select.getWhere() != null) {
      selectBuilder.where(select.getWhere());
    }
    if ((select.getGroupByColumns() != null) && (!select.getGroupByColumns().isEmpty())) {
      selectBuilder.groupBy(select.getGroupByColumns().toArray(new String[0]));
    }
    if (select.getOrderByColumn() != null) {
      var orderByBuilder = new ProtoOrderByBuilder(select.getOrderByColumn());
      if ((select.getOrderByReverse() != null) && (select.getOrderByReverse())) {
        orderByBuilder.desc();
      } else {
        orderByBuilder.asc();
      }
      if ((select.getOrderByCaseSensitive() != null) && (select.getOrderByCaseSensitive())) {
        orderByBuilder.caseSensitive();
      }
      selectBuilder.orderBy(orderByBuilder);
    }
    if (select.getLimit() != null) {
      selectBuilder.limit(select.getLimit());
    }
    if (select.getWindowSizeMs() != null) {
      // Decide which type of window.
      if (Objects.equals(select.getHopSizeMs(), select.getWindowSizeMs())) {
        var tumblingWindowBuilder =
            new ProtoTumblingWindowBuilder(new BasicBuilderValidator(), select.getWindowSizeMs());
        selectBuilder.window(tumblingWindowBuilder);
      } else {
        var hoppingWindowBuilder =
            new ProtoSlidingWindowBuilder(
                new BasicBuilderValidator(),
                select.getHopSizeMs(),
                (int) (select.getWindowSizeMs() / select.getHopSizeMs()));
        selectBuilder.window(hoppingWindowBuilder);
      }
    }
    // Decide whether to use timeRange or snappedTimeRange.
    if (select.getWindowSizeMs() != null) {
      selectBuilder.snappedTimeRange(
          select.getOriginTimeEpochMs(),
          select.getDeltaMs(),
          (select.getAlignmentDurationMs() != null) ? select.getAlignmentDurationMs() : 0);
    } else {
      selectBuilder.timeRange(select.getOriginTimeEpochMs(), select.getDeltaMs());
    }

    // Add onTheFly if present.
    if (select.getOnTheFly() == Boolean.TRUE) {
      selectBuilder.onTheFly();
    }

    return selectBuilder.build();
  }

  public static void getMetricsAndAttributes(
      final List<String> columns,
      List<String> attributes,
      List<Metric.MetricFinalSpecifier> metricsProto,
      List<GenericMetric> metricsGeneric) {
    Pattern simpleAttributePattern = Pattern.compile("^[\\w]+$");
    Pattern metricPattern = Pattern.compile("^([\\w]+)\\(([\\w]+)?\\)(\\s+[aA][sS]\\s+([\\w]+))?$");
    for (String column : columns) {
      if (simpleAttributePattern.matcher(column).find()) {
        attributes.add(column);
      } else {
        var matcher = metricPattern.matcher(column);
        if (!matcher.find()) {
          throw new IllegalArgumentException("Invalid syntax for column in select list: " + column);
        }
        var function = MetricFunction.valueOf(matcher.group(1).toUpperCase(Locale.ROOT));
        var metricBuilder = new ProtoMetricBuilder(new BasicBuilderValidator(), function);
        var genericMetric = new GenericMetric();
        genericMetric.setFunction(function);
        if ((matcher.groupCount() > 1) && (matcher.group(2) != null)) {
          metricBuilder.of(matcher.group(2));
          genericMetric.setOf(matcher.group(2));
        }
        if ((matcher.groupCount() > 2)
            && (matcher.group(4) != null)
            && (!matcher.group(4).isEmpty())) {
          metricBuilder.as(matcher.group(4));
          genericMetric.setAs(matcher.group(4));
        }
        if (metricsProto != null) {
          metricsProto.add(metricBuilder);
        } else if (metricsGeneric != null) {
          metricsGeneric.add(genericMetric);
        }
      }
    }
  }

  private MultiSelectRequest toSelectRequest(ISql.SignalSelect... statements) {
    if (statements == null) {
      throw new IllegalArgumentException("Must specify atleast one statement");
    }
    return () ->
        Arrays.stream(statements).map(this::toSelectStatement).collect(Collectors.toList());
  }

  private String[] makeGetStreamsOptions(
      Collection<String> names, Collection<GetStreamOption> options) {
    int optionsSize = Objects.requireNonNull(options).size() * 2;
    final String[] methodOptions;
    int index = 0;
    if (!Objects.requireNonNull(names).isEmpty()) {
      optionsSize += 2;
      methodOptions = new String[optionsSize];
      methodOptions[index++] = "names";
      methodOptions[index++] = names.stream().collect(Collectors.joining(","));
    } else {
      methodOptions = new String[optionsSize];
    }

    final var allowedOptions =
        Set.of(
            GetStreamOption.DETAIL,
            GetStreamOption.INCLUDE_INTERNAL,
            GetStreamOption.INCLUDE_INFERRED_TAGS);
    for (var option : Objects.requireNonNull(options)) {
      if (!allowedOptions.contains(option)) {
        throw new IllegalArgumentException("GetStreamOption " + option + " not supported");
      }
      methodOptions[index++] = option.getOptionName();
      methodOptions[index++] = "true";
    }
    return methodOptions;
  }
}
