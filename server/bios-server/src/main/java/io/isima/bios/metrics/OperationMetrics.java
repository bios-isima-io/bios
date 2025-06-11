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
package io.isima.bios.metrics;

import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;
import static io.isima.bios.metrics.MetricsConstants.BIOS_SYSTEM_OP_METRICS_SIGNAL;
import static io.isima.bios.metrics.MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL;
import static io.isima.bios.recorder.OperationsMetricsCollector.Entry;
import static java.util.Map.entry;

import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.AdminChangeListener;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.IngestState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.execution.digestor.Digestion;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.Event;
import io.isima.bios.models.Events;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.preprocess.Utils;
import io.isima.bios.recorder.ContextRequestType;
import io.isima.bios.recorder.OperationMetricGroupRecorder;
import io.isima.bios.recorder.OperationsMeasurementRegistry;
import io.isima.bios.recorder.OperationsMetricsCollector;
import io.isima.bios.recorder.RecorderConstants;
import io.isima.bios.recorder.RequestType;
import io.isima.bios.recorder.SignalRequestType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** shuts down the service. */
public class OperationMetrics extends Digestion<GenericExecutionState>
    implements AdminChangeListener, MetricsRecordFinder {
  private static final Logger logger = LoggerFactory.getLogger(OperationMetrics.class);
  private static final int MAX_PARALLEL_INGESTS = 100;
  private static final int MAX_PARALLEL_INGESTS_ON_DB_ERROR = 2;
  private static final int DNS_CALL_CYCLE = 10;
  private static final int MAX_CREATE_PER_MINUTE = 25;

  private static final Map<String, String> tenantUsageRequests =
      Map.ofEntries(
          entry(SignalRequestType.INSERT.name(), SignalRequestType.INSERT.name()),
          entry(SignalRequestType.INSERT_BULK.name(), SignalRequestType.INSERT.name()),
          entry(SignalRequestType.SELECT.name(), SignalRequestType.SELECT.name()),
          entry(ContextRequestType.SELECT_CONTEXT.name(), SignalRequestType.SELECT.name()),
          entry(ContextRequestType.UPSERT.name(), ContextRequestType.UPSERT.name()),
          entry(ContextRequestType.UPDATE.name(), ContextRequestType.UPDATE.name()),
          entry(
              ContextRequestType.REPLACE_CONTEXT_ATTRIBUTES.name(),
              ContextRequestType.REPLACE_CONTEXT_ATTRIBUTES.name()),
          entry(ContextRequestType.DELETE.name(), ContextRequestType.DELETE.name()));

  private final DataEngine dataEngine;
  private final OperationsMeasurementRegistry recorderRegistry;
  private final OperationsMetricsCollector operationsMetricsCollector;

  // following need not be thread safe, used only by a single thread
  // mainly to reduce dns calls
  private String thisNode;
  private int nodeNameCallCount;

  /**
   * TFOS Metrics component.
   *
   * <p>The class is responsible to collect metrics values on demand and to report metrics
   * periodically.
   */
  public OperationMetrics(DataEngine dataEngine) {
    super(
        "OperationMetricsLogger",
        TfosConfig.metricsReportIntervalSeconds(),
        TfosConfig.metricsReportIntervalSeconds(),
        TimeUnit.SECONDS,
        GenericExecutionState.class);
    this.dataEngine = dataEngine;
    this.thisNode = "";
    this.nodeNameCallCount = 0;

    logger.debug("Metrics component initializing ###");
    if (TfosConfig.metricsReportEnabled()) {
      recorderRegistry =
          OperationsMeasurementRegistry.newRegistryBuilder(TENANT_SYSTEM)
              .nodeNameSupplier(this::getThisNodeName)
              .maxSuccessBandwidth(MAX_PARALLEL_INGESTS)
              .maxErrorBandwidth(MAX_PARALLEL_INGESTS_ON_DB_ERROR)
              .maxRatePerMinute(MAX_CREATE_PER_MINUTE)
              .build();
      operationsMetricsCollector = recorderRegistry.getOperationsMetricsCollector();
      if (operationsMetricsCollector == null) {
        throw new RuntimeException(
            "OperationMetrics is initialized before the OperationsMetricsCollector is set up");
      }
    } else {
      recorderRegistry = null;
      operationsMetricsCollector = null;
    }
    logger.info(
        "### Metrics component initialized. Metrics Report is "
            + (TfosConfig.metricsReportEnabled() ? "enabled" : "disabled"));
  }

  private String getThisNodeName() {
    nodeNameCallCount++;
    if (thisNode.isEmpty() || ((nodeNameCallCount % DNS_CALL_CYCLE) == 0)) {
      String node = io.isima.bios.utils.Utils.getNodeName();
      if (!node.isEmpty()) {
        thisNode = node;
      }
    }
    return thisNode;
  }

  public AdminInternal getAdmin() {
    return BiosModules.getAdminInternal();
  }

  @Override
  public OperationMetricGroupRecorder getRecorder(
      String tenantName,
      String streamName,
      String appName,
      AppType appType,
      RequestType requestType) {
    if (recorderRegistry == null) {
      return null;
    }
    if (tenantName == null || tenantName.isEmpty()) {
      return null;
    }
    if (streamName == null) {
      streamName = "";
    }

    return recorderRegistry.getRecorder(tenantName, streamName, appName, appType, requestType);
  }

  // Digestion implementation //////////////////
  @Override
  public GenericExecutionState createState(EventExecutorGroup executorGroup) {
    return new GenericExecutionState("OperationMetricsRecord", executorGroup);
  }

  @Override
  public CompletableFuture<Void> executeAsync(GenericExecutionState state) {
    try {
      return report(state);
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }

  CompletableFuture<Void> report(ExecutionState parentState) {
    // AdminInternal instance might not be created, creation is a heavy operation.
    // Can be null at start of server.
    logger.debug("Reporting system metrics");
    if (getAdmin() == null) {
      logger.error("AdminInternal is not set up");
      return CompletableFuture.completedFuture(null);
    }

    return reportRequestMetrics(parentState);
  }

  private CompletableFuture<Void> reportRequestMetrics(ExecutionState state) {
    if (operationsMetricsCollector == null) {
      logger.warn("OperationsMetricsCollector is not set up");
      return CompletableFuture.completedFuture(null);
    }
    operationsMetricsCollector.checkpoint();
    return consumeOperationMetricsRecord(state);
  }

  private CompletableFuture<Void> consumeOperationMetricsRecord(ExecutionState state) {
    final var recorderEntry = operationsMetricsCollector.getNextEntry();
    if (recorderEntry == null) {
      return CompletableFuture.completedFuture(null);
    }
    return storeOperationMetricsToDb(recorderEntry, state)
        .thenCompose((none) -> consumeOperationMetricsRecord(state));
  }

  private CompletableFuture<Void> storeOperationMetricsToDb(Entry entry, ExecutionState state) {
    // system tenant
    var future =
        storeMetricsToDb(entry.getEvent(), BIOS_SYSTEM_OP_METRICS_SIGNAL, TENANT_SYSTEM, state)
            .thenAccept((x) -> operationsMetricsCollector.completed(entry.getKey()))
            .exceptionally((ex) -> processRequestMetricsError(TENANT_SYSTEM, entry, ex));

    // may forward to the operation destination tenant
    final var opTenantName =
        entry.getEvent().getOrDefault(RecorderConstants.ATTR_BIOS_TENANT, "").toString();
    final var perTenantAttributes = tryConvertToPerTenantMetrics(opTenantName, entry);

    if (perTenantAttributes != null) {
      future =
          future.thenCompose(
              (none) ->
                  storeMetricsToDb(
                          perTenantAttributes, BIOS_TENANT_OP_METRICS_SIGNAL, opTenantName, state)
                      .thenAccept((x) -> operationsMetricsCollector.completed(entry.getKey()))
                      .exceptionally((ex) -> processRequestMetricsError(opTenantName, entry, ex)));
    }
    return future;
  }

  /**
   * Converts an operation metrics entry to be usable for per-tenant usage signal if applicable.
   *
   * <p>A tenant usage signal records only billable operations, this method filters all incoming
   * operations records to billable ones. The conditions are:
   *
   * <ul>
   *   <li>The tenant exists
   *   <li>Request is a data operation except DeleteContextEntries
   *   <li>The AppType is not internal
   * </ul>
   *
   * <p>Note that the incoming entry is modified in place for the conversion for better performance.
   *
   * @param destinationTenantName The destination tenant name
   * @param entry Entry to test
   * @return Converted metrics attributes if the entry meets the condition, else null
   */
  private Map<String, Object> tryConvertToPerTenantMetrics(
      String destinationTenantName, Entry entry) {
    if (TENANT_SYSTEM.equalsIgnoreCase(destinationTenantName)) {
      return null;
    }
    try {
      getAdmin().getTenant(destinationTenantName);
    } catch (NoSuchTenantException e) {
      return null;
    }
    final var attributes = entry.getEvent();
    final var request =
        tenantUsageRequests.get(attributes.get(RecorderConstants.ATTR_BIOS_REQUEST));
    if (request == null) {
      return null;
    }
    attributes.put(RecorderConstants.ATTR_BIOS_REQUEST, request);
    final var zero = Long.valueOf(0);
    if (zero.equals(attributes.get(RecorderConstants.ATTR_OP_NUM_READS))
        && zero.equals(attributes.get(RecorderConstants.ATTR_OP_NUM_WRITES))) {
      return null;
    }
    if ("_clientMetrics".equals(attributes.get(RecorderConstants.ATTR_BIOS_STREAM))) {
      return null;
    }
    final var appType = attributes.get(RecorderConstants.ATTR_BIOS_APP_TYPE);
    if (appType != null && "Internal".equalsIgnoreCase(appType.toString())) {
      return null;
    }
    return attributes;
  }

  private Void processRequestMetricsError(String tenantName, Entry entry, Throwable ex) {
    if (ex instanceof CompletionException || ex instanceof ExecutionException) {
      ex = ex.getCause();
    }
    if (ex instanceof NoSuchTenantException || ex instanceof NoSuchStreamException) {
      final String logMessage =
          ((ex instanceof NoSuchTenantException) ? "Tenant" : "Stream")
              + " not found. Tenant is {}, Stream is {} and Error is {}.";
      if (TENANT_SYSTEM.equalsIgnoreCase(tenantName)) {
        // no one should delete the system tenant, so treat this as an error
        logger.error(logMessage, TENANT_SYSTEM, BIOS_SYSTEM_OP_METRICS_SIGNAL, ex.getMessage());
      } else {
        logger.info(
            logMessage,
            entry.getKey().getTenantName(),
            BIOS_SYSTEM_OP_METRICS_SIGNAL,
            ex.getMessage());
      }
      operationsMetricsCollector.completed(entry.getKey());
    } else if (ex instanceof TfosException) {
      if (((TfosException) ex).getStatus().getStatusCode() >= 500) {
        operationsMetricsCollector.error(entry.getKey(), true);
      } else {
        operationsMetricsCollector.error(entry.getKey(), false);
      }
    } else {
      operationsMetricsCollector.error(entry.getKey(), true);
    }
    return null;
  }

  @Override
  public void createTenant(TenantDesc tenantDesc, RequestPhase phase) {
    if (recorderRegistry == null) {
      return;
    }
    final var tenantName = tenantDesc.getName();
    if (!tenantName.equals(TENANT_SYSTEM)) {
      recorderRegistry.addTenant(tenantName);
    }
  }

  @Override
  public void deleteTenant(TenantDesc tenantDesc, RequestPhase phase) {
    if (recorderRegistry == null) {
      return;
    }
    if (tenantDesc.getName().equals(TENANT_SYSTEM)) {
      return;
    }
    if (phase == RequestPhase.FINAL) {
      recorderRegistry.removeTenant(tenantDesc.getName());
    }
  }

  @Override
  public void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    if (recorderRegistry == null) {
      return;
    }
    if (tenantName.equals(TENANT_SYSTEM)) {
      return;
    }
  }

  @Override
  public void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase) {
    if (recorderRegistry == null) {
      return;
    }
    if (tenantName.equals(TENANT_SYSTEM)) {
      return;
    }
  }

  @Override
  public void unload() {
    if (recorderRegistry != null) {
      recorderRegistry.resetRegistry();
    }
  }

  private CompletableFuture<IngestResponse> storeMetricsToDb(
      Map<String, Object> metricsAttributes,
      final String streamName,
      final String tenantName,
      ExecutionState parentState) {
    Event metricsEvent = Events.createEvent(Generators.timeBasedGenerator().generate());
    CompletableFuture<IngestResponse> insertFuture = new CompletableFuture<>();
    try {
      IngestState ingestState =
          new IngestState(
              metricsEvent,
              null,
              tenantName,
              streamName,
              BiosModules.getDigestor().getExecutor(),
              dataEngine);
      final StreamDesc streamDesc = getAdmin().getStream(tenantName, streamName);
      ingestState.setStreamDesc(streamDesc);
      for (var attribute : streamDesc.getAttributes()) {
        final var name = attribute.getName();
        final Object value = metricsAttributes.get(name);
        if (attribute.getAttributeType() == InternalAttributeType.ENUM) {
          try {
            metricsEvent.set(name, Attributes.convertValue(value.toString(), attribute));
          } catch (InvalidValueSyntaxException | InvalidEnumException e) {
            metricsEvent.set(name, Attributes.convertValue("UNKNOWN", attribute));
          }
        } else {
          metricsEvent.set(name, value);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "Inserting a metrics record; tenant={}, signal={}, record={}",
            tenantName,
            streamName,
            BiosObjectMapperProvider.get()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(metricsEvent));
      }

      // defence against harmful data
      Utils.validateEvent(streamDesc, metricsEvent);

      Consumer<IngestResponse> acceptor = MetricsUtil.createAcceptor(ingestState, insertFuture);
      Consumer<Throwable> errorHandler = MetricsUtil.createErrorHandler(ingestState, insertFuture);
      dataEngine.ingestEvent(ingestState, acceptor, errorHandler);

    } catch (NoSuchTenantException | NoSuchStreamException ex) {
      // detailed info is taken care of later upstream
      logger.info("Tenant or stream not found {}", ex.getMessage());
      insertFuture.completeExceptionally(ex);
    } catch (Throwable th) {
      logger.error(
          "Failed to store a metrics event; tenant={}, stream={}, error={}",
          tenantName,
          streamName,
          th.getMessage());
      insertFuture.completeExceptionally(th);
    }
    return insertFuture;
  }
}
