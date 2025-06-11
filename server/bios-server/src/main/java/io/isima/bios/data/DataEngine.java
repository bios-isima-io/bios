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
package io.isima.bios.data;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.IngestState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.dto.AllContextSynopses;
import io.isima.bios.dto.ContextSynopsis;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.dto.ReplaceContextAttributesRequest;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SynopsisRequest;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.AtomicOperationContext;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.ContextMaintenanceAction;
import io.isima.bios.models.ContextMaintenanceResult;
import io.isima.bios.models.Event;
import io.isima.bios.models.FeatureRefreshRequest;
import io.isima.bios.models.FeatureStatusRequest;
import io.isima.bios.models.FeatureStatusResponse;
import io.isima.bios.models.InsertBulkEachResult;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.MaintenanceAction;
import io.isima.bios.models.TenantKeyspaces;
import io.isima.bios.models.TenantTables;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.handlers.InsertState;
import io.isima.bios.server.handlers.SelectState;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** This interface provides boundary between persistence component and others. */
public interface DataEngine extends ContextReferrer {
  // constants //////////////////////////////////////////////////////////
  public static final String MAX_EXTRACT_CONCURRENCY = "prop.maxExtractConcurrency";
  public static final int DEFAULT_MAX_EXTRACT_CONCURRENCY = 128;

  ///////////////////////////////////////////////////////////////////////

  /**
   * Method used for ingesting an event.
   *
   * <p>The state object includes the persisting event as a member. The method persists the event in
   * asynchronous manner.
   *
   * @param state Ingest execution state
   * @param acceptor Asynchronous result acceptor. The method generates an IngestResponse object on
   *     successful execution and invokes {@code acceptor.accept()} to let the caller consume it.
   * @param errorHandler Asynchronous error handler. The method catches any exceptions thrown during
   *     the execution, and calls {@code errorHandler.accept()} to let the caller handle it.
   */
  @Deprecated
  void ingestEvent(
      IngestState state, Consumer<IngestResponse> acceptor, Consumer<Throwable> errorHandler);

  /**
   * A fire-and-forget method for inserting an event asynchronously.
   *
   * <p>Errors are logged. This is used to ingest events into system signals.
   *
   * @param streamDesc Target stream config.
   * @param event Event to ingest.
   * @param state Execution state
   */
  void insertEventIgnoreErrors(StreamDesc streamDesc, Event event, ExecutionState state);

  /**
   * Method used for ingesting multiple events.
   *
   * <p>This is a synchronous call. The method returns when all events have been ingested.
   *
   * @param streamDesc Target stream config
   * @param events Events to ingest
   * @throws ApplicationException When unexpected error happens
   * @throws TfosException When user error happens
   * @deprecated Dangerous blocking call. We must not use this.
   */
  // TODO(BIOS-4937): Port the insert bulk operation in the class DataServiceHandler to here.
  @Deprecated
  void ingestEventsBulk(StreamDesc streamDesc, List<Event> events, ExecutionState state)
      throws ApplicationException, TfosException;

  CompletionStage<InsertResponseRecord> insert(InsertState state);

  CompletionStage<List<InsertBulkEachResult>> insertBulk(
      StreamDesc streamDesc, List<Event> events, ExecutionState state);

  /**
   * Method to extract objects.
   *
   * <p>The state object includes parameters that are necessary for query. The method executes the
   * operation asynchrnously, i.e., the method returns immediately before execution completes. The
   * results are consumed by acceptor or errorHandler based on the success/failure state.
   *
   * @param state Ingest execution state
   * @param acceptor Asynchronous result acceptor. The method generates an IngestResponse object on
   *     successful execution and invokes {@code acceptor.accept()} to let the caller consume it.
   * @param errorHandler Asynchronous error handler. The method catches any exceptions thrown during
   *     the execution, and calls {@code errorHandler.accept()} to let the caller handle it.
   */
  void extractEvents(
      ExtractState state, Consumer<List<Event>> acceptor, Consumer<Throwable> errorHandler);

  @Deprecated
  void summarize(
      SummarizeState state,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler);

  CompletionStage<Map<Long, List<Event>>> summarize(SummarizeState state);

  Map<Long, List<Event>> summarize(SummarizeState state, String caller, long timeoutMillis)
      throws Throwable;

  void complexQuery(
      ComplexQueryState state,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler);

  CompletionStage<Void> select(SelectState state);

  CompletionStage<Void> putContextEntriesAsync(ContextWriteOpState<?, List<Event>> state);

  void listContextPrimaryKeysAsync(
      StreamDesc desc,
      ContextOpState state,
      Consumer<List<List<Object>>> acceptor,
      Consumer<Throwable> errorHandler);

  void countContextPrimaryKeysAsync(
      StreamDesc contextDesc,
      ContextOpState state,
      Consumer<Long> acceptor,
      Consumer<Throwable> errorHandler);

  @Deprecated
  List<Event> getContextEntries(List<List<Object>> keys, ContextOpState state)
      throws TfosException, ApplicationException;

  void getContextEntriesAsync(
      List<List<Object>> keys,
      ContextOpState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler);

  CompletionStage<Void> deleteContextEntriesAsync(ContextWriteOpState<?, List<List<Object>>> state);

  /**
   * Update attributes of a context entry.
   *
   * @param state context execution state, includes context name, phase and timestamp
   * @throws TfosException When operation fails due to user input
   * @throws ApplicationException When operation fails due to an unexpected error
   */
  CompletionStage<Void> updateContextEntryAsync(
      ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec> state);

  /**
   * Replace attributes of matching entries.
   *
   * @param state context execution state
   * @throws TfosException When operation fails due to user input
   * @throws ApplicationException When operation fails due to an unexpected error
   */
  CompletionStage<Void> replaceContextAttributesAsync(
      ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec> state);

  /**
   * Populate index entries.
   *
   * @param signalConfig Target signal StreamConfig
   * @param indexConfig Index StreamConfig to populate
   * @param indexes Map of time index and set of lists of values to populate. A list of Object
   *     values in the collection include index keys of an entry. Values must be time_index followed
   *     by index stream attributes. Order of attribute values must preserve original order in the
   *     config object.
   */
  // TODO(TFOS-1078): This method should not be exposed to outside the package. This method has
  // dangerous implicit value ordering restriction, so hide the method in the DataEngineImpl as soon
  // as possible.
  CompletableFuture<Void> populateIndexes(
      StreamDesc signalConfig,
      StreamDesc indexConfig,
      Map<Long, Set<List<Object>>> indexes,
      ExecutionState state);

  /**
   * Populate context index entries.
   *
   * @param contextDesc Target context descriptor
   * @param indexConfig Index StreamConfig to populate
   * @param newEvents List of new index Events to populate
   * @param deletedEvents List of Events to be deleted
   */
  CompletableFuture<Void> populateContextIndexes(
      StreamDesc contextDesc,
      StreamDesc indexConfig,
      List<Event> newEvents,
      List<Event> deletedEvents,
      ExecutionState state);

  CompletableFuture<FeatureStatusResponse> featureStatus(
      ExecutionState state, FeatureStatusRequest request);

  CompletableFuture<Void> featureRefresh(ExecutionState state, FeatureRefreshRequest request);

  /** Retrieves context synopses in a tenant. */
  CompletionStage<AllContextSynopses> getAllContextSynopses(
      SynopsisRequest request, ExecutionState state);

  /** Retrieves a context synopsis with attribute synopses. */
  CompletionStage<ContextSynopsis> getContextSynopsis(
      SynopsisRequest request, ExecutionState state);

  /** Executes a pending batch mutation if set. */
  CompletionStage<Void> commit(AtomicOperationContext atomicOperationContext, ExecutionState state);

  void registerDataPublisher(DataPublisher publisher);

  void shutdown();

  CompletionStage<SelectContextEntriesResponse> selectContextEntriesAsync(
      SelectContextRequest request, ContextOpState state);

  TenantKeyspaces maintainTenantKeyspaces(
      MaintenanceAction action,
      boolean includeDropped,
      Optional<Integer> limit,
      Optional<List<String>> keyspaces,
      Optional<List<String>> tenants)
      throws TfosException, ApplicationException;

  /** Deletes all events in a time index calculated by the specified timestamp. */
  CompletionStage<Void> clearTimeIndex(StreamDesc subStream, long timestamp, ExecutionState state);

  TenantTables maintainTenantTables(
      MaintenanceAction action, String keyspace, Integer limit, List<String> tables)
      throws TfosException, ApplicationException;

  ContextMaintenanceResult maintainContext(
      ContextMaintenanceAction action,
      String tenantName,
      String contextName,
      Integer gcGraceSeconds,
      Integer batchiSize)
      throws TfosException, ApplicationException;

  default CassStream getCassStream(StreamDesc streamDesc) {
    return null;
  }

  // To be used temporarily for running new async operations from conventional
  // synchronized methods. It is not ideal for thread management. Those conventional methods
  // should be deprecated and removed.
  @Deprecated
  default Executor getExecutor() {
    return null;
  }
}
