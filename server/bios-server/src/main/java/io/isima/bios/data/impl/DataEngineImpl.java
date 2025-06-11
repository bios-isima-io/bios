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
package io.isima.bios.data.impl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.AdminChangeListener;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.IngestState;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.DataPublisher;
import io.isima.bios.data.DynamicServerRecord;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.data.ReplaceContextAttributesSpec;
import io.isima.bios.data.ServerRecord;
import io.isima.bios.data.UpdateContextEntrySpec;
import io.isima.bios.data.impl.feature.Comparators;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.data.impl.maintenance.PostProcessScheduler;
import io.isima.bios.data.impl.maintenance.TableMaintainer;
import io.isima.bios.data.impl.maintenance.TaskSlots;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.impl.models.MaintenanceMode;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.CassTenant;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextIndexCassStream;
import io.isima.bios.data.impl.storage.IndexCassStream;
import io.isima.bios.data.impl.storage.RollupCassStream;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.data.impl.storage.TableInfo;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.dto.AllContextSynopses;
import io.isima.bios.dto.ContextSynopsis;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.dto.ReplaceContextAttributesRequest;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SynopsisRequest;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.dto.bulk.InsertBulkErrorResponse;
import io.isima.bios.errors.AdminError;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.DataEngineException;
import io.isima.bios.errors.exception.DataValidationError;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.InsertBulkFailedServerException;
import io.isima.bios.execution.AsyncExecutionStage;
import io.isima.bios.execution.AtomicOperationContext;
import io.isima.bios.execution.ConcurrentExecutionController;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.maintenance.ServiceStatus;
import io.isima.bios.maintenance.WorkerLock;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.ContextMaintenanceAction;
import io.isima.bios.models.ContextMaintenanceResult;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.FeatureRefreshRequest;
import io.isima.bios.models.FeatureStatusRequest;
import io.isima.bios.models.FeatureStatusResponse;
import io.isima.bios.models.InsertBulkEachResult;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.MaintenanceAction;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SelectResponseRecords;
import io.isima.bios.models.StreamTableInfo;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.TenantKeyspaces;
import io.isima.bios.models.TenantTables;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.QueryConfig;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.query.CompiledSortRequest;
import io.isima.bios.query.TfosQueryValidator;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.handlers.InsertState;
import io.isima.bios.server.handlers.SelectState;
import io.isima.bios.service.handler.DataServiceUtils;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of dataEngine backed by Cassandra.
 *
 * <p>The implementation assumes that EJB server framework instantiates this class as a singleton
 * using the dependency injection mechanism. After the instantiation, method init() is called to
 * bootstrap the engine.
 */
public class DataEngineImpl implements DataEngine, AdminChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(DataEngineImpl.class);

  // constants //////////////////////////////////////////////////////////
  private static final int SCHEDULED_EXECUTOR_NUM_THREADS = 3;

  private static final int NUM_MILLISECONDS_ONE_MINUTE = 60000;

  private static long lastTableMaintainedTime = 0;
  private static long lastContextMaintainedTime = 0;

  public static final String PROP_CACHE_ONLY_CONTEXTS = "prop.cacheOnlyContexts";

  public static final String PROP_TABLE_MAINTENANCE_INTERVAL = "prop.tableMaintenanceInterval";
  public static final String PROP_TABLE_MAINTENANCE_MODE = "prop.tableMaintenanceMode";
  public static final String PROP_MAX_TABLE_DELETIONS_PER_MAINTENANCE =
      "prop.maxTableDeletionsPerMaintenance";
  public static final String PROP_MAX_KEYSPACE_DELETIONS_PER_MAINTENANCE =
      "prop.maxKeyspaceDeletionsPerMaintenance";

  public static final String PROP_CONTEXT_MAINTENANCE_INTERVAL = "prop.contextMaintenanceInterval";
  private static final String PROP_CONTEXT_MAINTENANCE_SECONDS = "prop.contextMaintenanceSeconds";
  private static final String PROP_CONTEXT_MAINTENANCE_PROGRESS_PREFIX = "maint.context.";

  private static final String LOCK_TARGET_TABLE_MAINTENANCE = ".bios.maintenance.tables";

  private static final QueryConfig QUERY_CONFIG = TfosConfig::summarizeHorizontalLimit;

  ///////////////////////////////////////////////////////////////////////

  private final CassandraConnection cassandraConnection;
  private final Session session;

  @Setter private QueryLogger queryLogger;

  // sub components
  @Getter private final SignalExtractor signalExtractor;
  @Getter private final SignalSummarizer signalSummarizer;
  @Getter private final SketchesExtractor sketchesExtractor;
  private final ComplexQueryEngine complexQueryEngine;
  private final ContextExtractor contextExtractor;
  private final SynopsisExtractor synopsisExtractor;

  private Map<String, CassTenant> cassTenants;
  private final Map<String, String> deletedTenants; // For debugging.

  // Executor group to be used temporarily for running new async operations from conventional
  // synchronized methods. It is not ideal for thread management. Those conventional methods
  // should be deprecated and removed.
  @Deprecated private final EventExecutorGroup executorGroup;

  // Objects for maintenance ////////////////////////////////////////////
  @Getter private final PostProcessScheduler postProcessScheduler;

  @Getter private final SketchStore sketchStore;
  private final InferenceEngine inferenceEngine;
  @Getter private final DataEngineMaintenance maintenance;

  private final TableMaintainer tableMaintainer;

  @Getter private final String propContextMaintenanceProgress;

  // deleting tables and keyspace may be slow, we do it in a separate thread
  private final ExecutorService sidelineTasksExecutorService;
  private final AtomicBoolean tablesDropping;

  private final SharedConfig sharedConfig;

  /** Retry executor. */
  private final ScheduledExecutorService retryExecutor =
      Executors.newScheduledThreadPool(SCHEDULED_EXECUTOR_NUM_THREADS);

  private final int maxIngestConcurrency;

  // This is used for making a synchronous call using an asynchronous method.
  private final ExecutorService asyncService = Executors.newSingleThreadExecutor();

  /**
   * The constructor.
   *
   * <p>This method runs following setup to have the instance ready for accessing Cassandra:
   *
   * <dl>
   *   <li>Setup a Cassandra Cluster instance
   *   <li>Create a session and connect to Cassandra
   *   <li>Get all available tenants from admin and setup corresponding keyspaces if necessary
   *   <li>Get all available streams from AdminInternal and setup corresponding tables if necessary
   * </dl>
   *
   * @param connection CassandraConnection instance
   * @throws DataEngineException Any failure in this method would cause this runtime exception.
   */
  public DataEngineImpl(
      CassandraConnection connection,
      ServiceStatus serviceStatus,
      SharedConfig sharedConfig,
      SharedProperties sharedProperties,
      BiosModules parentModules) {
    if (connection == null) {
      throw new IllegalArgumentException("connection may not be null");
    }
    logger.debug("DataEngineImpl initializing ###");
    ContextCassStream.initializeContextCacheService();
    try {
      this.sharedConfig = sharedConfig;
      cassandraConnection = connection;
      session = cassandraConnection.getSession();
      cassTenants = new ConcurrentHashMap<>();
      deletedTenants = new ConcurrentHashMap<>();
      signalExtractor = new SignalExtractor(this, session);
      postProcessScheduler = new PostProcessScheduler(cassandraConnection, this);
      sketchStore = new SketchStore(cassandraConnection, postProcessScheduler);
      sketchesExtractor = new SketchesExtractor(session, sketchStore);
      signalSummarizer = new SignalSummarizer(this, session, sketchesExtractor);
      complexQueryEngine = new ComplexQueryEngine(this);
      contextExtractor = new ContextExtractor(this, sketchesExtractor, sharedConfig);
      synopsisExtractor = new SynopsisExtractor(this, sketchesExtractor);
      inferenceEngine = new InferenceEngine(this, cassandraConnection, sharedConfig);
      maintenance =
          new DataEngineMaintenance(
              cassandraConnection,
              this,
              postProcessScheduler,
              sketchStore,
              inferenceEngine,
              sharedConfig,
              sharedProperties,
              serviceStatus,
              parentModules);
      tableMaintainer = new TableMaintainer(cassandraConnection);
      maxIngestConcurrency = TfosConfig.getDbIngestConcurrency();
      sidelineTasksExecutorService = Executors.newSingleThreadExecutor();
      tablesDropping = new AtomicBoolean(false);
      propContextMaintenanceProgress =
          PROP_CONTEXT_MAINTENANCE_PROGRESS_PREFIX + Utils.getNodeName();

      // TODO(Naoki): We may want to read the number of threads from the executor manager, but
      // this should be removed later anyway.
      executorGroup =
          new DefaultEventExecutorGroup(
              4, ExecutorManager.makeThreadFactory("tentative", Thread.NORM_PRIORITY));
    } catch (Throwable t) {
      logger.error("Caught an exception during initialization", t);
      throw new DataEngineException("Cassandra data engine initialization failed. cause: " + t, t);
    }
    logger.debug("### DataEngineImpl initialized");
  }

  public DataEngineImpl(CassandraConnection connection) {
    this(connection, null, null, null, null);
  }

  @Override
  public Executor getExecutor() {
    return executorGroup.next();
  }

  @Override
  public void shutdown() {
    ContextCassStream.shutdown();
    retryExecutor.shutdownNow();
    asyncService.shutdownNow();
  }

  public PostProcessScheduler getPostProcessScheduler() {
    return postProcessScheduler;
  }

  public DataEngineMaintenance getMaintenanceWorker() {
    return maintenance;
  }

  /**
   * Resolve CassStream by tenant name and StreamDesc.
   *
   * <p>Stream versions and delete flags matter.
   *
   * @param streamDesc Target stream description
   * @return Resolved CassStream object for the specified stream name and version. Null is returned
   *     if the target CassStream is not found or has deleted flag.
   */
  @Override
  public CassStream getCassStream(StreamDesc streamDesc) {
    TenantDesc tenantDesc = streamDesc.getParent();
    CassTenant cassTenant = getCassTenant(tenantDesc, true);
    if (cassTenant == null) {
      // not loaded yet
      return null;
    }
    return cassTenant.getCassStream(streamDesc, true);
  }

  @Override
  public void ingestEvent(
      IngestState state, Consumer<IngestResponse> acceptor, Consumer<Throwable> errorHandler) {
    state.addHistory("(persist){");
    executeOperation(
        state,
        errorHandler,
        (cassStream) -> ingestEventMayThrow(state, cassStream, acceptor, errorHandler));
  }

  /**
   * Internal ingestEvent method. This method may throw an exception.
   *
   * @throws ApplicationException when unexpected error happens
   */
  private void ingestEventMayThrow(
      IngestState state,
      CassStream cassStream,
      Consumer<IngestResponse> acceptor,
      Consumer<Throwable> errorHandler)
      throws ApplicationException {

    state.addHistory("makeStatement");
    final Event event = state.getEvent();
    final Statement statement = cassStream.makeInsertStatement(event);

    // execute asynchronously
    persistIngestEvent(state, statement, acceptor, errorHandler);
  }

  private void persistIngestEvent(
      final IngestState state,
      final Statement statement,
      final Consumer<IngestResponse> acceptor,
      final Consumer<Throwable> errorHandler) {
    state.addHistory("store");
    state.endPreProcess();

    final var atomicOperationContext = state.getAtomicOperationContext();
    if (atomicOperationContext != null) {
      atomicOperationContext.addStatement(statement);
      try {
        final var event = state.getEvent();
        final IngestResponse response =
            new IngestResponse(event.getEventId(), event.getIngestTimestamp());
        acceptor.accept(response);
      } catch (Throwable t) {
        // make this internal server error
        logger.error("Async call response handler threw an exception: ", t);
        errorHandler.accept(t);
      }
      return;
    }

    state.startDbAccess();
    final var isCanceled = new AtomicBoolean(false);
    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          if (isCanceled.get()) {
            return;
          }
          try {
            final var event = state.getEvent();
            final IngestResponse response =
                new IngestResponse(event.getEventId(), event.getIngestTimestamp());
            acceptor.accept(response);
          } catch (Throwable t) {
            // make this internal server error
            logger.error("Async call response handler threw an exception: ", t);
            errorHandler.accept(t);
          }
        },
        (t) -> {
          isCanceled.set(true);
          errorHandler.accept(new ApplicationException("Ingest failed", t));
        });
  }

  @Override
  public void insertEventIgnoreErrors(StreamDesc streamDesc, Event event, ExecutionState state) {
    CompletableFuture.runAsync(
        () -> {
          try {
            CompletableFuture<InsertState> stage;
            final var preprocessors = streamDesc.getPreprocessStages();
            if (preprocessors != null) {
              final var tenantName = streamDesc.getParent().getName();
              final var signalName = streamDesc.getName();
              final var insertState =
                  new InsertState(tenantName, signalName, streamDesc, this, state);
              insertState.setEvent(event);
              stage = CompletableFuture.completedFuture(insertState);
              for (var processor : preprocessors) {
                stage = stage.thenCompose(processor.getProcess()::applyAsync);
              }
            } else {
              stage = CompletableFuture.completedFuture(null);
            }
            stage
                .thenCompose(
                    (none) ->
                        ExecutionHelper.supply(() -> insertInternal(streamDesc, event, state)))
                .thenRun(() -> state.markDone())
                .exceptionally(
                    (t) -> {
                      if (t instanceof TimeoutException) {
                        TfosException ex = new TfosException(GenericError.TIMEOUT);
                        logger.warn(
                            "{} failed. error={} status={}\n{}",
                            state.getExecutionName(),
                            ex.getMessage(),
                            ex.getStatus(),
                            state);
                      } else if (t instanceof TfosException) {
                        TfosException ex = (TfosException) t;
                        logger.warn(
                            "{} failed. error={} status={}\n{}",
                            state.getExecutionName(),
                            ex.getMessage(),
                            ex.getStatus(),
                            state);
                      } else if (t.getCause() instanceof TfosException) {
                        TfosException ex = (TfosException) t.getCause();
                        logger.error(
                            "{} failed. error={} status={}\n{}",
                            state.getExecutionName(),
                            ex.getMessage(),
                            ex.getStatus(),
                            state);
                      } else if (t.getCause() instanceof ServiceException) {
                        ServiceException ex = (ServiceException) t.getCause();
                        logger.warn(
                            "{} failed. error={} status={}\n{}",
                            state.getExecutionName(),
                            ex.getMessage(),
                            ex.getResponse().getStatus(),
                            state);
                      } else {
                        // This causes Internal Server Error
                        logger.error(
                            "{} error. error={} status=500\n{}",
                            state.getExecutionName(),
                            t.getMessage(),
                            state,
                            t);
                      }
                      state.markError();
                      return null;
                    });
          } catch (Throwable th) {
            logger.warn("Error while ingesting in ingestEventIgnoreError. ", th);
          }
        },
        state.getExecutor());
  }

  @Override
  @Deprecated
  public void ingestEventsBulk(StreamDesc streamDesc, List<Event> events, ExecutionState state)
      throws ApplicationException, TfosException {
    final TenantDesc tenantDesc = streamDesc.getParent();
    final String tenant = tenantDesc.getName();
    final StreamType streamType = streamDesc.getType();
    if (streamType != StreamType.SIGNAL
        && streamType != StreamType.ROLLUP
        && streamType != StreamType.VIEW) {
      throw new ApplicationException(
          String.format(
              "Stream type %s is not supported for bulk ingest; tenant=%s stream=%s",
              streamType.name(), tenant, streamDesc.getName()));
    }
    SignalCassStream cassStream = (SignalCassStream) getCassStream(streamDesc);
    if (cassStream == null) {
      throw new ApplicationException(
          String.format(
              "Cassandra ingest engine had unresolvable configuration data inconsistency. Could not "
                  + "find streamName=%s, version=%d, parentName=%s, version=%d",
              streamDesc.getName(),
              streamDesc.getVersion(),
              streamDesc.getParent().getName(),
              streamDesc.getParent().getVersion()));
    }

    Queue<Statement> batches = new ConcurrentLinkedQueue<>();

    final Map<Object, List<Statement>> partitions = new HashMap<>();
    final int maxBatch = TfosConfig.getDbIngestBatchSize();
    for (Event event : events) {
      Object partitionKey = cassStream.makePartitionKey(event);
      List<Statement> statements = partitions.get(partitionKey);
      if (statements == null) {
        statements = new ArrayList<>();
        partitions.put(partitionKey, statements);
      } else if (statements.size() == maxBatch) {
        final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batch.addAll(statements);
        batches.add(batch);
        partitions.clear();
      }
      statements.add(cassStream.makeInsertStatement(event));
    }
    // flush accumulated partitions
    for (List<Statement> statements : partitions.values()) {
      if (!statements.isEmpty()) {
        final BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batch.addAll(statements);
        batches.add(batch);
      }
    }

    DataUtils.wait(executeIngestAsync(batches, "ingest bulk", state), "ingest bulk");
  }

  @Override
  public CompletionStage<InsertResponseRecord> insert(InsertState state) {
    try {
      return insertInternal(state.getSignalDesc(), state.getEvent(true), state);
    } catch (TfosException | ApplicationException e) {
      return CompletableFuture.failedStage(e);
    }
  }

  private CompletionStage<InsertResponseRecord> insertInternal(
      StreamDesc signalDesc, Event event, ExecutionState state)
      throws TfosException, ApplicationException {
    state.addHistory("insert{");
    state.addHistory("(fetchConfig");
    CassStream cassStream = getCassStream(signalDesc);
    if (cassStream == null) {
      throw new TfosException(
          String.format(
              "executeOperation: Cassandra had unresolvable configuration data inconsistency. Could "
                  + "not find streamName=%s, version=%d, parentName=%s, version=%d",
              signalDesc.getName(),
              signalDesc.getVersion(),
              signalDesc.getParent().getName(),
              signalDesc.getParent().getVersion()));
    }
    state.addHistory(")(makeStatement");
    final var statement = cassStream.makeInsertStatement(event);
    final var atomicOperationContext = state.getAtomicOperationContext();
    state.endPreProcess();
    if (atomicOperationContext != null) {
      state.addHistory(")(writeTimeIndexingStatement");
      final Statement writeTimeIndexingStatement =
          makeWriteTimeIndexingStatement(signalDesc, event);
      if (writeTimeIndexingStatement != null) {
        atomicOperationContext.addStatement(writeTimeIndexingStatement);
      }
      state.addHistory(")(atomicMutate");
      atomicOperationContext.addStatement(statement);
      state.addRecordsWritten(1);
      atomicOperationContext.addPostOperation(
          () -> {
            state.addHistory(")}");
          });
      return CompletableFuture.completedStage(
          new InsertResponseRecord(event.getEventId(), event.getIngestTimestamp().getTime()));
    } else {
      state.startDbAccess();
      state.addHistory(")(store");
      return cassandraConnection
          .executeAsync(statement, state)
          .thenApply(
              (rows) -> {
                state.addHistory(")}");
                state.addRecordsWritten(1);
                return new InsertResponseRecord(
                    event.getEventId(), event.getIngestTimestamp().getTime());
              });
    }
  }

  private Statement makeWriteTimeIndexingStatement(StreamDesc signalDesc, Event event)
      throws ApplicationException {
    if (signalDesc.getViews() == null) {
      return null;
    }
    for (var viewDesc : signalDesc.getViews()) {
      if (viewDesc.getWriteTimeIndexing() == Boolean.TRUE) {
        final var viewConfigName = AdminUtils.makeViewStreamName(signalDesc, viewDesc);
        final var viewConfig = signalDesc.getParent().getStream(viewConfigName, false);
        final var viewCassStream = getCassStream(viewConfig);
        if (viewCassStream == null) {
          // something is wrong
          continue;
        }
        return viewCassStream.makeInsertStatement(event);
      }
    }
    return null;
  }

  @Override
  public CompletionStage<List<InsertBulkEachResult>> insertBulk(
      StreamDesc streamDesc, List<Event> events, ExecutionState state) {
    state.addHistory("(insertBulk");

    final var builder =
        ConcurrentExecutionController.newBuilder()
            .concurrency(Bios2Config.insertBulkMaxConcurrency());

    final var tenantName = streamDesc.getParent().getName();
    final var streamName = streamDesc.getName();

    final var allResults = new ArrayList<InsertBulkEachResult>();
    final var partialErrorOccurred = new AtomicBoolean(false);

    for (int i = 0; i < events.size(); ++i) {
      final var eachState = new InsertState(tenantName, streamName, streamDesc, this, state);
      eachState.setAtomicOperationContext(state.getAtomicOperationContext());
      final var result = new InsertBulkEachResult();
      allResults.add(result);

      final int index = i;
      final var event = events.get(i);
      final var stage =
          new AsyncExecutionStage<>(eachState) {
            @Override
            public CompletionStage<Void> runAsync() {
              try {
                return insertInternal(streamDesc, event, eachState)
                    .thenAccept(
                        (response) -> {
                          result.setSuccess(response);
                        })
                    .exceptionally(
                        (t) -> {
                          eachState.markError();
                          partialErrorOccurred.set(true);
                          final var cause = t instanceof CompletionException ? t.getCause() : t;
                          if (cause instanceof TfosException) {
                            result.setError(cause);
                            state.addError(cause instanceof DataValidationError);
                            final var ex = (TfosException) cause;
                            ex.replaceMessage(ex.getMessage() + state.makeErrorContext());
                            return null;
                          }
                          throw t != cause ? (CompletionException) t : new CompletionException(t);
                        });
              } catch (TfosException | ApplicationException e) {
                throw new CompletionException(e);
              }
            }
          };

      builder.addStage(stage);
    }

    return builder
        .build()
        .execute()
        .thenApplyAsync(
            (none) -> {
              state.addHistory(")");
              if (partialErrorOccurred.get()) {
                throwInsertBulkError(allResults);
              }
              return allResults;
            },
            state.getExecutor());
  }

  public static void throwInsertBulkError(List<InsertBulkEachResult> results) {
    BiosError overall = null;
    int indexCount = 0;
    final var indexSb = new StringBuilder();
    String delimiter = "";
    for (int i = 0; i < results.size(); ++i) {
      final var result = results.get(i);
      if (!result.isError()) {
        continue;
      }
      if (indexCount < 5) {
        indexSb.append(delimiter).append(i);
        delimiter = ", ";
      } else if (indexCount == 5) {
        indexSb.append(", ...");
      }
      // Update the overall error, takes the first one, but a non-TFOS error flips it to 500
      if (!result.isTfosError()) {
        overall = GenericError.APPLICATION_ERROR;
      } else if (overall == null) {
        overall = ((TfosException) result.getError());
      }
      ++indexCount;
    }
    if (overall == null) {
      // the results are empty; likely to be some serious bug
      overall = GenericError.APPLICATION_ERROR;
    }

    // make the exception and throw
    final var message =
        String.format(
            "Failed to insert %d/%d records (%s). For details, check property 'resultsWithError'",
            indexCount, results.size(), indexSb);
    final InsertBulkErrorResponse response = new InsertBulkErrorResponse(overall, message, results);
    final var e = new InsertBulkFailedServerException(overall, message, response, true);
    throw new CompletionException(e);
  }

  @Override
  @Deprecated
  public void summarize(
      SummarizeState state,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler) {
    state.addHistory("(summarize{");
    executeOperation(
        state,
        errorHandler,
        (cassStream) -> signalSummarizer.summarize(state, cassStream, acceptor, errorHandler));
  }

  @Override
  public CompletionStage<Map<Long, List<Event>>> summarize(SummarizeState state) {
    state.addHistory("(summarize{(fetchConfig");
    final var streamDesc = state.getStreamDesc();
    CassStream cassStream = getCassStream(streamDesc);
    if (cassStream == null) {
      return CompletableFuture.failedStage(
          new TfosException(
              String.format(
                  "executeOperation: Cassandra had unresolvable configuration data inconsistency. Could "
                      + "not find streamName=%s, version=%d, parentName=%s, version=%d",
                  streamDesc.getName(),
                  streamDesc.getVersion(),
                  streamDesc.getParent().getName(),
                  streamDesc.getParent().getVersion())));
    }
    state.addHistory(")");
    final var future = new CompletableFuture<Map<Long, List<Event>>>();
    try {
      signalSummarizer.summarize(
          state, cassStream, future::complete, future::completeExceptionally);
    } catch (TfosException e) {
      future.completeExceptionally(e);
    } catch (ApplicationException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public Map<Long, List<Event>> summarize(SummarizeState state, String caller, long timeoutMillis)
      throws Throwable {
    final Map<Long, List<Event>> result = new HashMap<>();
    final Lock lock = new ReentrantLock();
    final Condition done = lock.newCondition();
    try {
      Consumer<Map<Long, List<Event>>> acceptor =
          (resultFromOperation) -> {
            state.addHistory("}");
            logger.trace("{} was done successfully.\n{}", state.getExecutionName(), state);
            if (resultFromOperation == null) {
              logger.warn("Got null result from summarize! state={}", state);
            } else {
              result.putAll(resultFromOperation);
            }
            lock.lock();
            try {
              done.signal();
            } finally {
              lock.unlock();
            }
          };
      ErrorHandler errorHandler = new ErrorHandler(state, lock, done);

      asyncService.submit(() -> summarize(state, acceptor, errorHandler));

      lock.lock();
      try {
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final var waitUntil = System.currentTimeMillis() + timeoutMillis;
        if (!done.await(timeoutMillis, unit)) {
          logger.error(
              "Summarize timed out; caller={}, state={}, timeout={}ms",
              caller,
              state,
              timeoutMillis);
        }
      } finally {
        lock.unlock();
      }
      errorHandler.verify();
    } catch (ExecutionException e) {
      throw new ApplicationException("Summarize failed", e.getCause());
    }
    return result;
  }

  public static SummarizeState createSummarizeState(
      String executionName,
      final SummarizeRequest summarizeRequest,
      final DataEngine dataEngine,
      final StreamDesc streamDesc,
      ExecutionState parent) {
    final var summarizeState =
        new SummarizeState(
            executionName,
            streamDesc.getParent().getName(),
            streamDesc.getName(),
            new EventFactory() {},
            parent);
    // summarizeState.setDataEngine(dataEngine);
    summarizeState.setStreamDesc(streamDesc);
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);
    return summarizeState;
  }

  @Deprecated
  public static SummarizeState createSummarizeState(
      final SummarizeRequest summarizeRequest,
      final DataEngine dataEngine,
      final StreamDesc streamDesc,
      Executor executor) {
    final var summarizeState =
        new SummarizeState(
            null,
            streamDesc.getParent().getName(),
            streamDesc.getName(),
            new EventFactory() {},
            executor);
    // summarizeState.setDataEngine(dataEngine);
    summarizeState.setStreamDesc(streamDesc);
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);
    return summarizeState;
  }

  @Override
  public void complexQuery(
      ComplexQueryState state,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler) {
    state.addHistory("(complexQuery){");
    executeOperation(
        state,
        errorHandler,
        (cassStream) -> complexQueryEngine.complexQuery(state, cassStream, acceptor, errorHandler));
  }

  @Override
  public CompletableFuture<Void> populateIndexes(
      StreamDesc signalConfig,
      StreamDesc indexConfig,
      Map<Long, Set<List<Object>>> indexes,
      ExecutionState state) {
    IndexCassStream cassIndexConfig = (IndexCassStream) getCassStream(indexConfig);

    // make batches of statements
    Queue<Statement> statements = new ConcurrentLinkedQueue<>();
    for (Map.Entry<Long, Set<List<Object>>> entry : indexes.entrySet()) {
      BatchStatement batch = null;
      int batchCount = 0;
      final int maxBatch = TfosConfig.getDbIngestBatchSize();
      for (List<Object> attributeList : entry.getValue()) {
        if (batch == null || batchCount == maxBatch) {
          batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
          statements.add(batch);
          batchCount = 0;
        }
        ++batchCount;
        batch.add(cassIndexConfig.makeInsertStatement(session, attributeList.toArray()));
      }
    }

    return executeIngestAsync(statements, "populate index", state);
  }

  @Override
  public CompletableFuture<Void> populateContextIndexes(
      StreamDesc contextDesc,
      StreamDesc indexConfig,
      List<Event> newEvents,
      List<Event> deletedEvents,
      ExecutionState state) {

    ContextIndexCassStream cassIndexConfig = (ContextIndexCassStream) getCassStream(indexConfig);
    assert cassIndexConfig != null;

    // make batches of statements
    Queue<Statement> statements = new ConcurrentLinkedQueue<>();
    for (Event event : newEvents) {
      try {
        statements.add(cassIndexConfig.makeInsertStatement(event));
      } catch (ApplicationException e) {
        logger.error("populateContext failed to generate insert statement");
      }
    }

    for (Event event : deletedEvents) {
      try {
        statements.add(cassIndexConfig.makeDeleteStatement(event));
      } catch (ApplicationException e) {
        logger.error("populateContext failed to generate delete statement");
      }
    }
    return executeIngestAsync(statements, "populate context index", state);
  }

  @Override
  public CompletableFuture<FeatureStatusResponse> featureStatus(
      ExecutionState state, FeatureStatusRequest request) {
    logger.info("featureStatus: {}", request);

    final var admin = BiosModules.getAdminInternal();
    try {
      if (request.getStream() == null) {
        throw new InvalidRequestException("Property 'stream' must not be null in the request");
      }
      final var streamDesc = admin.getStream(state.getTenantName(), request.getStream());
      final StreamDesc featureStreamDesc;
      if (request.getFeature() == null) {
        featureStreamDesc = null;
      } else {
        final var featureStreamName =
            AdminUtils.makeRollupStreamName(request.getStream(), request.getFeature());
        featureStreamDesc = streamDesc.getParent().getStream(featureStreamName);
        if (featureStreamDesc == null) {
          throw new CompletionException(
              new NoSuchStreamException(
                  String.format(
                      "Feature stream %s not initialized in stream %s",
                      featureStreamName, request.getStream())));
        }
      }
      return postProcessScheduler
          .getDigestionCoverage(streamDesc, featureStreamDesc, state)
          .thenApply(
              (digestionSpecifier) -> {
                FeatureStatusResponse response = new FeatureStatusResponse();
                response.setDoneSince(digestionSpecifier.getDoneSince());
                response.setDoneUntil(digestionSpecifier.getDoneUntil());
                response.setRefreshRequested(digestionSpecifier.getRequested());
                return response;
              });
    } catch (TfosException e) {
      throw new CompletionException(e);
    }
  }

  @Override
  public CompletableFuture<Void> featureRefresh(
      ExecutionState state, FeatureRefreshRequest request) {
    logger.info("featureRefresh: {}", request);

    final var admin = BiosModules.getAdminInternal();
    try {
      if (request.getStream() == null) {
        throw new InvalidRequestException("Property 'stream' must not be null in the request");
      }
      final var streamDesc = admin.getStream(state.getTenantName(), request.getStream());
      if (streamDesc.getType() != StreamType.CONTEXT) {
        throw new InvalidRequestException("Stream type must be context");
      }
      final StreamDesc featureStreamDesc;
      if (request.getFeature() == null) {
        featureStreamDesc = null;
      } else {
        final var featureStreamName =
            AdminUtils.makeRollupStreamName(request.getStream(), request.getFeature());
        featureStreamDesc = streamDesc.getParent().getStream(featureStreamName);
        if (featureStreamDesc == null) {
          throw new CompletionException(
              new NoSuchStreamException(
                  String.format(
                      "Feature stream %s not initialized in stream %s",
                      featureStreamName, request.getStream())));
        }
      }
      return postProcessScheduler.requestRefresh(streamDesc, featureStreamDesc, state);
    } catch (TfosException e) {
      throw new CompletionException(e);
    }
  }

  @Override
  public CompletionStage<AllContextSynopses> getAllContextSynopses(
      SynopsisRequest request, ExecutionState state) {
    return synopsisExtractor.getAllContextSynopses(request, state);
  }

  @Override
  public CompletionStage<ContextSynopsis> getContextSynopsis(
      SynopsisRequest request, ExecutionState state) {
    return synopsisExtractor.getContextSynopsis(request, state);
  }

  @Override
  public CompletionStage<Void> commit(
      AtomicOperationContext atomicOperationContext, ExecutionState state) {
    if (atomicOperationContext == null || atomicOperationContext.getBatchStatement().size() == 0) {
      return CompletableFuture.completedStage(null);
    }
    state.startDbAccess();
    return cassandraConnection
        .executeAsync(atomicOperationContext.getBatchStatement(), state)
        .thenRunAsync(
            () -> atomicOperationContext.getPostOperations().forEach((task) -> task.run()),
            state.getExecutor());
  }

  @Override
  public void registerDataPublisher(DataPublisher publisher) {
    maintenance.registerPublisher(publisher);
  }

  @Override
  public Event lookupContext(IngestState state, StreamDesc refStream, List<Object> key)
      throws TfosException, ApplicationException {
    return ExecutionHelper.sync(lookupContextAsync(state, refStream, key));
  }

  @Override
  public CompletableFuture<Event> lookupContextAsync(
      ExecutionState state, StreamDesc remoteContext, List<Object> key) {

    // state.processing("fetchConfig");
    String stream = remoteContext.getName();
    if (remoteContext.getType() != StreamType.CONTEXT) {
      return CompletableFuture.failedFuture(
          new TfosException(
              String.format(
                  "Context lookup is called against unsupported stream %s of type %s",
                  stream, remoteContext.getType().name())));
    }

    final var cassStream = getCassStream(remoteContext);
    if (cassStream instanceof ContextCassStream) {
      final var specialRepository = ((ContextCassStream) cassStream).getSpecialRepository();
      if (specialRepository != null) {
        return specialRepository.getContextEntryAsync(state, key).toCompletableFuture();
      }
      final var lookupState = new ContextOpState("context lookup", state);
      lookupState.setTenantName(state.getTenantName());
      lookupState.setStreamName(remoteContext.getName());
      lookupState.setContextDesc(remoteContext);
      return ((ContextCassStream) cassStream)
          .getContextEntryAsync(lookupState, key)
          .toCompletableFuture();
    }

    return CompletableFuture.failedFuture(
        new TfosException(String.format("getting streamConfig %s failed", stream)));
  }

  @Override
  public CompletionStage<Void> select(SelectState state) {
    final var query = state.getQuery();
    if (query.getResponseShape() == ResponseShape.MULTIPLE_RESULT_SETS) {
      return complexQuery2(state);
    } else if (query.hasWindow()) {
      return summarize2(state);
    }
    return simpleExtract2(state);
  }

  private CompletionStage<Void> simpleExtract2(SelectState state) {
    return CompletableFuture.supplyAsync(() -> prepareSelect(state), state.getExecutor())
        .thenCompose(
            (cassStream) ->
                ExecutionHelper.supply(
                    () -> {
                      final var extractState =
                          new ExtractState(
                              "simpleExtract2",
                              state,
                              () -> DynamicServerRecord.newEventFactory(state.getSignalDesc()));
                      extractState.setStreamDesc(state.getSignalDesc());
                      extractState.setInput(
                          new ExtractRequestAdapter(
                              state.getQuery(), state.getDerived(), state.getClientVersion()));
                      extractState.setMetricsTracer(state.getMetricsTracer());
                      extractState.startPreProcess();
                      extractState.setValidated(true);
                      if (queryLogger != null) {
                        extractState.setQueryLoggerItem(queryLogger.newItem(extractState));
                        state.setQueryLoggerItem(extractState.getQueryLoggerItem());
                      }
                      extractState
                          .getQueryLoggerItem()
                          .ifPresent(
                              (item) ->
                                  item.setRequestReceivedTime(
                                      state.getMetricsTracer() != null
                                          ? state.getMetricsTracer().getStartTime()
                                          : Instant.now()));
                      final var future = new CompletableFuture<Void>();
                      extractState.addHistory("(extract{");
                      signalExtractor.extract(
                          extractState,
                          cassStream,
                          (events) -> {
                            extractState.startPostProcess();
                            extractState.addHistory("})(postProcess");
                            // Result handling. Reply from extractEngine.extract() comes as a
                            // completion stage
                            // for a list of events in the previous stage. This stage converts the
                            // events to a
                            // data window.
                            final Map<String, ColumnDefinition> definitions;
                            if (events.isEmpty()) {
                              definitions = Map.of();
                            } else {
                              definitions =
                                  ((ServerRecord.EventAdapter) events.get(0))
                                      .asRecord()
                                      .getDefinitions();
                            }
                            final var response = new SelectResponseRecords(definitions);
                            state.addResponse(response);
                            final var dataWindow =
                                response.getOrCreateDataWindow(state.getQuery().getStartTime());
                            for (var event : events) {
                              // The impl. assumes that the event is of type
                              // ServerRecord.EventAdapter.
                              assert event instanceof ServerRecord.EventAdapter;
                              final var serverRecordEvent = (ServerRecord.EventAdapter) event;
                              dataWindow.add(serverRecordEvent.asRecord());
                            }
                            state.addHistory(")");
                            state.markDone();
                            future.complete(null);
                          },
                          future::completeExceptionally);
                      return future;
                    }));
  }

  private CompletionStage<Void> summarize2(SelectState state) {
    // The method needs a dynamic event factory
    final var eventFactory = DynamicServerRecord.newEventFactory(state.getSignalDesc());
    return CompletableFuture.supplyAsync(() -> prepareSelect(state), state.getExecutor())
        .thenCompose(
            (cassStream) ->
                ExecutionHelper.supply(
                    () -> {
                      final var summarizeState =
                          new SummarizeState("summarize2", eventFactory, state);
                      summarizeState.setStreamDesc(state.getSignalDesc());
                      summarizeState.setInput(
                          SummarizeRequestUtils.toSummarizeRequest(
                              state.getQuery(), state.getDerived(), state.getClientVersion()));
                      summarizeState.setMetricsTracer(state.getMetricsTracer());
                      summarizeState.setValidated(true);
                      summarizeState.setBiosApi();
                      summarizeState.setCompiledSortRequest(
                          state
                              .getDerived()
                              .getComponent(
                                  DerivedQueryComponents.COMPILED_SORT, CompiledSortRequest.class));
                      if (queryLogger != null) {
                        summarizeState.setQueryLoggerItem(queryLogger.newItem(summarizeState));
                        state.setQueryLoggerItem(summarizeState.getQueryLoggerItem());
                      }
                      summarizeState
                          .getQueryLoggerItem()
                          .ifPresent(
                              (item) ->
                                  item.setRequestReceivedTime(
                                      state.getMetricsTracer() != null
                                          ? state.getMetricsTracer().getStartTime()
                                          : Instant.now()));
                      final var future = new CompletableFuture<Void>();
                      signalSummarizer.summarize(
                          summarizeState,
                          cassStream,
                          (result) -> {
                            // convert the result in V1 format to V2 SelectResponseRecords
                            final var definitions = eventFactory.getColumnDefinitions();
                            final var response =
                                new SelectResponseRecords(
                                    definitions != null ? definitions : Map.of());
                            for (var crate : result.entrySet()) {
                              final var timestamp = crate.getKey();
                              if (timestamp >= state.getQuery().getEndTime()) {
                                continue;
                              }
                              final var events = crate.getValue();
                              final var dataWindow = response.getOrCreateDataWindow(timestamp);
                              for (var event : events) {
                                assert event instanceof ServerRecord.EventAdapter;
                                dataWindow.add(((ServerRecord.EventAdapter) event).asRecord());
                              }
                            }
                            state.addResponse(response);
                            future.complete(null);
                          },
                          future::completeExceptionally);
                      return future;
                    }));
  }

  private CompletionStage<Void> complexQuery2(SelectState state) {
    return CompletableFuture.supplyAsync(() -> prepareSelect(state), state.getExecutor())
        .thenCompose(
            (cassStream) ->
                ExecutionHelper.supply(
                    () -> {
                      final var queryState =
                          new ComplexQueryState(
                              state,
                              () -> DynamicServerRecord.newEventFactory(state.getSignalDesc()));
                      queryState.setStreamDesc(state.getSignalDesc());
                      queryState.setInput(
                          SummarizeRequestUtils.toComplexQueryRequest(
                              state.getQuery(), state.getDerived(), state.getClientVersion()));
                      queryState.setValidated(true);
                      if (queryLogger != null) {
                        queryState.setQueryLoggerItem(queryLogger.newItem(queryState));
                        state.setQueryLoggerItem(queryState.getQueryLoggerItem());
                      }
                      queryState
                          .getQueryLoggerItem()
                          .ifPresent(
                              (item) ->
                                  item.setRequestReceivedTime(
                                      state.getMetricsTracer() != null
                                          ? state.getMetricsTracer().getStartTime()
                                          : Instant.now()));
                      final var future = new CompletableFuture<Void>();
                      complexQuery(
                          queryState,
                          (results) -> {
                            // convert the result in V1 style to V2 SelectResponseRecords
                            for (int i = 0; i < results.length; ++i) {
                              final var result = results[i];
                              final var crates = result.getSingleResponse();
                              final var response = new SelectResponseRecords();
                              for (var crate : crates.entrySet()) {
                                final var timestamp = crate.getKey();
                                final var events = crate.getValue();
                                final var dataWindow = response.getOrCreateDataWindow(timestamp);
                                for (var event : events) {
                                  assert event instanceof ServerRecord.EventAdapter;
                                  final var record = ((ServerRecord.EventAdapter) event).asRecord();
                                  dataWindow.add(record);
                                  if (response.getDefinitions() == null) {
                                    response.setDefinitions(record.getDefinitions());
                                  }
                                }
                              }
                              if (response.getDefinitions() == null) {
                                response.setDefinitions(Map.of());
                              }
                              state.addResponse(response);
                            }
                            future.complete(null);
                          },
                          future::completeExceptionally);
                      return future;
                    }));
  }

  private CassStream prepareSelect(SelectState state) throws CompletionException {
    return ExecutionHelper.supply(
        () -> {
          final var signalDesc = state.getSignalDesc();
          state.addHistory("(prepare");
          DataServiceUtils.validateQuery(
              new TfosQueryValidator(signalDesc, QUERY_CONFIG),
              state.getQuery(),
              state.getIndex(),
              state.getDerived());

          final var cassStream = getCassStream(signalDesc);
          if (cassStream == null) {
            throw new TfosException(
                String.format(
                    "executeOperation: Cassandra had unresolvable configuration data inconsistency."
                        + " Could not find streamName=%s, version=%d, parentName=%s, version=%d",
                    signalDesc.getName(),
                    signalDesc.getVersion(),
                    signalDesc.getParent().getName(),
                    signalDesc.getParent().getVersion()));
          }
          state.addHistory(")");
          return cassStream;
        });
  }

  @Override
  public CompletionStage<Void> putContextEntriesAsync(ContextWriteOpState<?, List<Event>> state) {
    final StreamDesc contextDesc = state.getContextDesc();
    final List<Event> entries = state.getInputData(true);

    ContextCassStream cassStream = (ContextCassStream) getCassStream(contextDesc);
    if (cassStream == null) {
      return CompletableFuture.failedStage(
          new ApplicationException(
              String.format(
                  "getting CassStream for context %s.%s failed",
                  state.getTenantName(), contextDesc.getName())));
    }
    if (cassStream.getSpecialRepository() != null) {
      return CompletableFuture.failedStage(
          new PermissionDeniedException(
              state.getUserContext(),
              String.format(
                  "%s not allowed on %s context",
                  state.getExecutionName(), contextDesc.getName())));
    }
    return cassStream
        .putContextEntriesAsync(state, session, entries, state.getPhase(), state.getTimestamp())
        .exceptionally(
            (t) -> {
              final var cause = t instanceof CompletionException ? t.getCause() : t;
              if (cause instanceof TfosException) {
                throw new CompletionException(cause);
              }
              throw new CompletionException(new ApplicationException("putContextEntries", cause));
            });
  }

  @Override
  public void listContextPrimaryKeysAsync(
      StreamDesc contextDesc,
      ContextOpState state,
      Consumer<List<List<Object>>> acceptor,
      Consumer<Throwable> errorHandler) {

    ContextCassStream cassStream = (ContextCassStream) getCassStream(contextDesc);
    if (cassStream == null) {
      errorHandler.accept(
          new ApplicationException(
              String.format("getting CassStream for context %s failed", contextDesc.getName())));
      return;
    }

    cassStream
        .listKeysAsync(state)
        .thenAccept(
            (keys) -> {
              keys.sort(Comparators.CONTEXT_PRIMARY_KEY);
              state.addRecordsRead(keys.size());

              final var tenantName = contextDesc.getParent().getName();
              final var contextName = contextDesc.getName();

              final List<List<Object>> primaryKeys =
                  keys.stream()
                      .map(
                          (key) -> {
                            final var values = new Object[key.size()];
                            for (int i = 0; i < values.length; ++i) {
                              final var attributeDesc =
                                  contextDesc.getPrimaryKeyAttributes().get(i);
                              values[i] =
                                  Attributes.dataEngineToPlane(
                                      key.get(i), attributeDesc, tenantName, contextName);
                            }
                            return List.of(values);
                          })
                      .collect(Collectors.toList());
              acceptor.accept(primaryKeys);
            })
        .exceptionally(
            (t) -> {
              errorHandler.accept(t);
              return null;
            });
  }

  @Override
  public void countContextPrimaryKeysAsync(
      StreamDesc contextDesc,
      ContextOpState state,
      Consumer<Long> acceptor,
      Consumer<Throwable> errorHandler) {
    ContextCassStream cassStream = (ContextCassStream) getCassStream(contextDesc);
    if (cassStream == null) {
      errorHandler.accept(
          new ApplicationException(
              String.format("getting CassStream for context %s failed", contextDesc.getName())));
      return;
    }

    cassStream
        .listKeysAsync(state)
        .thenAccept(
            (keys) -> {
              final long count = keys.size();
              state.addRecordsRead(count);
              acceptor.accept(count);
            })
        .exceptionally(
            (t) -> {
              errorHandler.accept(t.getCause() != null ? t.getCause() : t);
              return null;
            });
  }

  @Override
  public CompletionStage<SelectContextEntriesResponse> selectContextEntriesAsync(
      SelectContextRequest request, ContextOpState state) {
    ContextQueryState queryState = new ContextQueryState();
    final var cassStream = prepareContextSelect(request, queryState, state);
    final var tenantName = state.getTenantName();
    final var contextName = state.getStreamName();
    return contextExtractor
        .selectContextEntriesAsync(cassStream, request, queryState, state)
        .thenApplyAsync(
            (response) -> {
              // convert values of internal type to data plain, such as enum
              final var entries = response.getEntries();
              for (var record : entries) {
                for (var entry : record.entrySet()) {
                  final var attributeName = entry.getKey();
                  final var value = entry.getValue();
                  final var attribute = cassStream.getAttributeDesc(attributeName);
                  if (attribute == null) {
                    // may be some function
                    continue;
                  }
                  final var converted =
                      Attributes.dataEngineToPlane(value, attribute, tenantName, contextName);
                  entry.setValue(converted);
                }
              }
              return response;
            },
            state.getExecutor());
  }

  private ContextCassStream prepareContextSelect(
      SelectContextRequest request, ContextQueryState queryState, ContextOpState state)
      throws CompletionException {
    return ExecutionHelper.supply(
        () -> {
          state.addHistory("(prepare");
          final var contextDesc = state.getContextDesc();
          final ContextCassStream cassStream = (ContextCassStream) getCassStream(contextDesc);
          if (cassStream == null) {
            throw new TfosException(
                String.format(
                    "executeOperation: getting CassStream failed."
                        + " contextName=%s, version=%d, parentName=%s, version=%d",
                    contextDesc.getName(),
                    contextDesc.getVersion(),
                    contextDesc.getParent().getName(),
                    contextDesc.getParent().getVersion()));
          }
          DataServiceUtils.validateQuery(contextDesc, request, queryState);
          state.addHistory(")");
          return cassStream;
        });
  }

  @Override
  @Deprecated
  public List<Event> getContextEntries(List<List<Object>> keys, ContextOpState state)
      throws TfosException, ApplicationException {
    final var future = new CompletableFuture<List<Event>>();
    getContextEntriesAsync(keys, state, future::complete, future::completeExceptionally);

    return DataUtils.wait(future, state.getExecutionName());
  }

  @Override
  public void getContextEntriesAsync(
      List<List<Object>> keys,
      ContextOpState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {

    final StreamDesc context = state.getContextDesc();
    final String tenantName = context.getParent().getName();
    final var contextName = context.getName();
    ContextCassStream cassStream = (ContextCassStream) getCassStream(context);
    if (cassStream == null) {
      errorHandler.accept(
          new ApplicationException(
              String.format(
                  "getting CassStream for context %s.%s failed", tenantName, context.getName())));
      return;
    }
    final var specialRepository = cassStream.getSpecialRepository();
    if (specialRepository != null) {
      specialRepository.getContextEntriesAsync(keys, state, acceptor, errorHandler);
      return;
    }

    final boolean onTheFly = state.getOptions().contains(ContextOpOption.ON_THE_FLY);
    if (onTheFly) {
      if (context.getContextAdjuster() == null) {
        errorHandler.accept(
            new InvalidRequestException(
                "The onTheFly option is not supported by this context; tenant=%s, context=%s",
                tenantName, contextName));
        return;
      }
      final var validationError = context.getContextAdjuster().isValid();
      if (validationError != null) {
        errorHandler.accept(new InvalidRequestException(validationError));
        return;
      }
    }

    cassStream.getContextEntriesAsync(
        state,
        keys,
        (events) -> {
          // Convert internal events to Data Plane types
          // Also this logic clones cached events for output to prevent cached data from being
          // modified.
          for (int i = 0; i < events.length; ++i) {
            Event inEvent = events[i];
            if (inEvent == null) {
              continue;
            }
            Event outEvent = new EventJson();
            outEvent.setIngestTimestamp(inEvent.getIngestTimestamp());
            outEvent.setEventId(inEvent.getEventId());
            for (String attributeKey : inEvent.getAttributes().keySet()) {
              AttributeDesc desc = cassStream.getAttributeDesc(attributeKey);
              final Object returnedValue = inEvent.getAttributes().get(attributeKey);
              final Object finalValue =
                  Attributes.dataEngineToPlane(returnedValue, desc, tenantName, contextName);
              outEvent.getAttributes().put(attributeKey, finalValue);
            }
            events[i] = outEvent;
          }

          if (onTheFly) {
            context
                .getContextAdjuster()
                .adjust(keys, null, Arrays.asList(events), state)
                .whenCompleteAsync(
                    (result, t) -> {
                      if (t != null) {
                        errorHandler.accept(t);
                      } else {
                        acceptor.accept(result);
                      }
                    },
                    state.getExecutor());
          } else {
            acceptor.accept(Arrays.asList(events));
          }
        },
        errorHandler::accept);
  }

  @Override
  public CompletionStage<Void> deleteContextEntriesAsync(
      ContextWriteOpState<?, List<List<Object>>> state) {
    final StreamDesc contextDesc = state.getContextDesc();
    final List<List<Object>> keys = state.getInputData(true);
    ContextCassStream cassStream = (ContextCassStream) getCassStream(contextDesc);
    if (cassStream == null) {
      return CompletableFuture.failedStage(
          new ApplicationException(
              String.format("getting CassStream for context %s failed", contextDesc.getName())));
    }
    if (cassStream.getSpecialRepository() != null) {
      return CompletableFuture.failedStage(
          new PermissionDeniedException(
              state.getUserContext(),
              String.format(
                  "%s not allowed on %s context",
                  state.getExecutionName(), contextDesc.getName())));
    }
    return cassStream.deleteContextEntriesAsync(
        state, session, keys, state.getPhase(), state.getTimestamp());
  }

  @Override
  public CompletionStage<Void> updateContextEntryAsync(
      ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec> state) {
    final StreamDesc context = state.getContextDesc();
    final ContextCassStream cassStream = (ContextCassStream) getCassStream(context);
    if (cassStream == null) {
      return CompletableFuture.failedStage(
          new ApplicationException(
              String.format("getting CassStream for context %s failed", context.getName())));
    }
    if (cassStream.getSpecialRepository() != null) {
      return CompletableFuture.failedStage(
          new PermissionDeniedException(
              state.getUserContext(),
              String.format(
                  "%s not allowed on %s context", state.getExecutionName(), context.getName())));
    }

    final var updateSpec = state.getInputData(true);

    final var getState = new ContextOpState("GetContextEntryForUpdate", state);
    getState.clearPreProcessTimer();
    final List<Object> key = updateSpec.getKey();
    return cassStream
        .getContextEntriesAsync(getState, List.of(key))
        .thenComposeAsync(
            (events) -> {
              if (events.length < 1 || events[0] == null) {
                throw new CompletionException(
                    new TfosException(
                        AdminError.INVALID_PRIMARY_KEY,
                        String.format(
                            "No such primary key; context=%s, key=%s", context.getName(), key)));
              }
              state.addRecordsWritten(1);
              final var event = events[0];
              Event modified = new EventJson();
              final var newId = Generators.timeBasedGenerator().generate();
              modified.setEventId(newId);
              modified.setIngestTimestamp(new Date(Utils.uuidV1TimestampInMillis(newId)));
              for (var attributeName : event.getAttributes().keySet()) {
                modified.set(attributeName, event.get(attributeName));
              }
              updateSpec
                  .getNewAttributes()
                  .forEach((attributeName, value) -> modified.set(attributeName, value));
              return cassStream.putContextEntriesAsync(
                  state, session, List.of(modified), state.getPhase(), state.getTimestamp());
            },
            state.getExecutor());
  }

  @Override
  public CompletionStage<Void> replaceContextAttributesAsync(
      ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec> state) {
    final StreamDesc context = state.getContextDesc();
    final ContextCassStream cassStream = (ContextCassStream) getCassStream(context);
    if (cassStream == null) {
      return CompletableFuture.failedStage(
          new ApplicationException(
              String.format("getting CassStream for context %s failed", context.getName())));
    }
    if (cassStream.getSpecialRepository() != null) {
      return CompletableFuture.failedStage(
          new PermissionDeniedException(
              state.getUserContext(),
              String.format(
                  "%s not allowed on %s context", state.getExecutionName(), context.getName())));
    }

    final var spec = state.getInputData(true);
    return cassStream
        .listKeysAsync(state)
        .thenCompose((keys) -> cassStream.getContextEntriesAsync(state, keys))
        .thenCompose(
            (events) -> {
              List<Event> entries = new ArrayList<>();
              for (Event event : events) {
                final Object existingValue = event.getAttributes().get(spec.getAttributeName());
                if (spec.getOldValue().equals(existingValue)) {
                  Event modified = new EventJson();
                  modified.setEventId(event.getEventId());
                  modified.setIngestTimestamp(event.getIngestTimestamp());
                  modified.setAttributes(new HashMap<>(event.getAttributes()));
                  modified.getAttributes().put(spec.getAttributeName(), spec.getNewValue());
                  entries.add(modified);
                }
              }
              if (entries.isEmpty()) {
                return CompletableFuture.completedStage(null);
              }
              return cassStream.putContextEntriesAsync(
                  state, session, entries, state.getPhase(), state.getTimestamp());
            });
  }

  @Override
  public CompletionStage<Void> clearTimeIndex(
      StreamDesc subStream, long timestamp, ExecutionState state) {
    if (Objects.requireNonNull(subStream).getType() != StreamType.CONTEXT_FEATURE) {
      throw new IllegalArgumentException("Stream type must be CONTEXT_FEATURE");
    }
    final var rollupCassStream = (RollupCassStream) getCassStream(subStream);
    if (rollupCassStream == null) {
      return CompletableFuture.failedFuture(
          new ApplicationException(
              String.format(
                  "CassStream not found; stream=%s, version=%d",
                  subStream.getName(), subStream.getVersion())));
    }
    return rollupCassStream.clearTimeIndex(timestamp, state);
  }

  // Utility methods ////////////////////////////////////////////////////////////

  /**
   * Method to find a CassTenant entry by a TenantConfig instance.
   *
   * @param tenantDesc TenantConfig as a key
   * @return CassTenant object, it not found, null is returned.
   */
  public CassTenant getCassTenant(TenantDesc tenantDesc) {
    return getCassTenant(tenantDesc, false);
  }

  public CassTenant getCassTenant(TenantDesc tenantDesc, boolean logDetailsIfNotFound) {
    if (tenantDesc == null) {
      throw new IllegalArgumentException("tenant config must be non-null");
    }
    String key = tenantDesc.getNormalizedName() + "." + tenantDesc.getVersion();
    final var cassTenant = cassTenants.get(key);
    if (logDetailsIfNotFound && (cassTenant == null)) {
      final var ex =
          new RuntimeException(
              String.format(
                  "CassTenant not found: key=%s, cassTenants=%s, keysPresent=%s",
                  key, System.identityHashCode(cassTenants), cassTenants.keySet()));
      if (TfosConfig.isTestMode()) {
        throw ex;
      } else {
        logger.error("CassTenant fetch error", ex);
      }
    }
    if (deletedTenants.containsKey(key)) {
      logger.warn(
          "Requested CassTenant was found in deletedTenants; key={}, deletedTenants={}",
          key,
          deletedTenants);
    }
    return cassTenant;
  }

  protected void removeCassTenant(TenantDesc tenantDesc) {
    final var key = tenantDesc.getNormalizedName() + "." + tenantDesc.getVersion();
    logger.debug("Removing CassTenant key={}", key);
    if (!cassTenants.containsKey(key)) {
      logger.warn(
          "Attempting to remove tenant that is not present; cassTenants={}, key={}",
          System.identityHashCode(cassTenants),
          key);
    }
    cassTenants.remove(key);
    if (deletedTenants.containsKey(key)) {
      logger.warn(
          "This tenant was previously removed; key={}, timestamp={}", key, deletedTenants.get(key));
    }
    deletedTenants.put(key, StringUtils.tsToIso8601(System.currentTimeMillis()));
  }

  private void setCassTenant(CassTenant cassTenant) {
    if (cassTenant == null || cassTenant.getName() == null) {
      throw new IllegalArgumentException(
          "cass tenant config must not be null and must have a name");
    }
    final var key = cassTenant.getName().toLowerCase() + "." + cassTenant.getVersion();
    logger.debug(
        "Adding CassTenant key={} to cassTenants={}", key, System.identityHashCode(cassTenants));
    cassTenants.put(key, cassTenant);
    if (deletedTenants.containsKey(key)) {
      logger.warn(
          "Newly added CassTenant found in deletedTenants; key={}, time={} " + "deletedTenants={}",
          key,
          deletedTenants.get(key),
          deletedTenants);
    }
  }

  /**
   * Get the collection of available CassTenant objects for maintenance tasks.
   *
   * <p>The returned collection is unmodifiable, the collection is backed by the DataEngineImpl that
   * may change the collection by some operations.
   */
  public Collection<CassTenant> getCassTenants() {
    return Collections.unmodifiableCollection(cassTenants.values());
  }

  /** Generic executor that consumes a CassStream object. */
  private static interface QueryExecutor {
    void execute(CassStream cassStream) throws Throwable;
  }

  /**
   * Generic operation execution method.
   *
   * @param state Execution state for the operation.
   * @param errorHandler Asynchronous error handler.
   * @param operation QueryExecutor that runs queries for the operation.
   */
  private void executeOperation(
      ExecutionState state, Consumer<Throwable> errorHandler, QueryExecutor operation) {
    try {
      state.addHistory("fetchConfig");
      final var streamDesc = state.getStreamDesc();
      CassStream cassStream = getCassStream(streamDesc);
      if (cassStream == null) {
        throw new TfosException(
            String.format(
                "executeOperation: Cassandra had unresolvable configuration data inconsistency. Could "
                    + "not find streamName=%s, version=%d, parentName=%s, version=%d",
                streamDesc.getName(),
                streamDesc.getVersion(),
                streamDesc.getParent().getName(),
                streamDesc.getParent().getVersion()));
      }

      operation.execute(cassStream);

    } catch (Throwable t) {
      if (t instanceof InvalidQueryException || t.getCause() instanceof InvalidQueryException) {
        final var message = CassStream.translateInvalidQueryMessage(t.getMessage());
        errorHandler.accept(new TfosException(EventExtractError.INVALID_QUERY, message));
      } else {
        errorHandler.accept(t);
      }
    }
  }

  private void deleteCassStream(CassTenant cassTenant, CassStream cassStream)
      throws ApplicationException {
    String keyspace = cassTenant.getKeyspaceName();
    String table = cassStream.getTableName();
    try {
      deleteTableUsingQualifiedName(keyspace + "." + table);
    } catch (Throwable t) {
      throw new ApplicationException(
          String.format(
              "Failed to drop table for tenant=%s stream=%s keyspace=%s table=%s",
              cassTenant.getName(), cassStream.getStreamName(), keyspace, table),
          t);
    }
  }

  private void deleteTableUsingQualifiedName(String tableQualifiedName)
      throws ApplicationException {
    try {
      session.execute(
          String.format(CassandraConstants.FORMAT_DROP_TABLE_QUALIFIED_NAME, tableQualifiedName));
    } catch (Throwable t) {
      throw new ApplicationException(
          String.format("Failed to drop table %s", tableQualifiedName), t);
    }
  }

  /**
   * Generic method to run multiple queries for ingestion concurrently.
   *
   * <p>This method blocks until all ingestions are complete. But internally, the executions are run
   * asynchronously with maximum concurrency {@link #maxIngestConcurrency}.
   *
   * @param statements List of statements to execute. This method does not check the returned rows
   *     assuming no rows would be returned. Because of this, any statements must not be retrieval
   *     (i.e., SELECT).
   * @param opName Operation name to identify the caller on errorime.
   */
  private CompletableFuture<Void> executeIngestAsync(
      Queue<Statement> statements, String opName, ExecutionState state) {
    if (statements.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    final int concurrency = Math.min(statements.size(), maxIngestConcurrency);
    CompletableFuture<Void> completion = new CompletableFuture<>();

    final AtomicInteger remaining = new AtomicInteger(statements.size());
    for (int i = 0; i < concurrency; ++i) {
      runIngestStage(statements, remaining, completion, state);
    }

    return completion;
  }

  /**
   * Generic method to run a single asynchronous stage of Cassandra execution.
   *
   * <p>This method is meant to be called by executeIngestAsync where multiple stages would be
   * invoked concurrently.
   *
   * <p>On successful completion, the method invokes another stage of the execution, where a
   * statement is consumed if available.
   *
   * <p>On error, the method notifies the error to the completion future. Later on, any concurrent
   * successful completion would be ignored. Also, no further stages would be invoked after an
   * error.
   *
   * @param statements A queue of statements to consume. This method consumes one statement if
   *     available, else return without doing anything.
   * @param remaining Counter that tracks number of remaining stages to invoke.
   * @param completion Completion tracker future. This is also used for notifying an error.
   */
  private void runIngestStage(
      Queue<Statement> statements,
      AtomicInteger remaining,
      CompletableFuture<Void> completion,
      ExecutionState state) {
    Statement statement = statements.poll();
    if (statement == null) {
      return;
    }
    cassandraConnection.executeAsync(
        statement,
        state,
        (x) -> {
          if (!completion.isCancelled() && !completion.isCompletedExceptionally()) {
            int current = remaining.get();
            while (!remaining.compareAndSet(current, current - 1)) {
              current = remaining.get();
            }
            if (current == 1) { // I'm the last one
              completion.complete(null);
            } else {
              runIngestStage(statements, remaining, completion, state);
            }
          }
        },
        completion::completeExceptionally);
  }

  @Override
  public void extractEvents(
      ExtractState state, Consumer<List<Event>> acceptor, Consumer<Throwable> errorHandler) {
    state.addHistory("(extract){");
    executeOperation(
        state,
        errorHandler,
        (cassStream) -> signalExtractor.extract(state, cassStream, acceptor, errorHandler));
  }

  // AdminChangeListener implementation /////////////////////////////////
  @Override
  public void createTenant(TenantDesc tenantDesc, RequestPhase phase) throws ApplicationException {
    CassTenant cassTenant = getCassTenant(tenantDesc);
    if (cassTenant == null) {
      cassTenant = new CassTenant(tenantDesc);
      if (phase == RequestPhase.INITIAL) {
        final String keyspace = cassTenant.getKeyspaceName();
        try {
          cassandraConnection.createKeyspace(keyspace);
        } catch (ApplicationException e) {
          logger.error(
              "Failed to create tenant, tenant={} version={}({}) keyspace={} error={}",
              tenantDesc.getName(),
              tenantDesc.getVersion(),
              StringUtils.tsToIso8601(tenantDesc.getVersion()),
              keyspace,
              e);
          throw e;
        }
      }
      setCassTenant(cassTenant);
    }

    sketchStore.initializeTenant(
        cassTenant.getKeyspaceName(),
        TenantId.of(cassTenant.getName().toLowerCase(), cassTenant.getVersion()));
    if (phase == RequestPhase.INITIAL) {
      inferenceEngine.clearTenant(tenantDesc.getName());
    }

    // find delta and add or delete streams.
    // We should process them in ascending order of version.
    final List<StreamDesc> streams = tenantDesc.getNonDeletedStreamVersionsInVersionOrder();
    for (final StreamDesc streamDesc : streams) {
      CassStream cassStream = cassTenant.getCassStream(streamDesc);
      if (cassStream == null) {
        cassStream = CassStream.create(cassTenant, streamDesc, this);
        if (cassStream != null) {
          final boolean doCreateTable;
          switch (cassStream.getStreamDesc().getType()) {
            case SIGNAL:
            case CONTEXT:
            case METRICS:
            case ROLLUP:
            case CONTEXT_INDEX:
              doCreateTable = true;
              break;
            case INDEX:
            case VIEW:
              {
                Boolean tableEnabled =
                    cassStream.getStreamDesc().getViews().get(0).getIndexTableEnabled();
                doCreateTable = tableEnabled == Boolean.TRUE;
                break;
              }
            default:
              doCreateTable = false;
          }
          if (doCreateTable) {
            if (phase == RequestPhase.INITIAL) {
              cassStream.setUpTable();
            } else if (!streamDesc.isDeleted()) {
              cassStream.initialize(cassTenant.getKeyspaceName());
            }
            cassandraConnection.verifyTable(
                cassTenant.getKeyspaceName(), cassStream.getTableName());
          }
          cassTenant.putCassStream(cassStream);
        }
        logger.trace("Done putting cassStream {}", streamDesc.getName());
      }
    }
  }

  @Override
  public void deleteTenant(TenantDesc tenantDesc, RequestPhase phase)
      throws ApplicationException, NoSuchTenantException {
    if (phase == RequestPhase.FINAL) {
      maintenance.clearTenant(tenantDesc);
    }
  }

  public void deleteTenantInternal(TenantDesc tenantDesc, RequestPhase phase)
      throws ApplicationException, NoSuchTenantException {
    CassTenant cassTenant = getCassTenant(tenantDesc);
    if (cassTenant == null) {
      throw new NoSuchTenantException(tenantDesc.getName() + "." + tenantDesc.getVersion());
    }
    for (CassStream cassStream : cassTenant.getCassStreams()) {
      if (phase == RequestPhase.INITIAL) {
        deleteCassStream(cassTenant, cassStream);
      }
      cassTenant.removeCassStream(cassStream);
    }
    removeCassTenant(tenantDesc);
  }

  @Override
  public void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
      throws NoSuchTenantException, ApplicationException {
    CassTenant cassTenant = getCassTenant(streamDesc.getParent(), true);
    if (cassTenant == null) {
      throw new NoSuchTenantException(tenantName);
    }

    CassStream cassStream = cassTenant.getCassStream(streamDesc);
    if (cassStream != null) {
      final var currentStreamDesc = cassStream.getStreamDesc();
      if (currentStreamDesc != streamDesc) {
        cassStream = null;
      }
    }
    if (cassStream == null) {
      cassStream = CassStream.create(cassTenant, streamDesc, this);
      if (cassStream != null) {
        final boolean doCreateTable;
        switch (cassStream.getStreamDesc().getType()) {
          case SIGNAL:
          case CONTEXT:
          case METRICS:
          case CONTEXT_INDEX:
          case CONTEXT_FEATURE:
            doCreateTable = true;
            break;
          case ROLLUP:
            {
              doCreateTable = !cassStream.getStreamDesc().getViews().get(0).hasGenericDigestion();
              break;
            }
          case INDEX:
            {
              final var view = cassStream.getStreamDesc().getViews().get(0);
              doCreateTable = view.getIndexTableEnabled() == Boolean.TRUE;
              break;
            }
          case VIEW:
            {
              final var view = cassStream.getStreamDesc().getViews().get(0);
              doCreateTable =
                  view.getIndexTableEnabled() == Boolean.TRUE
                      || view.getWriteTimeIndexing() == Boolean.TRUE;
              break;
            }
          default:
            doCreateTable = false;
        }
        if (doCreateTable) {
          if (phase == RequestPhase.INITIAL) {
            cassStream.setUpTable();
          } else if (!streamDesc.isDeleted()) {
            cassStream.initialize(cassTenant.getKeyspaceName());
          }
        }
        cassTenant.putCassStream(cassStream);
      }
      logger.trace("Done putting cassStream {}", streamDesc.getName());
    }
  }

  @Override
  public void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
      throws NoSuchTenantException, NoSuchStreamException {
    if (phase == RequestPhase.FINAL) {
      maintenance.clearStream(streamDesc);
    }
  }

  // TODO(Naoki): Split to DataEngineMaintenance
  public void maintain(TaskSlots taskSlots) {
    if (TfosConfig.rollupEnabled()) {
      maintainInternal(taskSlots);
    }

    checkCacheOnlyContexts(taskSlots);
  }

  @Override
  public TenantKeyspaces maintainTenantKeyspaces(
      MaintenanceAction action,
      boolean includeDropped,
      Optional<Integer> limit,
      Optional<List<String>> keyspaceNames,
      Optional<List<String>> tenantNames)
      throws TfosException, ApplicationException {
    final var adminMaintenanceInfo = tableMaintainer.retrieveAdminMaintenanceInfo();
    final var keyspaces = tableMaintainer.findKeyspaces(adminMaintenanceInfo);
    final var tenantKeyspaces = new TenantKeyspaces();
    final var tenantNamesLower =
        tenantNames.orElse(List.of()).stream()
            .map((tenant) -> tenant.toLowerCase())
            .collect(Collectors.toUnmodifiableSet());
    var tenantsStream =
        keyspaces.values().stream()
            .limit(limit.isPresent() ? limit.get() : keyspaces.size())
            .filter((keyspace) -> includeDropped ? true : keyspace.getIsExisting())
            .filter(
                (keyspace) ->
                    keyspaceNames.isPresent()
                        ? keyspaceNames.get().contains(keyspace.getKeyspaceName())
                        : true)
            .filter(
                (keyspace) ->
                    !tenantNames.isPresent()
                        || tenantNamesLower.contains(keyspace.getTenantName().toLowerCase()));

    switch (action) {
      case SUMMARIZE:
        {
          final var activeKeyspaces = new AtomicInteger(0);
          final var inactiveKeyspaces = new AtomicInteger(0);
          final var removedKeyspaces = new AtomicInteger(0);
          final var activeTables = new AtomicInteger(0);
          final var inactiveTables = new AtomicInteger(0);
          final var removedTables = new AtomicInteger(0);
          tenantsStream.forEach(
              (keyspace) -> {
                if (keyspace.getIsTenantActive()) {
                  activeKeyspaces.incrementAndGet();
                } else if (keyspace.getIsExisting()) {
                  inactiveKeyspaces.incrementAndGet();
                } else {
                  removedKeyspaces.incrementAndGet();
                }
                try {
                  final var tenantTables =
                      maintainTenantTables(
                          MaintenanceAction.SUMMARIZE, keyspace.getKeyspaceName(), null, null);
                  if (tenantTables.getActiveTablesCount() != null) {
                    activeTables.addAndGet(tenantTables.getActiveTablesCount());
                  }
                  if (tenantTables.getInactiveTablesCount() != null) {
                    inactiveTables.addAndGet(tenantTables.getInactiveTablesCount());
                  }
                  if (tenantTables.getRemovedTablesCount() != null) {
                    removedTables.addAndGet(tenantTables.getRemovedTablesCount());
                  }

                } catch (ApplicationException | TfosException e) {
                  logger.warn(
                      "Error during summarizing keyspace {}", keyspace.getKeyspaceName(), e);
                }
              });
          tenantKeyspaces.setActiveKeyspacesCount(activeKeyspaces.get());
          tenantKeyspaces.setInactiveKeyspacesCount(inactiveKeyspaces.get());
          tenantKeyspaces.setRemovedKeyspacesCount(removedKeyspaces.get());
          tenantKeyspaces.setTotalActiveTablesCount(activeTables.get());
          tenantKeyspaces.setTotalInactiveTablesCount(inactiveTables.get());
          tenantKeyspaces.setTotalRemovedTablesCount(removedTables.get());
        }
        break;
      case SHOW:
        tenantKeyspaces.setTenants(
            tenantsStream
                .map(
                    (keyspace) -> {
                      keyspace.setTenantChangeHistory(
                          getStreamChangeHistory(
                              keyspace.getTenantName(), ".root", keyspace.getTenantVersion()));
                      return keyspace;
                    })
                .collect(Collectors.toUnmodifiableList()));
        break;
      case DROP:
        tenantKeyspaces.setTenants(tenantsStream.collect(Collectors.toUnmodifiableList()));
        try (final WorkerLock.LockEntity lock =
            BiosModules.getWorkerLock().lock(LOCK_TARGET_TABLE_MAINTENANCE)) {
          if (lock == null) {
            throw new TfosException(
                GenericError.BUSY, "Another maintainance task obtains the lock");
          } else {
            final var droppedKeyspaces = new ArrayList<String>();
            for (var tenantKeyspace : tenantKeyspaces.getTenants()) {
              if (tenantKeyspace.getIsTenantActive()) {
                continue;
              }
              final String keyspaceName = tenantKeyspace.getKeyspaceName();
              if (cassandraConnection.verifyKeyspace(keyspaceName)) {
                logger.info(
                    "Dropping keyspace {}; tenant={}, version={}",
                    keyspaceName,
                    tenantKeyspace.getTenantName(),
                    tenantKeyspace.getTenantVersion());
                cassandraConnection.dropKeyspace(keyspaceName);
                droppedKeyspaces.add(keyspaceName);
              }
            }
            if (!droppedKeyspaces.isEmpty()) {
              tenantKeyspaces.setDroppedKeyspaces(droppedKeyspaces);
            }
          }
        } catch (ApplicationException e) {
          logger.error("Error during table maintenance", e);
        }
        break;
      default:
        // no additional actions
        tenantKeyspaces.setTenants(tenantsStream.collect(Collectors.toUnmodifiableList()));
    }

    return tenantKeyspaces;
  }

  @Override
  public TenantTables maintainTenantTables(
      MaintenanceAction action, String keyspace, Integer limit, List<String> tables)
      throws TfosException, ApplicationException {
    final var adminMaintenanceInfo = tableMaintainer.retrieveAdminMaintenanceInfo();
    final var keyspaces = tableMaintainer.findKeyspaces(adminMaintenanceInfo);
    final var tenantInfo = keyspaces.get(keyspace);
    if (tenantInfo == null) {
      throw new TfosException(AdminError.NO_SUCH_ENTITY, "keyspace " + keyspace);
    }

    // fill the fundamental info
    final var tenantTables = new TenantTables();
    tenantTables.setTenantName(tenantInfo.getTenantName());
    tenantTables.setTenantVersion(tenantInfo.getTenantVersion());
    tenantTables.setIsTenantActive(
        tenantInfo.getIsTenantActive() != null ? tenantInfo.getIsTenantActive() : false);
    tenantTables.setIsKeyspaceExisting(tenantInfo.getIsExisting());

    final var activeTablesCount = new AtomicInteger(0);
    final var inactiveTablesCount = new AtomicInteger(0);
    final var removedTablesCount = new AtomicInteger(0);
    final var tablesInfo =
        tableMaintainer.findTablesToDrop(
            adminMaintenanceInfo,
            keyspaces,
            activeTablesCount,
            inactiveTablesCount,
            removedTablesCount,
            keyspace);

    tenantTables.setActiveTablesCount(activeTablesCount.get());
    tenantTables.setInactiveTablesCount(inactiveTablesCount.get());
    tenantTables.setRemovedTablesCount(removedTablesCount.get());

    final Function<Map<String, TableInfo>, Stream<TableInfo>> makeTablesStream =
        (ti) ->
            ti.values().stream()
                .filter(
                    (tableInfo) ->
                        tables != null ? tables.contains(tableInfo.getTableName()) : true)
                .limit(limit != null ? limit : tablesInfo.values().size());

    switch (action) {
      case SUMMARIZE:
        break;
      case LIST:
        tenantTables.setTables(
            makeTablesStream
                .apply(tablesInfo)
                .map(
                    (tableInfo) -> {
                      final var info = new StreamTableInfo();
                      info.setTableName(tableInfo.getTableName());
                      info.setStreamName(tableInfo.getStreamId().getStream());
                      info.setStreamVersion(tableInfo.getStreamId().getVersion());
                      return info;
                    })
                .collect(Collectors.toUnmodifiableList()));
        break;
      case SHOW:
        tenantTables.setTables(
            makeTablesStream
                .apply(tablesInfo)
                .map(this::makeDetailStreamTableInfo)
                .collect(Collectors.toUnmodifiableList()));
        break;
      case DROP:
        tenantTables.setTables(
            makeTablesStream
                .apply(tablesInfo)
                .map(
                    (tableInfo) -> {
                      final var info = new StreamTableInfo();
                      info.setTableName(tableInfo.getTableName());
                      info.setStreamName(tableInfo.getStreamId().getStream());
                      info.setStreamVersion(tableInfo.getStreamId().getVersion());
                      return info;
                    })
                .collect(Collectors.toUnmodifiableList()));
        if (tenantTables.getTables().isEmpty()) {
          break;
        }
        try (final WorkerLock.LockEntity lock =
            BiosModules.getWorkerLock().lock(LOCK_TARGET_TABLE_MAINTENANCE)) {
          if (lock == null) {
            throw new TfosException(
                GenericError.BUSY, "Another maintainance task obtains the lock");
          } else {
            for (var tableInfo : tenantTables.getTables()) {
              final String tableQualifiedName = keyspace + "." + tableInfo.getTableName();
              logger.info(
                  "Dropping table {}; tenant={}({}) stream={}({})",
                  tableQualifiedName,
                  tenantTables.getTenantName(),
                  tenantTables.getTenantVersion(),
                  tableInfo.getStreamName(),
                  tableInfo.getStreamVersion());
              deleteTableUsingQualifiedName(tableQualifiedName);
            }
          }
        } catch (ApplicationException e) {
          logger.error("Error during table maintenance", e);
        }
        break;
      default:
        logger.error("Unknown maintenance action={}", action);
        break;
    }

    return tenantTables;
  }

  @Override
  public ContextMaintenanceResult maintainContext(
      ContextMaintenanceAction action,
      String tenantName,
      String contextName,
      Integer gcGraceSeconds,
      Integer batchSize)
      throws NoSuchTenantException,
          NoSuchStreamException,
          ApplicationException,
          NoSuchEntityException,
          InvalidRequestException {
    final var admin = BiosModules.getAdminInternal();
    final var contextDesc = admin.getStream(tenantName, contextName);
    final var contextCassStream = (ContextCassStream) getCassStream(contextDesc);
    final var keyspaceName = contextCassStream.getKeyspaceName();
    final var tableName = contextCassStream.getTableName();
    final var result = new ContextMaintenanceResult();

    final var keyspaceMetadata =
        cassandraConnection.getCluster().getMetadata().getKeyspace(keyspaceName);
    final var tableMetadata =
        (keyspaceMetadata != null) ? keyspaceMetadata.getTable(tableName) : null;

    result.setTenantName(tenantName);
    result.setContextName(contextName);
    result.setContextVersion(contextDesc.getVersion());
    result.setSchemaVersion(contextCassStream.getSchemaVersion());
    result.setKeyspaceName(keyspaceName);
    result.setTableName(tableName);
    result.setTableExists(tableMetadata != null);

    switch (action) {
      case DESCRIBE:
        if (tableMetadata != null) {
          final var options = tableMetadata.getOptions();
          result.setTtl(options.getDefaultTimeToLive());
          result.setGcGraceSeconds(options.getGcGraceInSeconds());
          final var lastMaintenanceTime = contextCassStream.getLastMaintenanceTime();
          result.setLastMaintenanceTime(StringUtils.tsToIso8601(lastMaintenanceTime));
          result.setLastMaintenanceTimeInMillis(lastMaintenanceTime);
          final var nextToken = contextCassStream.getNextMaintenanceToken();
          result.setNextMaintenanceToken(nextToken);
          result.setMaintenanceProgress(
              (int)
                  (((double) nextToken - (double) Long.MIN_VALUE)
                      / ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE)
                      * 100));
        }
        break;
      case CHANGE_GC_GRACE_SECONDS:
        if (gcGraceSeconds == null) {
          throw new InvalidRequestException(
              "Property 'gcGraceSeconds' has to be specified to modify it");
        }
        if (tableMetadata == null) {
          throw new NoSuchEntityException("context table", keyspaceName + "." + tableName);
        }
        final String statement =
            String.format(
                "ALTER TABLE %s.%s WITH gc_grace_seconds = %d",
                keyspaceName, tableName, gcGraceSeconds);
        cassandraConnection.execute("Changing GC grace seconds", logger, statement);
        final var updatedTableMetadata = keyspaceMetadata.getTable(tableName);
        if (updatedTableMetadata != null) {
          final var options = updatedTableMetadata.getOptions();
          result.setTtl(options.getDefaultTimeToLive());
          result.setGcGraceSeconds(options.getGcGraceInSeconds());
        }
        break;
      case CLEANUP:
        if (tableMetadata == null) {
          throw new NoSuchEntityException("context table", keyspaceName + "." + tableName);
        } else {
          int limit = batchSize != null ? batchSize : 32768;
          final var state =
              new GenericExecutionState("table cleanup", ExecutorManager.getSidelineExecutor());
          final var res =
              contextCassStream.maintenanceMain(
                  0,
                  System.currentTimeMillis(),
                  0,
                  limit,
                  MaintenanceMode.ENABLED,
                  state,
                  sketchStore);
          result.setMaintenanceExecuted(res.isMaintenanceExecuted());
          if (res.isMaintenanceExecuted()) {
            final var nextToken = res.getNextMaintenanceToken();
            result.setNextMaintenanceToken(nextToken);
            result.setMaintenanceProgress(
                (int)
                    (((double) nextToken - (double) Long.MIN_VALUE)
                        / ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE)
                        * 100));
            result.setLastMaintenanceToken(res.getLastMaintenanceToken());
            result.setMaintenanceDone(res.isMaintenanceDone());
            final var lastMaintenanceTime = contextCassStream.getLastMaintenanceTime();
            result.setLastMaintenanceTime(StringUtils.tsToIso8601(lastMaintenanceTime));
            result.setLastMaintenanceTimeInMillis(lastMaintenanceTime);
          } else {
            result.setMaintenanceNotExecutedReason(res.getMaintenanceNotExecutedReason());
          }
        }
        break;
      default:
        logger.error("Unknown context maintenance action={}", action);
        break;
    }

    return result;
  }

  private StreamTableInfo makeDetailStreamTableInfo(TableInfo tableInfo) {
    final var info = new StreamTableInfo();
    info.setTableName(tableInfo.getTableName());
    final var streamName = tableInfo.getStreamId().getStream();
    info.setStreamName(streamName);
    final var streamVersion = tableInfo.getStreamId().getVersion();
    info.setStreamVersion(streamVersion);
    info.setStreamType(tableInfo.getStreamId().getType().toString());
    final var tenantName = tableInfo.getTenantInfo().getTenantName();
    final var parentStreamName = tableInfo.getParentStream();
    info.setParentStream(parentStreamName);
    final var parentStreamVersion = tableInfo.getParentStreamVersion();
    info.setParentStreamVersion(parentStreamVersion);
    if (parentStreamName != null) {
      try {
        final var stream = BiosModules.getAdminInternal().getStream(tenantName, parentStreamName);
        info.setParentStreamVLatest(stream.getVersion());
      } catch (TfosException e) {
        // ignore silently
      }
    }
    if (Set.of(StreamType.INDEX, StreamType.VIEW, StreamType.ROLLUP)
            .contains(tableInfo.getStreamId().getType())
        && parentStreamName != null) {
      String statement =
          "SELECT done_since, done_until FROM tfos_admin.postprocess_records"
              + " WHERE tenant = ? AND signal = ? AND signal_version = ?"
              + " AND substream = ? AND substream_version = ?";
      try {
        final var resultSet =
            cassandraConnection.execute(
                "Fetch rollup record",
                logger,
                statement,
                tenantName,
                parentStreamName,
                parentStreamVersion,
                streamName,
                streamVersion);
        if (!resultSet.isExhausted()) {
          final var row = resultSet.one();
          final var since = row.getTimestamp("done_since").getTime();
          final var until = row.getTimestamp("done_until").getTime();
          info.setRollupRecord(
              String.format(
                  "[%s:%s]", StringUtils.tsToIso8601(since), StringUtils.tsToIso8601(until)));
        }
      } catch (ApplicationException e) {
        logger.warn("Error while querying rollup record: {}", e.toString());
      }
      info.setParentStreamChangeHistory(
          getStreamChangeHistory(tenantName, parentStreamName, parentStreamVersion));
    } else {
      info.setStreamChangeHistory(getStreamChangeHistory(tenantName, streamName, streamVersion));
    }

    return info;
  }

  private List<String> getStreamChangeHistory(
      String tenantName, String streamName, Long targetVersion) {
    String statement =
        "SELECT version, is_deleted FROM tfos_admin.admin"
            + " WHERE tenant = ? AND stream = ? ALLOW FILTERING";
    try {
      final var resultSet =
          cassandraConnection.execute(
              "Get stream change history",
              logger,
              statement,
              tenantName.toLowerCase(),
              streamName.toLowerCase());
      final var streamChangeHistory = new ArrayList<String>();
      while (!resultSet.isExhausted()) {
        final var row = resultSet.one();
        final var version = row.getTimestamp("version").getTime();
        final var isDeleted = row.getBool("is_deleted");
        streamChangeHistory.add(
            String.format(
                "%d, %s, %s,%s",
                version,
                StringUtils.tsToIso8601Millis(version),
                isDeleted,
                Long.valueOf(version).equals(targetVersion) ? " *" : ""));
      }
      return streamChangeHistory;
    } catch (ApplicationException e) {
      logger.warn("Error while querying stream change history: {}", e.toString());
    }
    return null;
  }

  public void maintainInternal(TaskSlots taskSlots) {
    final var now = System.currentTimeMillis();
    final var tableMaintenanceInterval =
        SharedProperties.getLong(PROP_TABLE_MAINTENANCE_INTERVAL, 600000);
    var temp = MaintenanceMode.ENABLED;
    final var maintenanceModeSrc = SharedProperties.get(PROP_TABLE_MAINTENANCE_MODE);
    if (maintenanceModeSrc != null) {
      try {
        temp = MaintenanceMode.valueOf(maintenanceModeSrc.toUpperCase());
      } catch (IllegalArgumentException e) {
        logger.warn(
            "Invalid table maintenance mode in property {}; src={}, error={}",
            PROP_TABLE_MAINTENANCE_MODE,
            maintenanceModeSrc,
            e.toString());
      }
    }
    final var tableMaintenanceMode = temp;
    if (tableMaintenanceMode != MaintenanceMode.DISABLED
        && !tablesDropping.get()
        && (now - lastTableMaintainedTime) >= tableMaintenanceInterval) {
      // Tables dropping by the previous maintenance has done or not started
      final String targetName = LOCK_TARGET_TABLE_MAINTENANCE;
      sidelineTasksExecutorService.submit(
          () -> {
            try (final var ignored = taskSlots.acquireSlot(logger);
                final var lock = BiosModules.getWorkerLock().lock(LOCK_TARGET_TABLE_MAINTENANCE)) {
              if (lock == null) {
                logger.debug("Couldn't obtain lock on the target keyspace {}", targetName);
              } else {
                logger.info("Running table maintenance");
                tablesDropping.set(true);
                tableMaintainer.dropStaleTablesAndKeyspaces(tableMaintenanceMode, maintenance);
              }
              logger.info("Done table maintenance");
            } catch (Throwable t) {
              logger.error("Error during table maintenance task", t);
            } finally {
              tablesDropping.set(false);
              lastTableMaintainedTime = now;
            }
          });
    }
    final var contextMaintenanceInterval =
        SharedProperties.getLong(PROP_CONTEXT_MAINTENANCE_INTERVAL, 120000);
    if ((now - lastContextMaintainedTime) >= contextMaintenanceInterval) {
      logger.info("Running context maintenance");
      final AdminImpl admin = (AdminImpl) BiosModules.getAdminInternal();
      try (final var ignored = taskSlots.acquireSlot(logger)) {
        cleanUpContextEntries(admin);
      } catch (ApplicationException e) {
        logger.error("Error during context maintenance", e);
      } catch (InterruptedException e) {
        logger.error("Context maintenance interrupted", e);
        Thread.currentThread().interrupt();
      }
      logger.info("Done context maintenance");
      lastContextMaintainedTime = now;
    }
  }

  private void cleanUpContextEntries(AdminImpl admin) throws ApplicationException {
    logger.debug("Cleaning up stale context entries");
    final long maintenanceSeconds = SharedProperties.getLong(PROP_CONTEXT_MAINTENANCE_SECONDS, 30L);
    final var timeLimit = System.currentTimeMillis() + maintenanceSeconds * 1000;
    final var progressRecord = SharedProperties.get(propContextMaintenanceProgress);
    final var allTenants = admin.getAllTenants();
    String nextTenant = "";
    String nextContext = "";
    if (progressRecord != null) {
      final var elements = progressRecord.split(":");
      nextTenant = elements.length > 0 ? elements[0] : "";
      nextContext = elements.length > 1 ? elements[1] : "";
      if (!allTenants.contains(nextTenant)) {
        nextTenant = "";
        nextContext = "";
      }
    }
    for (int it = 0; it < allTenants.size(); ++it) {
      final String tenantName = allTenants.get(it);
      if (tenantName.compareTo(nextTenant) < 0) {
        continue;
      }
      final TenantDesc tenantDesc;
      try {
        tenantDesc = admin.getTenant(tenantName);
      } catch (NoSuchTenantException e1) {
        logger.warn(
            "Failed to get tenant descriptor. The tenant is likely to have been removed; tenant={}",
            tenantName);
        continue;
      }
      if (tenantDesc.getStream(nextContext) == null) {
        nextContext = "";
      }
      List<StreamDesc> allContexts =
          tenantDesc.getAllStreams().stream()
              .filter((stream) -> stream.getType() == StreamType.CONTEXT)
              .sorted(Comparator.comparing(StreamConfig::getName))
              .collect(Collectors.toList());
      for (int is = 0; is < allContexts.size(); ++is) {
        if (maintenance.isShutdown()) {
          // stop immediately on shutdown, the maintenance will be replayed
          return;
        }
        final StreamDesc streamDesc = allContexts.get(is);
        if (streamDesc.getName().compareTo(nextContext) < 0) {
          continue;
        }
        if (System.currentTimeMillis() >= timeLimit) {
          return;
        }
        ContextCassStream contextCassStream = (ContextCassStream) getCassStream(streamDesc);
        if (contextCassStream != null) {
          logger.debug(
              "Running maintenance for context {}; tenant={}",
              contextCassStream.getStreamName(),
              tenantName);
          contextCassStream.runMaintenance(sketchStore);
          if (is < allContexts.size() - 1) {
            nextContext = allContexts.get(is + 1).getName();
            nextTenant = tenantName;
          } else {
            nextContext = "";
            if (it < allTenants.size() - 1) {
              nextTenant = allTenants.get(it + 1);
            } else {
              nextTenant = "";
            }
          }
          sharedConfig.setProperty(propContextMaintenanceProgress, nextTenant + ":" + nextContext);
        }
      }
    }
    // All done. clear the last tenant and context
    sharedConfig.setProperty(propContextMaintenanceProgress, ":");
  }

  private void checkCacheOnlyContexts(TaskSlots taskSlots) {
    checkCacheOnlyContexts(taskSlots, BiosModules.getDigestor().getExecutor());
  }

  public CompletableFuture<Void> checkCacheOnlyContexts(TaskSlots taskSlots, Executor executor) {
    if (maintenance.isShutdown()) {
      return CompletableFuture.completedFuture(null);
    }
    final var state = new GenericExecutionState("checkCacheOnlyContexts", executor);
    return taskSlots
        .runAsyncTask(
            () ->
                sharedConfig
                    .getPropertyAsync(PROP_CACHE_ONLY_CONTEXTS, state)
                    .toCompletableFuture()
                    .thenComposeAsync(
                        (conf) -> {
                          final Map<String, Set<String>> cacheOnlyContexts =
                              parseCacheOnlyContexts(conf);
                          if (cacheOnlyContexts.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                          }
                          return enableCacheOnlyContexts(cacheOnlyContexts, state);
                        },
                        executor),
            executor,
            "checkCacheOnlyContexts")
        .toCompletableFuture();
  }

  private CompletableFuture<Void> enableCacheOnlyContexts(
      Map<String, Set<String>> cacheOnlyContexts, ExecutionState state) {
    final AdminImpl admin = (AdminImpl) BiosModules.getAdminInternal();

    final var taskCounter = new AtomicInteger(0);
    final var contexts = new LinkedList<ContextCassStream>();

    for (String tenant : admin.getAllTenants()) {
      final TenantDesc tenantDesc;
      try {
        tenantDesc = admin.getTenant(tenant);
      } catch (NoSuchTenantException e1) {
        logger.warn(
            "Failed to get tenant descriptor. The tenant is likely to have been removed; tenant={}",
            tenant);
        continue;
      }

      String nodeName = Utils.getNodeName();

      List<StreamDesc> allStreams = tenantDesc.getAllStreams();
      for (StreamDesc streamDesc : allStreams) {
        if (streamDesc.getType() == StreamType.CONTEXT) {
          ContextCassStream contextCassStream = (ContextCassStream) getCassStream(streamDesc);
          if (contextCassStream != null) {
            if (isCacheOnlyModeEnabled(tenant, streamDesc.getName(), nodeName, cacheOnlyContexts)) {
              if (contextCassStream.getCacheMode() == ContextCassStream.CacheMode.NORMAL) {
                taskCounter.incrementAndGet();
                contextCassStream.setCacheMode(ContextCassStream.CacheMode.IN_TRANSITION);
                contexts.add(contextCassStream);
              }
            } else if (contextCassStream.getCacheMode() == ContextCassStream.CacheMode.CACHE_ONLY) {
              logger.info(
                  "Exiting cache only mode; tenant={}, context={}",
                  tenant,
                  contextCassStream.getStreamName());
              contextCassStream.setCacheMode(ContextCassStream.CacheMode.NORMAL);
            }
          }
        }
      }
    }

    if (contexts.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return enableCacheOnlyMode(contexts, state);
  }

  private CompletableFuture<Void> enableCacheOnlyMode(
      Queue<ContextCassStream> contexts, ExecutionState state) {

    final var contextCassStream = contexts.poll();
    if (contextCassStream == null) {
      return CompletableFuture.completedFuture(null);
    }

    final var tenant = contextCassStream.getTenantName();
    logger.info(
        "Loading context entries to enter cache-only mode; tenant={}, context={}",
        tenant,
        contextCassStream.getStreamName());
    return contextCassStream
        .startCacheOnlyMode(state)
        .whenComplete(
            (dummy, t) -> {
              if (t == null) {
                logger.info("Loading cache-only contexts is done");
              } else {
                logger.info("Loading cache-only contexts failed", t);
              }
            })
        .thenComposeAsync((none) -> enableCacheOnlyMode(contexts, state), state.getExecutor());
  }

  protected static Map<String, Set<String>> parseCacheOnlyContexts(String conf) {
    if (conf == null) {
      return Map.of();
    }

    final var cacheOnlyContexts = new HashMap<String, Set<String>>();

    final var entries = conf.split(",");
    for (String entry : entries) {
      final var elements = entry.split("/");
      final var streamName = elements[0].trim().toLowerCase();
      if (streamName.isBlank()) {
        continue;
      }
      final var hosts = new HashSet<String>();
      for (int i = 1; i < elements.length; ++i) {
        final var host = elements[i].trim().toLowerCase();
        if (!host.isEmpty()) {
          hosts.add(host);
        }
      }
      cacheOnlyContexts.put(streamName, hosts);
    }

    return cacheOnlyContexts;
  }

  protected static boolean isCacheOnlyModeEnabled(
      String tenantName,
      String contextName,
      String nodeName,
      Map<String, Set<String>> cacheOnlyContexts) {
    final String canonContextName = tenantName.toLowerCase() + "." + contextName.toLowerCase();
    final var hosts = cacheOnlyContexts.get(canonContextName);
    return hosts != null && (nodeName == null || (hosts.isEmpty() || hosts.contains(nodeName)));
  }

  @Override
  public void unload() {
    cassTenants = new ConcurrentHashMap<>();
  }

  public CassandraConnection getCassandraConnection() {
    return cassandraConnection;
  }

  public static class ErrorHandler implements Consumer<Throwable> {
    private final ExecutionState state;
    private final Lock lock;
    private final Condition done;
    private Throwable caught;

    public ErrorHandler(ExecutionState state, Lock lock, Condition done) {
      this.state = state;
      this.lock = lock;
      this.done = done;
      caught = null;
    }

    @Override
    public void accept(Throwable throwable) {
      caught = throwable;
      lock.lock();
      try {
        done.signal();
      } finally {
        lock.unlock();
      }
    }

    /** Check caught exception and throws if anything has been caught. */
    public void verify() throws Throwable {
      if (caught != null) {
        if (caught instanceof InvalidRequestException) {
          logger.warn("Failed: state={}, exception={}", state, caught);
          throw new InvalidRequestException(state.getExecutionName() + " failed: " + state, caught);
        }
        throw new ApplicationException(state.getExecutionName() + " failed: " + state, caught);
      }
    }

    public boolean errorReceived() {
      if (caught != null) {
        return true;
      }
      return false;
    }
  }
}
