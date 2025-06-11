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
package io.isima.bios.data.impl.storage;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_PREFIX_PREV;
import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.isima.bios.data.impl.models.ContextOpOption.IGNORE_CACHE_ONLY;
import static io.isima.bios.data.impl.models.ContextOpOption.IGNORE_SOFT_DELETION;
import static io.isima.bios.data.impl.models.ContextOpOption.SKIP_ENRICHMENT;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.PREFIX_EVENTS_COLUMN;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.CompiledAttribute;
import io.isima.bios.admin.v1.CompiledEnrichment;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.IngestState;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.data.impl.GlobalContextRepository;
import io.isima.bios.data.impl.TenantId;
import io.isima.bios.data.impl.maintenance.DataMaintenanceUtils;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.impl.models.MaintenanceMode;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.AsyncExecutionStage;
import io.isima.bios.execution.ConcurrentExecutionController;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.maintenance.WorkerLock;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.ContextMaintenanceResult;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.Events;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.ObjectListEventValue;
import io.isima.bios.models.Range;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.recorder.SignalRequestType;
import io.isima.bios.service.handler.DataServiceUtils;
import io.isima.bios.service.handler.GlobalContextHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextCassStream extends CassStream {
  private static final Logger logger = LoggerFactory.getLogger(ContextCassStream.class);

  private static final int NO_CHANGE = 0;
  private static final int NEW_ITEM = 1;
  private static final int ANY_CHANGED = 2;

  public enum CacheMode {
    NORMAL,
    IN_TRANSITION,
    CACHE_ONLY,
  }

  // used for auditing a context record
  public enum OperationId {
    INSERT,
    UPDATE,
    DELETE;

    private final String value;

    OperationId() {
      this.value = name().substring(0, 1) + name().substring(1).toLowerCase();
    }

    public String getValue() {
      return value;
    }
  }

  @AllArgsConstructor
  @Getter
  protected class Tokens {
    private final long lastToken;
    private final long nextToken;
    private final boolean done;
  }

  private static final String AUDIT_FIELD_PREFIX = CONTEXT_AUDIT_ATTRIBUTE_PREFIX_PREV;
  private static final String AUDIT_OP_FIELD = CONTEXT_AUDIT_ATTRIBUTE_OPERATION;

  public static final String PROPERTY_CONTEXT_CACHE_SIZE = "prop.contextCacheMaxNumEntries";
  public static final String PROPERTY_CONTEXT_CACHE_LOADING_BATCH_SIZE =
      "prop.contextCacheLoadingBatchSize";

  public static final String PROPERTY_NEXT_TOKEN_PREFIX = "maint.next.";
  public static final String PROPERTY_LAST_TIMESTAMP_PREFIX = "maint.last.";
  public static final String PROPERTY_CONTEXT_MAINTENANCE_BATCH_SIZE =
      "prop.contextMaintenanceBatchSize";
  public static final String PROPERTY_CONTEXT_MAINTENANCE_MODE = "prop.contextMaintenanceMode";
  public static final String PROPERTY_CONTEXT_MAINTENANCE_REVISIT_INTERVAL =
      "prop.contextMaintenanceRevisitIntervalSeconds";

  protected static final String COLUMN_ENTRY_ID = "entry_id";
  static final String COLUMN_WRITE_TIME = "writeTime(" + COLUMN_ENTRY_ID + ")";

  private static final long DELETE_SKETCHES_TIME_MARGIN_MS = 3600000;

  protected final List<String> primaryKeyNames;
  @Getter protected final List<String> keyColumns;
  // String primary key indexes
  protected final List<Integer> stringPrimaryKeyElements;
  // Blob primary key indexes
  protected final List<Integer> blobPrimaryKeyElements;

  private static ContextMetricsCounter contextMetricsCounter;
  private static ObjectName mbNameContextCache;

  private final String logContext;

  private final int contextEntryCacheMaxSize;
  private final ContextCache cache;

  private final String fetchEntryStatementString;
  private PreparedStatement fetchEntryStatement;

  private PreparedStatement preparedDelete;

  @Getter private final String auditSignalName;
  @Getter private final StreamDesc auditSignalDesc;

  @Getter protected final String propertyNextToken;
  @Getter protected final String propertyLastTimestamp;

  protected String queryGetToken;
  protected String querySelectEntriesForPartitionKey;
  protected String queryTrimOldEntries;

  // Special repository that provides context data from a non-DB data source.
  @Getter private GlobalContextHandler specialRepository;

  @Getter @Setter private CacheMode cacheMode = CacheMode.NORMAL;

  private Set<List<Object>> obsoleteEntryKeys;

  /** initializes context cache JMX service. */
  public static void initializeContextCacheService() {
    if (contextMetricsCounter != null) {
      return;
    }
    contextMetricsCounter = getMetrics();

    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      mbNameContextCache =
          new ObjectName("com.tieredfractals.tfos.metrics:type=Metrics,name=Context");
      mbs.registerMBean(contextMetricsCounter, mbNameContextCache);
    } catch (MalformedObjectNameException
        | InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns context metrics counter.
   *
   * @return The metrics counter
   */
  public static ContextMetricsCounter getMetrics() {
    if (contextMetricsCounter == null) {
      contextMetricsCounter = new ContextMetricsCounter();
    }
    return contextMetricsCounter;
  }

  /** Shuts down this module. */
  public static void shutdown() {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      mbs.unregisterMBean(mbNameContextCache);
      contextMetricsCounter = null;
    } catch (MBeanRegistrationException | InstanceNotFoundException e) {
      logger.warn("error while unregistering MBean entry {}", mbNameContextCache, e);
    }
  }

  protected ContextCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);
    // sanity check
    if (streamDesc.getAttributes().size() < 1) {
      throw new IllegalArgumentException(
          "streamDesc for a context must have one attribute at least");
    }

    String nodeName = Utils.getNodeName().toLowerCase();

    final var contextCacheMaxNumEntriesSrc = SharedProperties.get(PROPERTY_CONTEXT_CACHE_SIZE);
    final int limit =
        determineContextCacheLimit(
            contextCacheMaxNumEntriesSrc, getTenantName(), getStreamName(), nodeName);

    contextEntryCacheMaxSize = limit;
    cache =
        new ConcurrentLinkedHashMapContextCache(
            contextEntryCacheMaxSize,
            streamDesc.getAttributes().stream()
                .map((attribute) -> attribute.getName())
                .collect(Collectors.toList()));
    logContext = String.format(" tenant=%s context=%s", getTenantName(), getStreamName());

    // initialize instance specific parameters
    primaryKeyNames = streamDesc.getPrimaryKey();
    final var indexesForString = new ArrayList<Integer>();
    final var indexesForBlob = new ArrayList<Integer>();
    final var keyColumns = new ArrayList<String>();
    for (int i = 0; i < primaryKeyNames.size(); ++i) {
      final var attribute = getAttributeDesc(primaryKeyNames.get(i));
      final var type = attribute.getAttributeType();
      if (type == InternalAttributeType.STRING) {
        indexesForString.add(i);
      } else if (type == InternalAttributeType.BLOB) {
        indexesForBlob.add(i);
      }
      keyColumns.add(attribute.getColumn());
    }
    this.stringPrimaryKeyElements = Collections.unmodifiableList(indexesForString);
    this.blobPrimaryKeyElements = Collections.unmodifiableList(indexesForBlob);
    this.keyColumns = Collections.unmodifiableList(keyColumns);

    // The first non-primary key attribute is used for getting row write time.
    String valueColumnName = "";
    for (var attribute : streamDesc.getAttributes()) {
      final var attributeName = attribute.getName();
      if (!primaryKeyNames.contains(attributeName)) {
        valueColumnName = PREFIX_EVENTS_COLUMN + attributeName.toLowerCase();
        break;
      }
    }

    queryGetToken =
        String.format(
            "SELECT TOKEN(%s) FROM %s.%s WHERE %s = ? LIMIT 1",
            keyColumns.get(0), keyspaceName, tableName, keyColumns.get(0));

    final String selectKeyColumns = keyColumns.stream().collect(Collectors.joining(", "));

    querySelectEntriesForPartitionKey =
        String.format(
            "SELECT %s, %s FROM %s.%s WHERE %s = ?",
            selectKeyColumns, COLUMN_WRITE_TIME, keyspaceName, tableName, keyColumns.get(0));

    fetchEntryStatementString = makeFetchStatementString();

    propertiesImpl = new DefaultCassTableProperties(this);

    // Set special repository if registered.
    specialRepository =
        GlobalContextRepository.getInstance().getContextHandler(streamDesc.getName());

    propertyLastTimestamp = PROPERTY_LAST_TIMESTAMP_PREFIX + keyspaceName + "." + tableName;
    propertyNextToken = PROPERTY_NEXT_TOKEN_PREFIX + keyspaceName + "." + tableName;
    auditSignalName =
        StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, streamDesc.getName());

    if (Boolean.TRUE.equals(streamDesc.getAuditEnabled())) {
      TenantDesc tenantDesc = streamDesc.getParent();
      auditSignalDesc = tenantDesc.getStream(auditSignalName);
    } else {
      auditSignalDesc = null;
    }

    obsoleteEntryKeys = new HashSet<>();
  }

  @Override
  public void initialize(String keyspaceName) throws ApplicationException {
    if (specialRepository != null) {
      // none of below are necessary for a special repository
      return;
    }
    // prepare statement for getting a context entry
    getFetchEntryStatement();

    deleteQueryString = generateDeleteQueryString(keyspaceName);
    preparedDelete =
        BiosModules.getCassandraConnection()
            .createPreparedStatement(deleteQueryString, "deleteStatement", logger);
  }

  PreparedStatement getFetchEntryStatement() throws ApplicationException {
    if (fetchEntryStatement == null) {
      fetchEntryStatement =
          BiosModules.getCassandraConnection()
              .createPreparedStatement(fetchEntryStatementString, "fetchEntryStatement", logger);
    }
    return fetchEntryStatement;
  }

  protected static int determineContextCacheLimit(
      String contextCacheMaxNumEntriesSrc, String tenantName, String contextName, String nodeName) {
    int limit = TfosConfig.contextEntryCacheMaxSize();
    if (contextCacheMaxNumEntriesSrc == null || contextCacheMaxNumEntriesSrc.isBlank()) {
      return limit;
    }

    // parsing the config repeatedly for each context is inefficient, but it's OK
    // since this happens only once on startup
    // syntax: <tenant>.<context>:<limit>[,<tenant>.<contest>:<limit>[..]]
    final var outerEntries = contextCacheMaxNumEntriesSrc.split(",");
    for (final var outer : outerEntries) {
      final var entries = outer.split("/");

      if (!checkNodeName(entries, nodeName)) {
        continue;
      }

      final var entry = entries[0].trim();
      final var elements = entry.trim().split(":");
      if (elements.length != 2) {
        logger.error("Syntax error in {}: {}", PROPERTY_CONTEXT_CACHE_SIZE, entry);
        continue;
      }
      final var tenantContext = elements[0].trim().split("\\.");
      final var limitSrc = elements[1].trim();
      final String tenant;
      final String context;
      if (tenantContext.length != 2) {
        if (elements[0].trim().equals("*")) {
          tenant = "*";
          context = "*";
        } else {
          logger.error("Syntax error in {}: {}", PROPERTY_CONTEXT_CACHE_SIZE, entry);
          continue;
        }
      } else {
        tenant = tenantContext[0].trim();
        context = tenantContext[1].trim();
      }
      if (!tenant.equalsIgnoreCase(tenantName) && !tenant.equals("*")) {
        continue;
      }
      if (!context.equalsIgnoreCase(contextName) && !context.equals("*")) {
        continue;
      }
      try {
        limit = Integer.parseInt(limitSrc);
        logger.info(
            "Setting context cache limitation; tenant={}, context={}, limit={}",
            tenantName,
            contextName,
            limit);
        return limit;
      } catch (NumberFormatException e) {
        logger.error("Syntax error in {}: {}", PROPERTY_CONTEXT_CACHE_SIZE, entry);
      }
    }
    return limit;
  }

  private static boolean checkNodeName(String[] entries, String nodeName) {
    if (entries.length == 1) {
      return true;
    }
    for (int i = 1; i < entries.length; ++i) {
      final var host = entries[i].trim().toLowerCase();
      if (nodeName.equals(host)) {
        return true;
      }
    }
    return false;
  }

  public Set<List<Object>> fetchObsoleteEntryKeys() {
    synchronized (obsoleteEntryKeys) {
      final var result = Set.copyOf(obsoleteEntryKeys);
      obsoleteEntryKeys.clear();
      return result;
    }
  }

  private ContextCache getCache() {
    return cache;
  }

  @Override
  public String makeCreateTableStatement(String keyspaceName) {
    final var delimiter = ", ";
    final var primaryKeyColumns = new StringJoiner(delimiter);
    final var valueColumns = new StringBuilder();

    final String primaryKeyNames = getKeyColumns().stream().collect(Collectors.joining(", "));

    for (final var attribute : streamDesc.getAttributes()) {
      CassAttributeDesc desc = getAttributeDesc(attribute.getName());
      final String column =
          desc.getColumn() + " " + valueTypeToCassDataTypeName(desc.getAttributeType());
      if (this.primaryKeyNames.contains(attribute.getName())) {
        primaryKeyColumns.add(column);
      } else {
        valueColumns.append(delimiter).append(column);
      }
    }

    final long ttl = streamDesc.getTtl() != null ? streamDesc.getTtl() / 1000 : 0;

    final var statement =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s"
                + " (%s, %s timeuuid%s, PRIMARY KEY (%s))"
                + " WITH comment = 'tenant=%s (version=%d), context=%s (version=%d)"
                + " (schemaVersion=%d)' AND default_time_to_live = %d",
            keyspaceName,
            getTableName(),
            primaryKeyColumns,
            COLUMN_ENTRY_ID,
            valueColumns,
            primaryKeyNames,
            getTenantName(),
            getTenantVersion(),
            streamDesc.getName(),
            streamDesc.getVersion(),
            streamDesc.getSchemaVersion(),
            ttl);

    return statement;
  }

  /**
   * Makes a alter table statment by comparing the new schema and existing table.
   *
   * @param columns
   * @return Alter table statement if there are any changes, null otherwise
   */
  private String makeAlterTableStatement(Map<String, String> columns) throws ApplicationException {
    final var statement =
        new StringBuilder(
            String.format("ALTER TABLE %s.%s", cassTenant.getKeyspaceName(), getTableName()));

    final var add = new StringJoiner(", ", " ADD (", ")");
    String alter = null;
    final int hasAdd = 1;
    final int hasAlter = 2;
    int flags = 0;
    for (var attrDesc : streamDesc.getAttributes()) {
      CassAttributeDesc desc = attributeTable.get(attrDesc.getName().toLowerCase());
      final String columnName = desc.getColumn();
      final var columnType = valueTypeToCassDataType(desc.getAttributeType());
      final var existingColumnType = columns.get(columnName);
      if (existingColumnType == null) {
        add.add(columnName + " " + columnType);
        flags |= hasAdd;
      } else {
        if (!existingColumnType.equals(columnType.toString())) {
          if (alter != null) {
            throw new ApplicationException("We can't alter two attributes at a time for now");
          }
          alter = String.format(" ALTER %s TYPE %s", columnName, columnType);
          flags |= hasAlter;
        }
      }
    }
    // TODO(BIOS-4432): Handle dropping the column

    if (flags == 0) {
      // No change, we don't alter the table
      return null;
    }

    if ((flags & hasAdd) != 0) {
      statement.append(add);
    }
    if ((flags & hasAlter) != 0) {
      statement.append(alter);
    }

    return statement.toString();
  }

  @Override
  public void setUpTable() throws ApplicationException {
    if (specialRepository != null) {
      // table is not necessary for a special repository
      return;
    }

    final String tenant = getTenantName();
    final String stream = streamDesc.getName();
    final String keyspace = cassTenant.getKeyspaceName();

    fetchEntryStatement = null;

    final var columns =
        cassandraConnection.getTableColumns(
            getKeyspaceName(),
            getTableName(),
            logger,
            (name) -> name.startsWith(PREFIX_EVENTS_COLUMN));

    final String op;
    final String statement;
    if (columns == null) {
      op = "Creating";
      statement = makeCreateTableStatement(keyspace);
    } else {
      op = "Altering";
      statement = makeAlterTableStatement(columns);
    }

    if (statement == null) {
      // This happens when the table already exists and there is nothing to alter.
      return;
    }

    RetryHandler retryHandler =
        new RetryHandler(
            columns == null ? "create table" : "alter table",
            logger,
            String.format(
                "tenant=%s, stream=%s, version=%d, keyspace=%s, table=%s, statement=%s",
                tenant,
                stream,
                streamDesc.getSchemaVersion(),
                keyspace,
                getTableName(),
                statement));
    while (true) {
      try {
        session.execute(new SimpleStatement(statement).setConsistencyLevel(ConsistencyLevel.ALL));
        logger.info(
            "{} table done; tenant={}, stream={}, version={}({}), keyspace={}, table={}",
            op,
            tenant,
            stream,
            streamDesc.getSchemaVersion(),
            StringUtils.tsToIso8601(streamDesc.getSchemaVersion()),
            keyspace,
            getTableName());
        break;
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  @Override
  protected String generateIngestQueryString(final String keyspaceName) {
    final StringBuilder statementBuilder =
        new StringBuilder(
            String.format("INSERT INTO %s.%s (%s", keyspaceName, tableName, COLUMN_ENTRY_ID));
    final StringBuilder valuesPartBuilder = new StringBuilder(") VALUES (?");

    final String delimiter = ", ";
    for (final var attribute : streamDesc.getAttributes()) {
      CassAttributeDesc desc = attributeTable.get(attribute.getName().toLowerCase());
      statementBuilder.append(delimiter).append(desc.getColumn());
      valuesPartBuilder.append(delimiter).append("?");
    }

    statementBuilder.append(valuesPartBuilder).append(") USING TIMESTAMP ?");
    return statementBuilder.toString();
  }

  @Override
  protected String generateDeleteQueryString(final String keyspaceName) {
    final var where = new StringJoiner(" AND ");

    for (var primaryKeyAttributeName : streamDesc.getPrimaryKey()) {
      CassAttributeDesc desc = attributeTable.get(primaryKeyAttributeName.toLowerCase());
      where.add(desc.getColumn() + " = ?");
    }

    return String.format(
        "DELETE FROM %s.%s USING TIMESTAMP ? WHERE %s", keyspaceName, tableName, where);
  }

  /**
   * Returns primary key name.
   *
   * @return primary key name
   */
  public List<String> getPrimaryKeyNames() {
    return primaryKeyNames;
  }

  public CompletionStage<List<List<Object>>> listKeysAsync(ContextOpState state) {
    final List<List<Object>> outKeys = new ArrayList<>();
    final int maxBatchSize = 32768;

    state.endPreProcess();

    try {
      final var iterationState =
          new ContextIterationState(state.getExecutionName(), state)
              .setPrimaryKeyConsumer(outKeys::add);
      final var contextKeyRetriever =
          new ContextEntryRetriever(
              this, iterationState, maxBatchSize, ContextEntryRetriever.Type.ONLY_PRIMARY_KEY);
      return iterateRows(contextKeyRetriever, iterationState).thenApply((none) -> outKeys);
    } catch (ApplicationException e) {
      return CompletableFuture.failedStage(e);
    }
  }

  /**
   * Insert entries into the audit signal.
   *
   * <p>This method does not wait for
   *
   * @param state
   * @param auditEventsSrc List of audit events to be inserted in audit signal
   */
  private CompletionStage<Void> insertAuditEvents(
      ExecutionState state, Collection<Event> auditEventsSrc) {
    if (auditEventsSrc.isEmpty()) {
      return CompletableFuture.completedStage(null);
    }
    final var auditEvents = List.copyOf(auditEventsSrc);
    state.logActivity("----- ContextCassStream.insertAuditEvents ENTER -----");
    state.logActivity("events: %s", auditEvents);
    // TODO(Naoki): Figure out the way to measure total data size of the auditEvents
    final long bytesWritten = 0;
    final var signalName = auditSignalDesc.getName();
    final var userContext = state.getUserContext();
    final var recorder =
        BiosModules.getMetrics()
            .getRecorder(
                getTenantName(),
                signalName,
                userContext.getAppName(),
                userContext.getAppType(),
                SignalRequestType.INSERT_AUDIT);
    final var futures = new CompletableFuture[auditEvents.size()];
    final var metricsTracer = new OperationMetricsTracer(bytesWritten);
    metricsTracer.attachRecorder(recorder);
    final var localRecorder = metricsTracer.getLocalRecorder(signalName);
    for (int i = 0; i < auditEvents.size(); ++i) {
      final int index = i;
      final var event = auditEvents.get(index);
      final var initInsertionState =
          new IngestState(
              localRecorder, getTenantName(), signalName, dataEngine.getExecutor(), dataEngine);
      initInsertionState.setEvent(event);
      initInsertionState.setStreamDesc(auditSignalDesc);

      var enrichmentChain = CompletableFuture.completedFuture(initInsertionState);
      // enrich if configured
      final var enrichments = auditSignalDesc.getPreprocessStages();
      if (enrichments != null) {
        for (var enrichmentStage : enrichments) {
          enrichmentChain = enrichmentChain.thenApply(enrichmentStage.getProcess());
        }
      }

      futures[i] =
          enrichmentChain
              .thenCompose(
                  (insertionState) -> {
                    insertionState.setAtomicOperationContext(state.getAtomicOperationContext());
                    final var future = new CompletableFuture<Void>();
                    dataEngine.ingestEvent(
                        insertionState,
                        // acceptor
                        (response) -> {
                          localRecorder.getNumWrites().increment();
                          future.complete(null);
                        },
                        // error handler
                        (t) -> future.completeExceptionally(t));
                    return future;
                  })
              .exceptionally(
                  (t) -> {
                    logger.error("Error happened while storing a context audit; error={}", t);
                    localRecorder.getNumTransientErrors().increment();
                    return null;
                  });
    }
    return CompletableFuture.allOf(futures)
        .thenRun(
            () -> {
              metricsTracer.stop(0);
              state.logActivity("----- ContextCassStream.insertAuditEvents DONE -----");
            });
  }

  @Override
  public Statement makeInsertStatement(final Event event) throws ApplicationException {
    final int numExtraColumns = 2; // eventId and timestamp
    final Object[] values = new Object[streamDesc.getAttributes().size() + numExtraColumns];
    final var eventId = event.getEventId();
    int index = 0;
    values[index++] = eventId;
    final long timestampUs = Utils.uuidV1TimestampInMicros(eventId);
    event.setIngestTimestamp(new Date(timestampUs / 1000));
    index = populateIngestValues(index, event, streamDesc.getAttributes(), values);
    values[index] = timestampUs;
    final PreparedStatement prepared = getPreparedIngest();
    if (prepared != null) {
      return prepared.bind(values);
    } else {
      final var statement = getIngestQueryString();
      return new SimpleStatement(statement, values);
    }
  }

  public Statement makeDeleteStatement(final Event event, final Long timestampUs)
      throws ApplicationException {
    final int numExtraColumns = 1; // timestamp
    final Object[] values = new Object[primaryKeyNames.size() + numExtraColumns];
    int index = 0;
    event.setIngestTimestamp(new Date(timestampUs / 1000));
    values[index++] = timestampUs;
    populateDeleteValues(index, event, values);
    if (preparedDelete != null) {
      return preparedDelete.bind(values);
    } else {
      return new SimpleStatement(getDeleteQueryString(), values);
    }
  }

  private int populateDeleteValues(final int start, final Event event, final Object[] values)
      throws ApplicationException {
    int index = start;
    for (var attributeDesc : streamDesc.getPrimaryKeyAttributes()) {
      final String primaryKeyAtributeName = attributeDesc.getName();
      Object value = event.get(primaryKeyAtributeName);
      if (value == null) {
        value = attributeDesc.getInternalDefaultValue();
      }
      values[index++] = dataEngineToCassandra(value, attributeDesc);
    }
    return index;
  }

  @Override
  public List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      Collection<CassAttributeDesc> attributes,
      Long startTime,
      Long endTime) {
    return null;
  }

  public CompletableFuture<Event> getContextEntryAsync(ContextOpState state, List<Object> key) {
    final List<List<Object>> keys = List.of(key);
    return getContextEntriesAsync(state, keys).thenApply((events) -> events[0]);
  }

  public CompletableFuture<Event[]> getContextEntriesAsync(
      ContextOpState state, List<List<Object>> keys) {
    final var future = new CompletableFuture<Event[]>();
    getContextEntriesAsync(state, keys, future::complete, future::completeExceptionally);
    return future;
  }

  public void getContextEntriesAsync(
      ContextOpState state,
      List<List<Object>> keys,
      Consumer<Event[]> acceptor,
      Consumer<Throwable> errorHandler) {
    Event[] events = new Event[keys.size()];
    if (events.length == 0) {
      acceptor.accept(events);
      return;
    }

    state.logActivity("----- ContextCassStream.getContextEntriesAsync ENTER ------");
    state.logActivity("keys: %s", keys);

    // parse options
    final var options = state.getOptions();
    final boolean doCountMetrics = !options.contains(ContextOpOption.SKIP_COUNT_METRICS);
    final boolean ignoreCacheOnly = options.contains(IGNORE_CACHE_ONLY);
    final boolean skipEnrichment = options.contains(SKIP_ENRICHMENT);
    final boolean ignoreSoftDeletion = options.contains(IGNORE_SOFT_DELETION);

    Map<List<Object>, Integer> toQuery = null;
    // try cache first
    for (int i = 0; i < keys.size(); ++i) {
      final List<Object> key = keys.get(i);
      if (key == null || key.size() != primaryKeyNames.size()) {
        errorHandler.accept(new InvalidRequestException("Size of primaryKey[%d] is invalid", i));
        return;
      }
      Event event = getCache().get(key);
      if (doCountMetrics) {
        contextMetricsCounter.cacheLookedUp();
      }
      if (event != null) {
        state.logActivity("  key=%s found entry", key);
        events[i] = event;
        if (doCountMetrics) {
          contextMetricsCounter.cacheHit();
        }
      } else {
        state.logActivity("  key=%s entry not found", key);
        events[i] = null;
        if (cacheMode == CacheMode.CACHE_ONLY && !ignoreCacheOnly) {
          continue;
        }
        if (hasEmptyElement(key)) {
          continue;
        }
        state.logActivity("  key=%s entry to be queried", key);
        if (toQuery == null) {
          toQuery = new HashMap<>();
        }
        toQuery.put(key, i);
      }
    }

    state.endPreProcess();
    if (toQuery == null) {
      // no need to make queries
      state.logActivity("nothing to query, going to post processing");
      postProcessGetContextEntries(events, state, skipEnrichment, acceptor, errorHandler);
      return;
    }

    final var queryFuture = new CompletableFuture<>();
    final var optionalTimer = state.startStorageAccess();

    state.logActivity("to query: %s", toQuery);
    final int concurrency = 64;
    final var queryExecutor = new AsyncQueryExecutor(session, concurrency, state.getExecutor());
    for (var queryEntry : toQuery.entrySet()) {
      final var key = queryEntry.getKey();
      final var index = queryEntry.getValue();
      final Statement boundStatement;
      try {
        final Object[] components = key.toArray();
        boundStatement = getFetchEntryStatement().bind(components);
      } catch (ApplicationException e) {
        errorHandler.accept(e);
        return;
      }
      queryExecutor.addStage(
          new AsyncQueryStage() {
            @Override
            public Statement getStatement() {
              return boundStatement;
            }

            @Override
            public void handleResult(ResultSet results) {
              // pick up only the first entry since it has the latest timestamp
              if (!results.isExhausted()) {
                final Row row = results.one();
                final Event event = new EventJson();
                UUID entryId = row.getUUID(COLUMN_ENTRY_ID);
                event.setEventId(entryId);
                long writeTimeEpochMillis = row.getLong(COLUMN_WRITE_TIME) / 1000;
                logger.trace("{} getContextEntries: {}", LocalDateTime.now(), writeTimeEpochMillis);
                event.setIngestTimestamp(new Date(writeTimeEpochMillis));
                for (final var attribute : streamDesc.getAttributes()) {
                  CassAttributeDesc desc = attributeTable.get(attribute.getName().toLowerCase());
                  Object value = row.getObject(desc.getColumn());
                  value = cassandraToDataEngine(value, desc);
                  event.getAttributes().put(desc.getName(), value);
                }
                final var keyValues = makePrimaryKey(event);
                getCache().put(keyValues, event, writeTimeEpochMillis);
                state.logActivity("  retrieved: %s", event);
                if (events != null) {
                  events[index] = event;
                }
              }
            }

            @Override
            public void handleError(Throwable t) {
              queryFuture.completeExceptionally(t);
            }

            @Override
            public void handleCompletion() {
              optionalTimer.ifPresent((timer) -> timer.commit());
              queryFuture.complete(null);
            }
          });
    }

    queryExecutor.execute();

    queryFuture
        .thenRun(
            () ->
                postProcessGetContextEntries(events, state, skipEnrichment, acceptor, errorHandler))
        .exceptionally(
            (t) -> {
              state.markError();
              errorHandler.accept(t);
              return null;
            });
  }

  /** Checks if a primary key has an empty element. */
  private boolean hasEmptyElement(List<Object> primaryKey) {
    for (var index : stringPrimaryKeyElements) {
      if (((String) primaryKey.get(index)).isEmpty()) {
        return true;
      }
    }
    for (var index : blobPrimaryKeyElements) {
      if (!((ByteBuffer) primaryKey.get(index)).hasRemaining()) {
        return true;
      }
    }
    return false;
  }

  private void postProcessGetContextEntries(
      Event[] events,
      ContextOpState state,
      boolean skipEnrichments,
      Consumer<Event[]> acceptor,
      Consumer<Throwable> errorHandler) {

    if (streamDesc.getCompiledEnrichments() == null || skipEnrichments) {
      acceptor.accept(finalizeGetContextEntries(events, state));
      return;
    }

    CompletionStage<Void> currentStage = CompletableFuture.completedStage(null);
    for (int i = 0; i < events.length; i++) {
      if (events[i] == null) {
        continue;
      }
      final int index = i;
      currentStage =
          currentStage.thenCompose(
              (x) ->
                  createEventWithEnrichmentsAsync(session, events[index], state)
                      .thenAccept((event) -> events[index] = event));
    }

    currentStage
        .thenAccept(
            (x) -> {
              acceptor.accept(finalizeGetContextEntries(events, state));
            })
        .exceptionally(
            (t) -> {
              state.markError();
              errorHandler.accept(t);
              return null;
            });
  }

  private Event[] finalizeGetContextEntries(Event[] events, ContextOpState state) {
    // If the table schema has been altered, old entries have null values for new
    // attributes. Fill the value here in the case.
    final boolean isSchemaAltered = !streamDesc.getVersion().equals(streamDesc.getSchemaVersion());

    for (int i = 0; i < events.length; ++i) {
      final var event = events[i];
      if (event == null) {
        continue;
      }
      state.addRecordsRead(1);
      if (isSchemaAltered) {
        for (var attrDesc : streamDesc.getAttributes()) {
          final Object value = event.get(attrDesc.getName());
          if (value == null) {
            event.set(attrDesc.getName(), attrDesc.getInternalDefaultValue());
          }
        }
      }
    }
    state.logActivity("----- ContextCassStream.getContextEntriesAsync DONE ------");
    return events;
  }

  public CompletionStage<SelectContextEntriesResponse> genericSelectAsync(
      SelectContextRequest request,
      String statement,
      ContextQueryState queryState,
      long dbSelectContextLimit,
      ContextOpState state)
      throws ApplicationException {

    return getCassandraConnection()
        .executeAsync(new SimpleStatement(statement), state)
        .thenComposeAsync(
            (resultSet) -> {
              final Iterator<Row> rowIterator = resultSet.iterator();
              final List<Map<String, Object>> records = new ArrayList<>();

              long recordsFetched = 0;
              while (rowIterator.hasNext()) {
                recordsFetched++;
                if (recordsFetched > dbSelectContextLimit) {
                  throw new CompletionException(
                      new InvalidRequestException(
                          "Scale of the query was too large to process."
                              + " Please downsize the query or consider creating an index; context="
                              + request.getContext()));
                }
                Row row = rowIterator.next();
                boolean filterPass = true;
                for (var filterElement : queryState.getFilterElements()) {
                  if (!filterElement.test(row)) {
                    filterPass = false;
                    break;
                  }
                }
                if (!filterPass) {
                  continue;
                }
                HashMap<String, Object> record = new HashMap<>();
                for (final var attribute : streamDesc.getAttributes()) {
                  CassAttributeDesc desc = attributeTable.get(attribute.getName().toLowerCase());
                  Object value = row.getObject(desc.getColumn());
                  value = cassandraToDataEngine(value, desc);
                  record.put(desc.getName(), value);
                }
                records.add(record);
              }
              final var response = new SelectContextEntriesResponse();
              if (streamDesc.getCompiledEnrichments() != null) {
                final var entries = new ArrayList<Map<String, Object>>();
                // TODO(Naoki): Make the operation concurrent
                CompletionStage<Void> currentStage = CompletableFuture.completedStage(null);
                for (var entry : records) {
                  currentStage =
                      currentStage.thenCompose(
                          (none) ->
                              createEnrichedAttributesAsync(session, entry, state)
                                  .thenAccept((enriched) -> entries.add(enriched)));
                }
                return currentStage.thenApply(
                    (none) -> {
                      response.setEntries(entries);
                      return response;
                    });
              }
              response.setEntries(records);
              return CompletableFuture.completedStage(response);
            },
            state.getExecutor());
  }

  CompletableFuture<List<Event>> getAuditEvents(
      SelectContextRequest request, ContextOpState state, Range timeRange, final long currentTime) {
    if (request.getOnTheFly() != Boolean.TRUE) {
      return CompletableFuture.completedFuture(List.of());
    }

    logger.debug(
        "onTheFly: context={}, indexingDoneUntil={}, currentTime={}",
        streamDesc.getName(),
        StringUtils.tsToIso8601(timeRange.getBegin()),
        StringUtils.tsToIso8601(timeRange.getEnd()));
    // Get the latest events from the audit signal since the time indexes were updated.
    StreamDesc auditSignalDesc =
        BiosModules.getAdminInternal()
            .getStreamOrNull(
                streamDesc.getParent().getName(),
                StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, streamDesc.getName()));
    if (auditSignalDesc == null) {
      throw new CompletionException(
          new ApplicationException(
              "Failed to get audit signal for onTheFly query; check audit enabled for context "
                  + streamDesc.getName()));
    }
    CassStream auditSignalCassStream = cassTenant.getCassStream(auditSignalDesc);
    ExtractRequest auditRequest = new ExtractRequest();
    auditRequest.setStartTime(timeRange.getBegin());
    auditRequest.setEndTime(timeRange.getEnd());
    final var extractState = new ExtractState("getAuditEvents", state, () -> new EventFactory() {});
    extractState.setTenantName(streamDesc.getParent().getName());
    extractState.setStreamName(auditSignalDesc.getName());
    extractState.setInput(auditRequest);
    extractState.setStreamDesc(auditSignalDesc);
    try {
      return dataEngine.getSignalExtractor().extract(extractState, auditSignalCassStream);
    } catch (TfosException | ApplicationException e) {
      throw new CompletionException(e);
    }
  }

  /**
   * Returns a newly created event object including all attributes and previous context attributes.
   */
  private Event createAuditEvent(
      Event newEntry, Event oldEntry, UUID auditEventId, OperationId opId) {
    Event event = Events.createEvent(auditEventId);
    for (AttributeDesc attrDesc : streamDesc.getAttributes()) {
      String attributeName = attrDesc.getName();
      var newValue = newEntry.get(attributeName);
      event.set(attributeName, newValue);
      final var prevAttributeName =
          StringUtils.prefixToCamelCase(AUDIT_FIELD_PREFIX, attributeName);
      final Object oldValue;
      if (oldEntry == null) {
        oldValue = newValue;
      } else {
        var temp = oldEntry.get(attributeName);
        // The value may be null when the attribute is a newly-added one by context mod
        if (temp == null) {
          temp = getAttributeDesc(attributeName).getInternalDefaultValue();
        }
        oldValue = temp;
      }
      event.set(prevAttributeName, oldValue);
    }
    event.set(AUDIT_OP_FIELD, opId.getValue());
    return event;
  }

  private CompletionStage<Event> createEventWithEnrichmentsAsync(
      Session session, Event inEvent, ContextOpState state) {
    return createEnrichedAttributesAsync(session, inEvent.getAttributes(), state)
        .thenApply(
            (attributes) -> {
              if (attributes == null) {
                return null;
              }
              return new EventJson(inEvent.getEventId(), inEvent.getIngestTimestamp(), attributes);
            });
  }

  private CompletableFuture<Map<String, Object>> createEnrichedAttributesAsync(
      Session session, Map<String, Object> inAttributes, ContextOpState state) {
    return ExecutionHelper.supply(
        () -> {
          final var outAttributes = new HashMap<>(inAttributes);

          CompletableFuture<Map<String, Object>> currentFuture =
              CompletableFuture.completedFuture(outAttributes);

          for (final var enrichment : streamDesc.getCompiledEnrichments()) {
            // Processing for enriched attributes depends on the kind of enrichment.
            switch (enrichment.getEnrichmentKind()) {
              case SIMPLE_VALUE:
                currentFuture =
                    currentFuture.thenCompose(
                        (attrs) ->
                            simpleContextEnrichment(
                                enrichment, enrichment.getForeignKey(), attrs, state));
                break;
              case VALUE_PICK_FIRST:
                currentFuture =
                    currentFuture.thenCompose(
                        (attrs) ->
                            valuePickUpFirstContextEnrichment(
                                enrichment, enrichment.getForeignKey(), attrs, state));
                break;

              default:
                break;
            }
          }
          return currentFuture;
        });
  }

  private CompletableFuture<Map<String, Object>> simpleContextEnrichment(
      CompiledEnrichment enrichment,
      List<String> foreignKey,
      Map<String, Object> outAttributes,
      ContextOpState state) {

    if (outAttributes == null) {
      return CompletableFuture.completedFuture(null);
    }

    // Get the foreign key value we need to lookup.
    final List<Object> foreignKeyValues = DataUtils.makeCompositeKey(foreignKey, outAttributes);
    if (!DataServiceUtils.isValidPrimaryKey(foreignKeyValues)) {
      logger.info(
          "Incoming foreignKeyValues is null; tenant={}, context={}, foreignKey={}, entry={}",
          getTenantName(),
          getStreamName(),
          foreignKey.get(0),
          outAttributes);
      return CompletableFuture.completedFuture(null);
    }

    // Get the context to join with.
    // Ideally we should fetch this once and cache it in CompiledEnrichment. However,
    // CompiledEnrichment is created by AdminImpl, which is in "server-common" sub-project
    // and so it cannot include ContextCassStream which is in "server" sub-project due to
    // the dependency order of sub-projects. We should make this perf improvement in bios2.
    final ContextCassStream joinedCassStream =
        cassTenant.getCassStream(
            enrichment.getJoiningContext().getName(),
            enrichment.getJoiningContext().getVersion(),
            true);
    if (joinedCassStream == null) {
      return CompletableFuture.failedFuture(
          new ApplicationException(
              String.format(
                  "Getting ContextCassStream %s failed",
                  enrichment.getJoiningContext().getName())));
    }

    // Lookup entry in the joined context.
    return joinedCassStream
        .getContextEntryAsync(state, foreignKeyValues)
        .thenApply(
            (joinedEntry) -> {
              if (joinedEntry == null) {
                logger.debug(
                    "Simple value lookup missing; tenant={}, context={},"
                        + " foreignKey={}, foreignKeyValues={}, joiningContext={}",
                    getTenantName(),
                    getStreamName(),
                    enrichment.getForeignKey().get(0),
                    foreignKeyValues,
                    enrichment.getJoiningContext().getName());
                if (enrichment.getMissingLookupPolicy()
                    != MissingLookupPolicy.STORE_FILL_IN_VALUE) {
                  return null;
                }
              }

              // Get the enriched attributes we need from this looked up entry, and add
              // them to the event that we need to return.
              for (final var attribute : enrichment.getCompiledAttributes()) {
                // The attribute should not already be present.
                if (outAttributes.get(attribute.getAliasedName()) != null) {
                  throw new CompletionException(
                      new ApplicationException(
                          String.format(
                              "Attribute %s unexpectedly present before looking up:%s",
                              attribute.getAliasedName(),
                              outAttributes.get(attribute.getAliasedName()).toString())));
                }
                // If there is an "as" property for the enriched attribute, we need to
                // return it with that name. However, the joined entry we received will
                // have the attribute in its original name. So get it with the original
                // name and add it with the aliased name.
                final Object value =
                    joinedEntry != null
                        ? joinedEntry.get(attribute.getJoinedAttributeName())
                        : attribute.getFillIn();
                outAttributes.put(attribute.getAliasedName(), value);
              }
              return outAttributes;
            });
  }

  /**
   * Every enrichedAttribute has a list of context+attribute candidates and each enrichedAttribute
   * needs to be processed independently.
   */
  private CompletableFuture<Map<String, Object>> valuePickUpFirstContextEnrichment(
      CompiledEnrichment enrichment,
      List<String> foreignKey,
      Map<String, Object> outAttributes,
      ContextOpState state) {

    if (outAttributes == null) {
      return CompletableFuture.completedFuture(null);
    }

    // Get the foreign key value we need to lookup.
    final List<Object> foreignKeyValue = DataUtils.makeCompositeKey(foreignKey, outAttributes);
    if (foreignKeyValue == null || foreignKeyValue.stream().anyMatch((value) -> value == null)) {
      logger.info(
          "Incoming foreignKeyValue is null; tenant={}, context={}, foreignKey={}, entry={}",
          getTenantName(),
          getStreamName(),
          foreignKey.get(0),
          outAttributes);
      return CompletableFuture.completedFuture(null);
    }

    var currentAttributeFuture = CompletableFuture.completedFuture(outAttributes);
    for (final var attribute : enrichment.getCompiledAttributes()) {
      // The attribute we are looking up/calculating, should not already be present.
      if (outAttributes.get(attribute.getAliasedName()) != null) {
        return CompletableFuture.failedFuture(
            new ApplicationException(
                String.format(
                    "Attribute %s unexpectedly present before being looked up:%s",
                    attribute.getAliasedName(),
                    outAttributes.get(attribute.getAliasedName()).toString())));
      }

      final var index = new AtomicInteger(0);
      currentAttributeFuture =
          currentAttributeFuture
              .thenCompose(
                  (none) -> findEnrichedAttribute(attribute, index, foreignKeyValue, state))
              .thenApply(
                  (candidateValue) -> {
                    // No candidate had a matching entry.
                    // TODO(BIOS-1475) Handle fill-in values.
                    if (candidateValue == null) {
                      logger.debug(
                          "valuePickFirst lookup missing; tenant={}, context={}"
                              + ", foreignKey={}, foreignKeyValue={}, enrichedAttribute={}",
                          getTenantName(),
                          getStreamName(),
                          enrichment.getForeignKey().get(0),
                          foreignKeyValue,
                          attribute.getAliasedName());
                      if (enrichment.getMissingLookupPolicy()
                          != MissingLookupPolicy.STORE_FILL_IN_VALUE) {
                        return null;
                      }
                    }

                    // Get the necessary attribute from this candidate entry.
                    // If there is an "as" property for the enriched attribute, we need to return
                    // it with that name. However, the joined entry we received will have the
                    // attribute in its original name. So get it with the original name and add it
                    // with the aliased name.
                    final Object value =
                        candidateValue != null ? candidateValue : attribute.getFillIn();
                    outAttributes.put(attribute.getAliasedName(), value);
                    return outAttributes;
                  });
    }
    return currentAttributeFuture;
  }

  /*
   * Go through the list of candidates recursively until we find a candidate that has
   * an entry with the matching foreign key value.
   */
  private CompletableFuture<Object> findEnrichedAttribute(
      CompiledAttribute attribute,
      AtomicInteger index,
      List<Object> foreignKey,
      ContextOpState state) {

    final var candidateNames = attribute.getCandidateAttributeNames();

    if (index.get() >= candidateNames.size()) {
      return CompletableFuture.completedFuture(null);
    }

    final String candidateJoinedAttributeName = candidateNames.get(index.get());
    final StreamDesc candidateContext = attribute.getCandidateContexts().get(index.get());
    final ContextCassStream candidateCassStream =
        cassTenant.getCassStream(candidateContext.getName(), candidateContext.getVersion(), true);
    if (candidateCassStream == null) {
      return CompletableFuture.failedFuture(
          new ApplicationException(
              String.format("Getting ContextCassStream %s failed", candidateContext.getName())));
    }

    return candidateCassStream
        .getContextEntryAsync(state, foreignKey)
        .thenCompose(
            (candidateEntry) -> {
              if (candidateEntry != null) {
                return CompletableFuture.completedFuture(
                    candidateEntry.get(candidateJoinedAttributeName));
              }
              index.incrementAndGet();
              return findEnrichedAttribute(attribute, index, foreignKey, state);
            });
  }

  public CompletionStage<Void> putContextEntriesAsync(
      ContextOpState state,
      Session session,
      List<Event> origEntries,
      RequestPhase phase,
      Long timestamp) {
    state.addHistory("store{");
    state.logActivity("----- ContextCassStream.putContextEntries ENTER ------");
    if (state.getActivityRecorder() != null) {
      state.logActivity(
          "keys: %s",
          origEntries.stream().map((entry) -> makePrimaryKey(entry)).collect(Collectors.toList()));
    }

    final var primaryKeys = new ArrayList<List<Object>>();
    final var entries = new ArrayList<Event>();
    final var dedupingMap = new HashMap<List<Object>, Integer>();
    for (var entry : origEntries) {
      final var primaryKey = makePrimaryKey(entry);
      final var index = dedupingMap.putIfAbsent(primaryKey, primaryKeys.size());
      if (index != null) {
        entries.set(index, entry);
      } else {
        primaryKeys.add(primaryKey);
        entries.add(entry);
      }
    }

    final Set<Event> auditEntries = ConcurrentHashMap.newKeySet();

    if (phase == RequestPhase.FINAL) {
      for (int i = 0; i < entries.size(); ++i) {
        List<Object> primaryKey = primaryKeys.get(i);
        final var entry = entries.get(i);
        entry.setIngestTimestamp(new Date(timestamp));
        getCache().put(primaryKey, entry, timestamp);
      }
      return CompletableFuture.completedStage(null);
    } else {
      // initial
      CompletionStage<Event[]> existing;
      final boolean auditEnabled = streamDesc.getAuditEnabled() == Boolean.TRUE;
      if (auditEnabled) {
        final var getEntriesState = new ContextOpState("RetrieveExistingEntries", state);
        getEntriesState.setStreamName(state.getStreamName());
        getEntriesState.setOptions(Set.of(IGNORE_CACHE_ONLY));
        getEntriesState.setMetricsRecorder(null);
        existing = getContextEntriesAsync(getEntriesState, primaryKeys);
      } else {
        final var events = new Event[primaryKeys.size()];
        for (int i = 0; i < events.length; ++i) {
          events[i] = getCache().get(primaryKeys.get(i));
        }
        existing = CompletableFuture.completedStage(events);
      }
      return existing
          .thenComposeAsync(
              (retrieved) -> {
                final var executionControllerBuilder =
                    ConcurrentExecutionController.newBuilder()
                        .concurrency(Bios2Config.insertBulkMaxConcurrency());
                for (int i = 0; i < entries.size(); ++i) {
                  final var event = entries.get(i);
                  final var cached = retrieved[i];
                  final int changeType = checkAttributesChange(cached, event);
                  if (changeType == NO_CHANGE) {
                    // We don't have to update the context entry in DB. We'll just update the
                    // timestamp
                    // of the cache entry in the final phase.
                    continue;
                  }
                  if (event.getEventId() == null) {
                    event.setEventId(Generators.timeBasedGenerator().generate());
                  }
                  final var eachState = new ContextOpState(String.format("upsert %s", i), state);
                  final Statement statement;
                  try {
                    statement = makeInsertStatement(event);
                  } catch (ApplicationException e) {
                    throw new CompletionException(e);
                  }
                  final Consumer<ResultSet> generateAuditEvent =
                      (result) -> {
                        if (auditEnabled) {
                          final var operationId =
                              changeType == NEW_ITEM ? OperationId.INSERT : OperationId.UPDATE;
                          Event auditEvent =
                              createAuditEvent(event, cached, event.getEventId(), operationId);
                          auditEntries.add(auditEvent);
                        }
                      };
                  final var atomicOperationContext = state.getAtomicOperationContext();
                  if (atomicOperationContext != null) {
                    atomicOperationContext.addStatement(statement);
                    generateAuditEvent.accept(null);
                  } else {
                    executionControllerBuilder.addStage(
                        new ContextOpStage(statement, session, eachState, generateAuditEvent));
                  }
                }
                state.endPreProcess();
                return executionControllerBuilder.build().execute();
              },
              state.getExecutor())
          .thenComposeAsync((none) -> insertAuditEvents(state, auditEntries), state.getExecutor())
          .thenRun(
              () -> {
                state.logActivity("----- ContextCassStream.putContextEntries DONE ------");
              });
    }
  }

  public CompletionStage<Void> deleteContextEntriesAsync(
      ContextOpState state,
      Session session,
      List<List<Object>> keys,
      RequestPhase phase,
      Long timestamp) {
    if (phase == RequestPhase.FINAL) {
      keys.forEach(key -> getCache().delete(key, timestamp));
      return CompletableFuture.completedStage(null);
    } else {
      state.logActivity("----- ContextCassStream.deleteContextEntries ENTER ------");
      state.logActivity("keys: %s", keys);
      final boolean auditEnabled = streamDesc.getAuditEnabled() == Boolean.TRUE;
      final Set<Event> auditEntries = auditEnabled ? ConcurrentHashMap.newKeySet() : null;
      final var getEntriesState = new ContextOpState("CheckEntriesExistence", state);
      getEntriesState.setStreamName(state.getStreamName());
      getEntriesState.setContextDesc(state.getContextDesc());
      getEntriesState.setOptions(Set.of(SKIP_ENRICHMENT, IGNORE_CACHE_ONLY, IGNORE_SOFT_DELETION));
      getEntriesState.setMetricsRecorder(null);
      return getContextEntriesAsync(getEntriesState, keys)
          .thenComposeAsync(
              (retrieved) -> {
                final var executionControllerBuilder =
                    ConcurrentExecutionController.newBuilder()
                        .concurrency(Bios2Config.insertBulkMaxConcurrency());
                for (int index = 0; index < retrieved.length; ++index) {
                  final var event = retrieved[index];
                  if (event == null) {
                    // ignore silently
                    continue;
                  }
                  final var deletionId = Generators.timeBasedGenerator().generate();
                  if (state.getActivityRecorder() != null) {
                    state.logActivity("  (d) retrieved[%d]: %s", index, keys.get(index));
                  }
                  final var eachState =
                      new ContextOpState(String.format("delete %s", index), state);
                  final Statement statement;
                  try {
                    statement =
                        makeDeleteStatement(event, Utils.uuidV1TimestampInMicros(deletionId));
                  } catch (ApplicationException e) {
                    throw new CompletionException(e);
                  }
                  final Consumer<ResultSet> generateAuditEvent =
                      (result) -> {
                        if (auditEnabled) {
                          Event auditEvent =
                              createAuditEvent(event, event, deletionId, OperationId.DELETE);
                          auditEntries.add(auditEvent);
                          if (state.getActivityRecorder() != null) {
                            state.logActivity("  audit log added: key=%s", makePrimaryKey(event));
                          }
                        }
                      };
                  final var atomicOperationContext = state.getAtomicOperationContext();
                  if (atomicOperationContext != null) {
                    atomicOperationContext.addStatement(statement);
                    generateAuditEvent.accept(null);
                  } else {
                    executionControllerBuilder.addStage(
                        new ContextOpStage(statement, session, eachState, generateAuditEvent));
                  }
                }
                return executionControllerBuilder.build().execute();
              },
              state.getExecutor())
          .thenComposeAsync(
              (none) -> {
                state.addRecordsWritten(keys.size());
                if (auditEnabled) {
                  state.logActivity("number of audit entries: %d", auditEntries.size());
                  return insertAuditEvents(state, auditEntries);
                }
                return CompletableFuture.completedStage(null);
              },
              state.getExecutor())
          .thenRun(
              () -> state.logActivity("----- ContextCassStream.deleteContextEntries DONE ------"));
    }
  }

  private String makeFetchStatementString() {
    final StringBuilder columns = new StringBuilder();
    final String delimiter = ", ";
    for (final var attribute : streamDesc.getAttributes()) {
      CassAttributeDesc desc = attributeTable.get(attribute.getName().toLowerCase());
      columns.append(delimiter).append(desc.getColumn());
    }

    final String where =
        keyColumns.stream().map((column) -> column + " = ?").collect(Collectors.joining(" AND "));

    final var statement =
        String.format(
            "SELECT %s, %s%s FROM %s.%s WHERE %s",
            COLUMN_ENTRY_ID, COLUMN_WRITE_TIME, columns, keyspaceName, tableName, where);
    logger.debug("Statement={}", statement);
    return statement;
  }

  // Utility classes ////////////////////////////////////////////

  /** Async stage to execute a DB access for a context operation. */
  private class ContextOpStage extends AsyncExecutionStage<ContextOpState> {
    private final Statement statement;
    private final Session session;
    private final ContextOpState stateForStage;
    private Consumer<ResultSet> completionHandler;

    /**
     * The constructor.
     *
     * @param statement Statement to execute
     * @param session Cassandra session
     * @param stateForStage State for the stage
     * @param completionHandler Completion handler
     */
    public ContextOpStage(
        Statement statement,
        Session session,
        ContextOpState stateForStage,
        Consumer<ResultSet> completionHandler) {
      super(stateForStage);
      this.statement = statement;
      this.session = session;
      this.stateForStage = stateForStage;
      this.completionHandler = completionHandler;
    }

    @Override
    public CompletionStage<Void> runAsync() {
      stateForStage.startDbAccess();
      return CassandraConnection.executeAsync(session, statement, state)
          .thenAcceptAsync(
              (rows) -> {
                completionHandler.accept(rows);
              },
              state.getExecutor());
    }
  }

  // Utility methods ////////////////////////////////////////////

  /**
   * Get the token value for the specified primary key.
   *
   * <p>Primary key is the partition key of the context table. This method fetches the token value
   * of specified primary key.
   *
   * @param partitionKey The partition key.
   * @return Token value of the key.
   * @throws ApplicationException When an unexpected error happens.
   */
  @Deprecated
  private long getToken(Object partitionKey) throws ApplicationException {
    final ResultSet result =
        getCassandraConnection().execute("get token", logger, queryGetToken, partitionKey);
    if (result.isExhausted()) {
      throw new ApplicationException(
          String.format(
              "Context table data is inconsistent; tenant=%s.%d context=%s.%d entry=%s",
              getTenantName(),
              getTenantVersion(),
              getStreamName(),
              getStreamVersion(),
              partitionKey.toString()));
    }
    final Row row = result.one();
    return row.getLong(0);
  }

  public CompletableFuture<Void> startCacheOnlyMode(ExecutionState state) {
    final var startTime0 = System.currentTimeMillis();
    final var propFuture = new CompletableFuture<Long>();
    SharedProperties.getCachedAsync(
        PROPERTY_CONTEXT_CACHE_LOADING_BATCH_SIZE,
        32768L,
        state,
        propFuture::complete,
        propFuture::completeExceptionally);

    final var iterationState =
        new ContextIterationState(state.getExecutionName(), state)
            .enableRowCounter()
            .enableEntryCounter()
            .setEntryConsumer(
                (primaryKey, entry, timestampUs) -> {
                  getCache().put(primaryKey, entry, timestampUs / 1000);
                });

    return propFuture
        .thenComposeAsync(
            (batchSize) -> {
              try {
                final var contextKeyRetriever =
                    new ContextEntryRetriever(
                        this, iterationState, batchSize.intValue(), ContextEntryRetriever.Type.ALL);

                iterationState.setBatchPostProcess(
                    (batchElapsedTime) -> {
                      final var nextStartToken = contextKeyRetriever.getTokenStart();
                      final var coverage =
                          ((double) nextStartToken - (double) Long.MIN_VALUE)
                              / ((double) Long.MAX_VALUE - (double) Long.MIN_VALUE)
                              * 100;
                      logger.info(
                          "{}: LOADED; token={} ({}%), numRows={}, numEntries={} elapsed={}",
                          getStreamName(),
                          nextStartToken,
                          String.format("%.2f", coverage),
                          iterationState.getRowCounter().get(),
                          iterationState.getEntryCounter().get(),
                          batchElapsedTime);
                      return CompletableFuture.completedStage(null);
                    });

                return iterateRows(contextKeyRetriever, iterationState);
              } catch (ApplicationException e) {
                throw new CompletionException(e);
              }
            },
            state.getExecutor())
        .whenCompleteAsync(
            (none, t) -> {
              if (t == null) {
                // success
                final long elapsed_millis = System.currentTimeMillis() - startTime0;
                final long elapsed_seconds = elapsed_millis % 60000 / 1000;
                final long elapsed_minutes = elapsed_millis / 60000;
                logger.info(
                    "Entered cache-only mode; tenant={}, context={}, numEntries={}, timeTaken={}:{}",
                    getTenantName(),
                    getStreamName(),
                    iterationState.getEntryCounter().get(),
                    elapsed_minutes,
                    elapsed_seconds);
                cacheMode = CacheMode.CACHE_ONLY;
              } else {
                // fail
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                logger.error(
                    "An exception happened while loading context cache entries,"
                        + " turning off cache-only mode; tenant={}, context={}",
                    getTenantName(),
                    getStreamName(),
                    cause);
                cacheMode = CacheMode.NORMAL;
              }
            },
            state.getExecutor());
  }

  /**
   * Iterates all rows in the storage and handle them as per the configuration in the execution
   * state.
   */
  public CompletionStage<Void> iterateRows(
      ContextEntryRetriever contextEntryRetriever, ContextIterationState state) {
    final var batchStartTime = System.currentTimeMillis();
    return contextEntryRetriever
        .getNextEntries()
        .thenComposeAsync(
            (allEntryValues) -> {
              if (allEntryValues.isEmpty()) {
                return CompletableFuture.completedStage(null);
              }
              if (state.getRowCounter() != null) {
                state.getRowCounter().addAndGet(allEntryValues.size());
              }
              state.assertSingleThread();
              for (Object[] entryValues : allEntryValues) {
                handleRetrievedEntry(entryValues, contextEntryRetriever.getType(), state);
              }
              if (state.getBatchPostProcess() != null) {
                final var batchElapsedTime = System.currentTimeMillis() - batchStartTime;
                return state.getBatchPostProcess().apply(batchElapsedTime);
              }
              return CompletableFuture.completedStage(null);
            },
            state.getExecutor())
        .thenComposeAsync(
            (none) -> {
              if (contextEntryRetriever.isCancelled()) {
                throw new CompletionException(new InterruptedException());
              }
              if (contextEntryRetriever.hasNext()) {
                return iterateRows(contextEntryRetriever, state);
              }
              return CompletableFuture.completedStage(null);
            },
            state.getExecutor());
  }

  private void handleRetrievedEntry(
      Object[] entryValues, ContextEntryRetriever.Type retrieverType, ContextIterationState state) {
    state.assertSingleThread();
    if (state.getPrimaryKeyConsumer() != null) {
      assert entryValues.length >= primaryKeyNames.size();
      final List<Object> primaryKey = Arrays.asList(entryValues).subList(0, primaryKeyNames.size());
      state.getPrimaryKeyConsumer().accept(primaryKey);
    }
    if (state.getEntryCounter() != null) {
      state.getEntryCounter().incrementAndGet();
    }

    if (state.getEntryConsumer() != null) {
      assert retrieverType == ContextEntryRetriever.Type.ALL;
      assert state.getAttributeNameToIndex() != null;
      final Event event =
          ObjectListEventValue.toEvent(state.getAttributeNameToIndex(), entryValues);
      final long timestampUs = (Long) entryValues[streamDesc.getAttributes().size()];
      long writeTimeEpochMillis = timestampUs / 1000;
      event.setIngestTimestamp(new Date(writeTimeEpochMillis));
      final var entryId = (UUID) entryValues[streamDesc.getAttributes().size() + 1];
      event.setEventId(entryId);
      final List<Object> primaryKey = DataUtils.makeCompositeKey(primaryKeyNames, event);
      state.getEntryConsumer().consume(primaryKey, event, timestampUs);
    }
  }

  private int checkAttributesChange(Event oldEvent, Event newEvent) {
    Objects.requireNonNull(newEvent);
    if (oldEvent == null) {
      return NEW_ITEM;
    }
    int flag = NO_CHANGE;
    for (var entry : newEvent.getAttributes().entrySet()) {
      final var name = entry.getKey();
      final Object newValue = entry.getValue();
      Object oldValue = oldEvent.get(name);
      if (oldValue == null) {
        oldValue = getAttributeDesc(name).getInternalDefaultValue();
      }
      if (!Objects.equals(oldValue, newValue)) {
        return ANY_CHANGED;
      }
    }
    return flag;
  }

  List<Object> makePrimaryKey(Event event) {
    return DataUtils.makeCompositeKey(primaryKeyNames, event);
  }

  private List<Object> makePrimaryKey(Row row) {
    final var values = new Object[keyColumns.size()];
    for (int i = 0; i < values.length; ++i) {
      values[i] = row.getObject(keyColumns.get(i));
    }
    return Arrays.asList(values);
  }

  // Maintenance /////////////////////////////////////////////////

  /**
   * Maintenance startpoint.
   *
   * <p>The maintenance task of this class trims deleted and overwritten context entries from DB.
   *
   * <p>there are several configurable parameters as follows:
   *
   * <dl>
   *   <dt>maintenanceInterval
   *   <dd>Interval in milliseconds between maintenance executions.
   *   <dt>margin
   *   <dd>Time margin in milliseconds from current time to allow entry removal. Any deleted or
   *       overwritten entries newer than this time margin would stay in DB and are removed in the
   *       later maintenance.
   *   <dt>limit
   *   <dd>Maximum number of primary keys to handle in a maintenance execution.
   * </dl>
   */
  public void runMaintenance(SketchStore sketchStore) {
    if (specialRepository != null) {
      // no table to maintain
      return;
    }
    // TODO(BIOS-4937): Use digestor's executor group
    final var state = new GenericExecutionState("context maintenance", dataEngine.getExecutor());
    final long currentTime = System.currentTimeMillis();
    final long margin = TfosConfig.contextMaintenanceCleanupMargin();
    int limit = SharedProperties.getInteger(PROPERTY_CONTEXT_MAINTENANCE_BATCH_SIZE, 8192);
    var maintenanceMode = MaintenanceMode.ENABLED;
    final var maintenanceModeSrc = SharedProperties.get(PROPERTY_CONTEXT_MAINTENANCE_MODE);
    if (maintenanceModeSrc != null && !maintenanceModeSrc.isBlank()) {
      try {
        maintenanceMode = MaintenanceMode.valueOf(maintenanceModeSrc.toUpperCase());
      } catch (IllegalArgumentException e) {
        logger.warn(
            "Invalid context maintenance mode in property {}; src={}, error={}",
            PROPERTY_CONTEXT_MAINTENANCE_MODE,
            maintenanceModeSrc,
            e.toString());
      }
    }
    if (maintenanceMode == MaintenanceMode.DISABLED) {
      return;
    }
    try {
      final long revisitInterval =
          SharedProperties.getLong(PROPERTY_CONTEXT_MAINTENANCE_REVISIT_INTERVAL, 32L * 60 * 60);
      maintenanceMain(
          revisitInterval, currentTime, margin, limit, maintenanceMode, state, sketchStore);
    } catch (Throwable t) {
      logger.error(
          "Failed to maintain context; tenant={}, context={}, version={}({}),"
              + " keyspace={}, table={}",
          getTenantName(),
          getStreamDesc().getName(),
          getStreamDesc().getVersion(),
          StringUtils.tsToIso8601(getStreamDesc().getVersion()),
          keyspaceName,
          tableName,
          t);
    }
  }

  public ContextMaintenanceResult maintenanceMain(
      long revisitInterval,
      long currentTime,
      long margin,
      int limit,
      MaintenanceMode maintenanceMode,
      ExecutionState state,
      SketchStore sketchStore)
      throws ApplicationException {
    final var result = new ContextMaintenanceResult();
    final String lockTarget =
        getTenantName() + "." + getStreamName() + "." + getSchemaVersion() + ".maintenance";
    try (WorkerLock.LockEntity lock = BiosModules.getWorkerLock().lock(lockTarget)) {
      if (lock == null) {
        result.setMaintenanceExecuted(false);
        result.setMaintenanceNotExecutedReason("Could not acquire lock: " + lockTarget);
        return result;
      }
      // Delete stale sketches if any. Even if we miss some, they can get cleaned up in
      // subsequent passes later.
      cleanupStaleSketches(sketchStore);
      if (getEffectiveTtl() != null) {
        final var startToken = getNextMaintenanceToken();
        if (startToken == Long.MIN_VALUE) {
          final long lastTimestamp = getPropertyAsLong(propertyLastTimestamp, 0);
          if (revisitInterval > 0 && currentTime - lastTimestamp < revisitInterval * 1000) {
            // yield to other contexts
            logger.debug(
                "Too early to start maintenance yet;"
                    + " tenant={}, context={}, current={}, last={}, interval={}",
                getTenantName(),
                getStreamName(),
                currentTime,
                lastTimestamp,
                revisitInterval);
            result.setMaintenanceExecuted(false);
            result.setMaintenanceNotExecutedReason(
                String.format(
                    "Too early, current=%d, last=%d, interval=%d",
                    currentTime, lastTimestamp, revisitInterval));
            return result;
          }
        }
        final var tokens =
            clearOldEntries(startToken, currentTime - margin, limit, maintenanceMode, state);
        result.setMaintenanceExecuted(true);
        result.setNextMaintenanceToken(tokens.getNextToken());
        result.setLastMaintenanceTimeInMillis(tokens.getLastToken());
        result.setMaintenanceDone(tokens.isDone());
        if (maintenanceMode != MaintenanceMode.DRY_RUN) {
          recordMaintenance(tokens.getNextToken(), currentTime);
        }
      }
    }
    return result;
  }

  /**
   * Method to write the result of a maintenance execution.
   *
   * @param nextToken The starting token value to be used for the next maintenance execution.
   * @param timestamp Timestamp of starting the maintenance.
   * @throws ApplicationException when an unexpected error happens.
   */
  private void recordMaintenance(long nextToken, long timestamp) throws ApplicationException {
    final Properties properties = new Properties();
    properties.setProperty(propertyNextToken, Long.toString(nextToken));
    properties.setProperty(propertyLastTimestamp, Long.toString(timestamp));
    addProperties(properties);
  }

  private Long getEffectiveTtl() {
    final Long ttl;
    final var info =
        BiosModules.getAdminInternal().getFeatureAsContextInfo(getTenantName(), getStreamName());
    if (info != null) {
      ttl =
          info.getTtlInMillis() != null
              ? info.getTtlInMillis()
              : BiosConstants.DEFAULT_LAST_N_COLLECTION_TTL;
    } else {
      ttl = streamDesc.getTtl();
    }
    return ttl;
  }

  /** Fetches next maintenance token from the table properties. */
  public long getNextMaintenanceToken() throws ApplicationException {
    return getPropertyAsLong(propertyNextToken, Long.MIN_VALUE);
  }

  public long getLastMaintenanceTime() throws ApplicationException {
    return getPropertyAsLong(propertyLastTimestamp, 0);
  }

  /**
   * Method to clear old context entries from the DB table.
   *
   * <p>The method finds primary keys that are the partition keys of the table by selecting with
   * token range. The range is from the specified tokenStart to Long.MAX_VALUE, but the number of
   * keys to select is limited to batchSize.
   *
   * <p>The method, then, iterates the keys to fetch all entries for each key. Deleted or
   * overwritten entrties would be deleted.
   *
   * @param startToken Cassandra partition key token value to scan from. The method would find keys
   *     from this value to Long.MAX_VALUE.
   * @param currentTime Current time in milliseconds.
   * @param batchSize Limits number of keys to handle.
   * @return The token starting point that should be used for the next cleanup execution.
   * @throws ApplicationException When an unexpected error happens.
   */
  protected Tokens clearOldEntries(
      long startToken,
      long currentTime,
      int batchSize,
      MaintenanceMode maintenanceMode,
      ExecutionState state)
      throws ApplicationException {
    final var iterationState = new ContextIterationState(state.getExecutionName(), state);
    final var contextKeyRetriever =
        new ContextEntryRetriever(
            this,
            iterationState,
            startToken,
            batchSize,
            ContextEntryRetriever.Type.INCLUDE_WRITE_TIME);
    final List<Object[]> retrievedKeys;
    try {
      retrievedKeys = contextKeyRetriever.getNextEntries().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof ApplicationException) {
        throw (ApplicationException) cause;
      }
      throw new RuntimeException(e);
    }
    if (retrievedKeys.isEmpty()) {
      if (maintenanceMode == MaintenanceMode.DRY_RUN) {
        logger.info(
            "dry-run: Context keys for maintenance; tenant={}, context={}, keys=0, range=[{}:{}]",
            getTenantName(),
            getStreamName(),
            startToken,
            startToken);
      } else {
        logger.info(
            "Maintained context;"
                + " tenant={}, context={}, numEntries={}, deleted=0, range=[{}:{}], done={}",
            getTenantName(),
            getStreamName(),
            retrievedKeys.size(),
            startToken,
            startToken,
            true);
      }
      return new Tokens(startToken, startToken, true);
    }
    final Object lastPartitionKey = retrievedKeys.get(retrievedKeys.size() - 1)[0];
    // Determine the next token before cleaning up entries (cleaning deleted entries may erase the
    // last key). We'll find the token of the last key if the number of keys is equal to the batch
    // size, which means there can be more keys in the rest token range. Otherwise, the next token
    // is the smallest possible one.
    final long lastToken = getToken(lastPartitionKey);
    final long nextToken = retrievedKeys.size() == batchSize ? lastToken : Long.MIN_VALUE;
    if (maintenanceMode == MaintenanceMode.DRY_RUN) {
      logger.info(
          "dry-run: Context keys for maintenance; tenant={}, context={}, keys={}, range=[{}:{}]",
          getTenantName(),
          getStreamName(),
          retrievedKeys.size(),
          startToken,
          lastToken);
      return new Tokens(lastToken, nextToken, nextToken == Long.MIN_VALUE);
    }
    // start cleaning up

    final var ttl = getEffectiveTtl();
    assert ttl != null;
    final var keysForObsoleteEntries = new ArrayList<List<Object>>();
    for (var keyValues : retrievedKeys) {
      final var writeTimeInMillis = (Long) keyValues[keyValues.length - 1] / 1000;
      if (currentTime > writeTimeInMillis + ttl) {
        keysForObsoleteEntries.add(Arrays.asList(keyValues).subList(0, primaryKeyNames.size()));
      }
    }

    if (!keysForObsoleteEntries.isEmpty()) {
      try {
        logger.debug(
            "{}: deleting : {}",
            StringUtils.tsToIso8601(System.currentTimeMillis()),
            keysForObsoleteEntries);
        DataMaintenanceUtils.deleteContextEntriesWithFanRouter(
            getTenantName(), getStreamName(), System.currentTimeMillis(), keysForObsoleteEntries);
        logger.debug(
            "{}: delete completed : {}",
            StringUtils.tsToIso8601(System.currentTimeMillis()),
            keysForObsoleteEntries);
      } catch (TfosException e) {
        throw new ApplicationException("Deleting TTL expired entries failed", e);
      }
    }

    boolean finished = nextToken == Long.MIN_VALUE;
    logger.info(
        "Maintained context;"
            + " tenant={}, context={}, numEntries={}, deleted={}, range=[{}:{}], done={}",
        getTenantName(),
        getStreamName(),
        retrievedKeys.size(),
        keysForObsoleteEntries.size(),
        startToken,
        lastToken,
        finished);
    return new Tokens(lastToken, nextToken, finished);
  }

  protected void cleanupStaleSketches(SketchStore sketchStore) {
    if (sketchStore == null) {
      return;
    }

    // Get the timestamp of the latest set of sketches.
    final Long timestampOfLatestSketches =
        dataEngine
            .getPostProcessScheduler()
            .getTimestampOfLatestSketches(getTenantName(), streamDesc.getStreamNameProxy());
    if (timestampOfLatestSketches == null) {
      return;
    }

    final var tenantId = TenantId.of(getTenantName().toLowerCase(), getTenantVersion());

    // Calculate the timestamp of the oldest set of sketches that should be kept by subtracting
    // a margin to allow in-progress select operations to complete.
    final long deleteTimestamp = timestampOfLatestSketches - DELETE_SKETCHES_TIME_MARGIN_MS;

    final boolean deleteSummary;
    final Long oldestSummary =
        sketchStore.getOldestSummaryForContext(tenantId, streamDesc.getStreamNameProxy());
    if ((oldestSummary != null) && (oldestSummary <= deleteTimestamp)) {
      deleteSummary = true;
    } else {
      deleteSummary = false;
    }

    final boolean deleteBlob;
    final Long oldestBlob =
        sketchStore.getOldestBlobForContext(tenantId, streamDesc.getStreamNameProxy());
    if ((oldestBlob != null) && (oldestBlob <= deleteTimestamp)) {
      deleteBlob = true;
    } else {
      deleteBlob = false;
    }

    // Delete all sketches older than the calculated timestamp.
    // We need to delete sketches for each attribute one by one.
    for (var attribute : streamDesc.getAttributes()) {
      if (deleteSummary) {
        sketchStore.deleteSummaryForContext(
            tenantId,
            streamDesc.getStreamNameProxy(),
            streamDesc.getAttributeProxy(attribute.getName()),
            deleteTimestamp);
      }
      if (deleteBlob) {
        sketchStore.deleteBlobForContext(
            tenantId,
            streamDesc.getStreamNameProxy(),
            streamDesc.getAttributeProxy(attribute.getName()),
            deleteTimestamp);
      }
    }
  }
}
