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
package io.isima.bios.data.impl.sketch;

import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_SKETCH_BLOB;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_SKETCH_BLOB_CONTEXT;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_SKETCH_SUMMARY;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_SKETCH_SUMMARY_CONTEXT;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.TenantId;
import io.isima.bios.data.impl.maintenance.PostProcessScheduler;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString
public class SketchStore {

  private static final Logger logger = LoggerFactory.getLogger(SketchStore.class);

  private static final String SKETCH_TABLES_COMMON_OPTIONS =
      "    AND comment = 'tenant=%s'"
          + "    AND compaction = {"
          + "      'compaction_window_size': 1,"
          + "      'compaction_window_unit': 'DAYS',"
          + "      'tombstone_compaction_interval': '864000',"
          + "      'max_threshold': '32',"
          + "      'min_threshold': '4',"
          + "      'tombstone_threshold': '0.8',"
          + "      'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
          + "      'unchecked_tombstone_compaction': 'true'}"
          + "    AND compression = {"
          + "        'chunk_length_in_kb': '64',"
          + "        'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}"
          + "    AND crc_check_chance = 1.0"
          + "    AND default_time_to_live = %d"
          + "    AND gc_grace_seconds = 259200"
          + "    AND speculative_retry = '95PERCENTILE';";

  private static String makeCreateSummaryTableQuery(
      String keyspaceName, String tenantName, String tableName) {
    final String initialColumns;
    final String keyAndClustering;
    final long default_time_to_live;
    if (tableName.equals(TABLE_SKETCH_SUMMARY)) {
      initialColumns =
          "  time_index timestamp,"
              + "  stream_name_proxy int,"
              + "  duration_type tinyint,"
              + "  attribute_proxy smallint,"
              + "  end_time timestamp";
      keyAndClustering =
          "  , PRIMARY KEY ((time_index), "
              + "      stream_name_proxy, duration_type, attribute_proxy, end_time)"
              + ") WITH CLUSTERING ORDER BY ("
              + "    stream_name_proxy ASC, duration_type ASC, attribute_proxy ASC, end_time ASC)";
      default_time_to_live = TfosConfig.featureRecordsDefaultTimeToLive();
    } else {
      initialColumns =
          "  stream_name_proxy int," + "  attribute_proxy smallint," + "  end_time timestamp";
      keyAndClustering =
          "  , PRIMARY KEY ((stream_name_proxy), attribute_proxy, end_time)"
              + ") WITH CLUSTERING ORDER BY (attribute_proxy ASC, end_time ASC)";
      default_time_to_live = 0;
    }
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + initialColumns
            + SketchSummaryColumn.getColumnsAndCassandraDatatypes()
            + keyAndClustering
            + SKETCH_TABLES_COMMON_OPTIONS,
        keyspaceName,
        tableName,
        tenantName,
        default_time_to_live);
  }

  private static String makeAlterSummaryTableQuery(
      String keyspaceName, String tableName, DataSketchType sketchType) {
    return String.format(
        "ALTER TABLE %s.%s ADD ("
            + SketchSummaryColumn.getColumnsAndCassandraDatatypes(sketchType)
            + ");",
        keyspaceName,
        tableName);
  }

  private static String makeCreateBlobTableQuery(
      String keyspaceName, String tenantName, String tableName) {
    final String initialColumns;
    final String keyAndClustering;
    final long default_time_to_live;
    if (tableName.equals(TABLE_SKETCH_BLOB)) {
      initialColumns =
          "  time_index timestamp,"
              + "  stream_name_proxy int,"
              + "  duration_type tinyint,"
              + "  attribute_proxy smallint,"
              + "  sketch_type tinyint,"
              + "  end_time timestamp,";
      keyAndClustering =
          "  , PRIMARY KEY ((time_index), "
              + "      stream_name_proxy, duration_type, attribute_proxy, sketch_type, end_time)"
              + ") WITH CLUSTERING ORDER BY ("
              + "    stream_name_proxy ASC, duration_type ASC, attribute_proxy ASC, sketch_type ASC,"
              + "    end_time ASC)";
      default_time_to_live = TfosConfig.featureRecordsDefaultTimeToLive();
    } else {
      initialColumns =
          "  stream_name_proxy int,"
              + "  attribute_proxy smallint,"
              + "  sketch_type tinyint,"
              + "  end_time timestamp,";
      keyAndClustering =
          "  , PRIMARY KEY ((stream_name_proxy), "
              + "   attribute_proxy, sketch_type, end_time)"
              + ") WITH CLUSTERING ORDER BY (attribute_proxy ASC, sketch_type ASC, end_time ASC)";
      default_time_to_live = 0;
    }
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ("
            + initialColumns
            + "  count bigint,"
            + "  size int,"
            + "  sketch_header blob,"
            + "  sketch_data blob"
            + keyAndClustering
            + SKETCH_TABLES_COMMON_OPTIONS,
        keyspaceName,
        tableName,
        tenantName,
        default_time_to_live);
  }

  private static final String SKETCH_SUMMARY_TIME_INDEX_WIDTH_KEY =
      "prop.sketchSummaryTimeIndexWidth";
  private static final Long SKETCH_SUMMARY_TIME_INDEX_WIDTH_DEFAULT = 1000L * 60 * 60 * 24;

  private static final String SKETCH_BLOB_TIME_INDEX_WIDTH_KEY = "prop.sketchBlobTimeIndexWidth";
  private static final Long SKETCH_BLOB_TIME_INDEX_WIDTH_DEFAULT = 1000L * 60 * 60 * 24;

  private final CassandraConnection cassandraConnection;
  private final PostProcessScheduler postProcessScheduler;
  @Deprecated private final Session session;
  private final Map<TenantId, String> tenantToKeyspaceMap = new HashMap<>();

  // These maps keep all the insert/select sketch statements for all the tenants.
  private final Map<TenantId, PreparedStatement> insertSummaryStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> insertSummaryContextStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> deleteSummaryContextStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> getOldestSummaryContextStatements =
      new HashMap<>();
  private final Map<TenantId, PreparedStatement> insertBlobStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> insertBlobContextStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> deleteBlobContextStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> getOldestBlobContextStatements = new HashMap<>();

  /**
   * This is a cache of select summary statements for each tenant. The set of columns selected can
   * vary across tenant and time based on reports and usage. For now we are just using a map instead
   * of a cache. TODO: replace inner ConcurrentMap with an LRU cache.
   */
  private final Map<TenantId, ConcurrentMap<Set<SketchSummaryColumn>, PreparedStatement>>
      selectSummaryStatements = new HashMap<>();

  private final Map<TenantId, ConcurrentMap<Set<SketchSummaryColumn>, PreparedStatement>>
      selectSummaryContextStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> selectBlobStatements = new HashMap<>();
  private final Map<TenantId, PreparedStatement> selectBlobContextStatements = new HashMap<>();

  @Getter private final long summaryTimeIndexWidth;
  @Getter private final long blobTimeIndexWidth;

  public SketchStore(
      CassandraConnection cassandraConnection, PostProcessScheduler postProcessScheduler) {
    if (cassandraConnection == null) {
      throw new NullPointerException("cassandraConnection must not be null.");
    }
    this.cassandraConnection = cassandraConnection;
    this.session = cassandraConnection.getSession();
    this.postProcessScheduler = postProcessScheduler;
    this.summaryTimeIndexWidth =
        SharedProperties.getCached(
            SKETCH_SUMMARY_TIME_INDEX_WIDTH_KEY, SKETCH_SUMMARY_TIME_INDEX_WIDTH_DEFAULT);
    this.blobTimeIndexWidth =
        SharedProperties.getCached(
            SKETCH_BLOB_TIME_INDEX_WIDTH_KEY, SKETCH_BLOB_TIME_INDEX_WIDTH_DEFAULT);
  }

  public void initializeTenant(String keyspaceName, TenantId tenantId) throws ApplicationException {
    try {
      createOrUpdateSummaryTableIfNeeded(keyspaceName, tenantId.getName(), TABLE_SKETCH_SUMMARY);
      createOrUpdateSummaryTableIfNeeded(
          keyspaceName, tenantId.getName(), TABLE_SKETCH_SUMMARY_CONTEXT);
      createBlobTableIfNeeded(keyspaceName, tenantId.getName(), TABLE_SKETCH_BLOB);
      createBlobTableIfNeeded(keyspaceName, tenantId.getName(), TABLE_SKETCH_BLOB_CONTEXT);

      insertSummaryStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeInsertSummaryQuery(keyspaceName)));
      insertSummaryContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeInsertSummaryContextQuery(keyspaceName)));
      deleteSummaryContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeDeleteSummaryContextQuery(keyspaceName)));
      getOldestSummaryContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeGetOldestSummaryContextQuery(keyspaceName)));

      selectSummaryStatements.put(tenantId, new ConcurrentHashMap<>());
      selectSummaryContextStatements.put(tenantId, new ConcurrentHashMap<>());

      insertBlobStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeInsertBlobQuery(keyspaceName)));
      insertBlobContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeInsertBlobContextQuery(keyspaceName)));
      deleteBlobContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeDeleteBlobContextQuery(keyspaceName)));
      getOldestBlobContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeGetOldestBlobContextQuery(keyspaceName)));

      selectBlobStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeSelectBlobQuery(keyspaceName)));
      selectBlobContextStatements.computeIfAbsent(
          tenantId, (key) -> session.prepare(makeSelectBlobContextQuery(keyspaceName)));

      tenantToKeyspaceMap.put(tenantId, keyspaceName);
    } catch (Throwable t) {
      String message =
          String.format(
              "Caught exception while initializing tenant=%s, keyspace=%s", tenantId, keyspaceName);
      logger.error(message, t);
      throw new ApplicationException(message, t);
    }
  }

  private void createOrUpdateSummaryTableIfNeeded(
      String keyspaceName, String tenantName, String tableName) {
    final boolean hasSummaryTable = cassandraConnection.verifyTable(keyspaceName, tableName);
    if (!hasSummaryTable) {
      // Create the sketch summary table for this tenant.
      final String createTableQuery =
          makeCreateSummaryTableQuery(keyspaceName, tenantName, tableName);
      logger.info(
          "Creating sketch summary table: {}.{} for tenant={}",
          keyspaceName,
          tableName,
          tenantName);
      logger.debug("Query to create sketch summary table: {}", createTableQuery);
      session.execute(createTableQuery);
    } else {
      addColumnsIfAbsent(keyspaceName, tenantName, tableName, DataSketchType.QUANTILES);
      addColumnsIfAbsent(keyspaceName, tenantName, tableName, DataSketchType.DISTINCT_COUNT);
      addColumnsIfAbsent(keyspaceName, tenantName, tableName, DataSketchType.SAMPLE_COUNTS);
    }
  }

  private void createBlobTableIfNeeded(String keyspaceName, String tenantName, String tableName) {
    final boolean hasBlobTable = cassandraConnection.verifyTable(keyspaceName, tableName);
    if (!hasBlobTable) {
      // Create the sketch blob table for this tenant.
      final String createTableQuery = makeCreateBlobTableQuery(keyspaceName, tenantName, tableName);
      logger.info(
          "Creating sketch blob table: {}.{} for tenant={}", keyspaceName, tableName, tenantName);
      logger.debug("Query to create sketch blob table: {}", createTableQuery);
      session.execute(createTableQuery);
    }
  }

  private void addColumnsIfAbsent(
      String keyspaceName, String tenantName, String tableName, DataSketchType sketchType) {
    // Add columns for this sketch type to the table if not already present.
    if (!cassandraConnection.verifyTableHasColumn(
        keyspaceName,
        tableName,
        SketchSummaryColumn.getAnyColumnForSketchType(sketchType).getColumnName())) {
      final String alterTableQuery =
          makeAlterSummaryTableQuery(keyspaceName, tableName, sketchType);
      logger.info(
          "Adding columns to sketch summary table: {}.{} for sketchType={} and tenant={}",
          keyspaceName,
          tableName,
          sketchType,
          tenantName);
      logger.debug("Query to alter sketch summary table: {}", alterTableQuery);
      session.execute(alterTableQuery);
    }
  }

  private static String makeInsertSummaryQuery(String keyspaceName) {
    return String.format(
        "INSERT INTO %s.%s ("
            + " time_index, stream_name_proxy, duration_type, attribute_proxy, end_time"
            + SketchSummaryColumn.getColumns()
            + " ) values (?, ?, ?, ?, ?"
            + ", ?".repeat(SketchSummaryColumn.values().length)
            + ")",
        keyspaceName,
        TABLE_SKETCH_SUMMARY);
  }

  private static String makeInsertSummaryContextQuery(String keyspaceName) {
    return String.format(
        "INSERT INTO %s.%s ("
            + " stream_name_proxy, attribute_proxy, end_time"
            + SketchSummaryColumn.getColumns()
            + " ) values (?, ?, ?"
            + ", ?".repeat(SketchSummaryColumn.values().length)
            + ")",
        keyspaceName,
        TABLE_SKETCH_SUMMARY_CONTEXT);
  }

  private static String makeDeleteSummaryContextQuery(String keyspaceName) {
    return String.format(
        "DELETE FROM %s.%s WHERE " + "stream_name_proxy=? AND attribute_proxy=? AND end_time<?",
        keyspaceName, TABLE_SKETCH_SUMMARY_CONTEXT);
  }

  private static String makeGetOldestSummaryContextQuery(String keyspaceName) {
    return String.format(
        "SELECT min(end_time) FROM %s.%s WHERE stream_name_proxy=?",
        keyspaceName, TABLE_SKETCH_SUMMARY_CONTEXT);
  }

  private static String makeInsertBlobQuery(String keyspaceName) {
    return String.format(
        "INSERT INTO %s.%s ("
            + " time_index, stream_name_proxy, duration_type, attribute_proxy, sketch_type, "
            + "end_time, count, size, sketch_header, sketch_data"
            + " ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        keyspaceName, TABLE_SKETCH_BLOB);
  }

  private static String makeInsertBlobContextQuery(String keyspaceName) {
    return String.format(
        "INSERT INTO %s.%s ("
            + " stream_name_proxy, attribute_proxy, sketch_type, end_time, "
            + "count, size, sketch_header, sketch_data"
            + " ) values (?, ?, ?, ?, ?, ?, ?, ?)",
        keyspaceName, TABLE_SKETCH_BLOB_CONTEXT);
  }

  private static String makeDeleteBlobContextQuery(String keyspaceName) {
    return String.format(
        "DELETE FROM %s.%s WHERE stream_name_proxy=? AND attribute_proxy=? "
            + "AND sketch_type IN (2, 3, 4) AND end_time<?",
        keyspaceName, TABLE_SKETCH_BLOB_CONTEXT);
  }

  private static String makeGetOldestBlobContextQuery(String keyspaceName) {
    return String.format(
        "SELECT min(end_time) FROM %s.%s WHERE stream_name_proxy=?",
        keyspaceName, TABLE_SKETCH_BLOB_CONTEXT);
  }

  public CompletableFuture<Void> insertSummaryAsync(
      SketchSummaryKey summaryKey, SketchSummary summary, ExecutionState parentState) {
    final var state = new GenericExecutionState("InsertSketchSummary", parentState);
    state.addHistory("(makeStatement");
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    if (summaryKey.getDurationType() != DataSketchDuration.ALL_TIME) {
      // This is for a signal.
      statement = insertSummaryStatements.get(summaryKey.getTenantId());
      if (statement == null) {
        logger.warn(
            "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
                + "tenant={}",
            summaryKey.getTenantId());
        return CompletableFuture.completedFuture(null);
      }
      boundStatement =
          statement.bind(
              summaryKey.getTimeIndex(),
              summaryKey.getStreamNameProxy(),
              summaryKey.getDurationType().getProxy(),
              summaryKey.getAttributeProxy(),
              summaryKey.getEndTime());
    } else {
      // This is for a context.
      statement = insertSummaryContextStatements.get(summaryKey.getTenantId());
      if (statement == null) {
        logger.warn(
            "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
                + "tenant={}",
            summaryKey.getTenantId());
        return CompletableFuture.completedFuture(null);
      }
      boundStatement =
          statement.bind(
              summaryKey.getStreamNameProxy(),
              summaryKey.getAttributeProxy(),
              summaryKey.getEndTime());
      logger.debug("storing {}", statement.getQueryKeyspace());
      logger.debug("statement {}", statement.getQueryString());
    }
    summary.bindAvailableSummaryColumns(boundStatement);
    state.addHistory(")(store");
    return cassandraConnection
        .executeAsync(boundStatement, state)
        .thenRun(
            () -> {
              state.addHistory(")");
              state.markDone();
            })
        .toCompletableFuture();
  }

  public void deleteSummaryForContext(
      TenantId tenantId, int streamNameProxy, short attributeProxy, long endTime) {
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    statement = deleteSummaryContextStatements.get(tenantId);
    boundStatement = statement.bind(streamNameProxy, attributeProxy, endTime);
    logger.debug("statement {}", statement.getQueryString());
    assert !ExecutorManager.isInIoThread();
    session.execute(boundStatement);
  }

  public Long getOldestSummaryForContext(TenantId tenantId, int streamNameProxy) {
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    statement = getOldestSummaryContextStatements.get(tenantId);
    if (statement == null) {
      logger.warn(
          "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
              + "tenant={}",
          tenantId);
      return null;
    }
    boundStatement = statement.bind(streamNameProxy);
    logger.debug("statement {}", statement.getQueryString());
    assert !ExecutorManager.isInIoThread();

    final Long minEndTime;
    final ResultSet results = session.execute(boundStatement);
    if (!results.isExhausted()) {
      final Row row = results.one();
      minEndTime = row.getLong(0);
      logger.debug(
          "getOldestSummaryForContext tenant={} streamNameProxy={} minEndTime={} ({})",
          tenantId,
          streamNameProxy,
          minEndTime,
          StringUtils.tsToIso8601Millis(minEndTime));
    } else {
      minEndTime = null;
    }
    return minEndTime;
  }

  public CompletableFuture<Void> insertBlobAsync(
      SketchSummaryKey summaryKey,
      DataSketchType sketchType,
      long count,
      ByteBuffer header,
      ByteBuffer data,
      ExecutionState parentState) {
    final var state = new GenericExecutionState("InsertSketchBlob", parentState);
    state.addHistory("(makeStatement");
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    if (summaryKey.getDurationType() != DataSketchDuration.ALL_TIME) {
      // This is for a signal.
      statement = insertBlobStatements.get(summaryKey.getTenantId());
      if (statement == null) {
        logger.warn(
            "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
                + "tenant={}",
            summaryKey.getTenantId());
        state.addHistory(")");
        state.markDone();
        return CompletableFuture.completedFuture(null);
      }
      boundStatement =
          statement.bind(
              summaryKey.getTimeIndex(),
              summaryKey.getStreamNameProxy(),
              summaryKey.getDurationType().getProxy(),
              summaryKey.getAttributeProxy(),
              sketchType.getProxy(),
              summaryKey.getEndTime(),
              count,
              data.limit(),
              header,
              data);
    } else {
      // This is for a context.
      statement = insertBlobContextStatements.get(summaryKey.getTenantId());
      if (statement == null) {
        logger.warn(
            "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
                + "tenant={}",
            summaryKey.getTenantId());
        state.addHistory(")");
        state.markDone();
        return CompletableFuture.completedFuture(null);
      }
      boundStatement =
          statement.bind(
              summaryKey.getStreamNameProxy(),
              summaryKey.getAttributeProxy(),
              sketchType.getProxy(),
              summaryKey.getEndTime(),
              count,
              data.limit(),
              header,
              data);
    }
    state.addHistory(")(store");
    return cassandraConnection
        .executeAsync(boundStatement, state)
        .thenRun(
            () -> {
              state.addHistory(")");
              state.markDone();
            })
        .toCompletableFuture();
  }

  public void deleteBlobForContext(
      TenantId tenantId, int streamNameProxy, short attributeProxy, long endTime) {
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    statement = deleteBlobContextStatements.get(tenantId);
    if (statement == null) {
      logger.warn(
          "Failed to get sketch blob insertion statement. The tenant is likely to be removed; "
              + "tenant={}",
          tenantId);
      return;
    }
    boundStatement = statement.bind(streamNameProxy, attributeProxy, endTime);
    logger.debug("statement {}", statement.getQueryString());
    assert !ExecutorManager.isInIoThread();
    session.execute(boundStatement);
  }

  public Long getOldestBlobForContext(TenantId tenantId, int streamNameProxy) {
    final PreparedStatement statement;
    final BoundStatement boundStatement;
    statement = getOldestBlobContextStatements.get(tenantId);
    boundStatement = statement.bind(streamNameProxy);
    logger.debug("statement {}", statement.getQueryString());
    assert !ExecutorManager.isInIoThread();

    final Long minEndTime;
    final ResultSet results = session.execute(boundStatement);
    if (!results.isExhausted()) {
      final Row row = results.one();
      minEndTime = row.getLong(0);
      logger.debug(
          "getOldestBlobForContext tenant={} streamNameProxy={} minEndTime={} ({})",
          tenantId,
          streamNameProxy,
          minEndTime,
          StringUtils.tsToIso8601Millis(minEndTime));
    } else {
      minEndTime = null;
    }
    return minEndTime;
  }

  public BoundStatement makeSelectSummaryStatement(
      TenantId tenantId,
      Set<SketchSummaryColumn> columns,
      long timeIndex,
      int streamNameProxy,
      DataSketchDuration durationType,
      short attributeProxy,
      long firstRowTime,
      long lastRowTime) {
    final String keyspaceName = tenantToKeyspaceMap.get(tenantId);

    // Find the prepared statement for this set of columns in the cache, and if not found,
    // create one and add it to the cache.
    final var selectSummaryStatementCache = selectSummaryStatements.get(tenantId);
    final var preparedStatement =
        selectSummaryStatementCache.computeIfAbsent(
            columns,
            k -> {
              final String columnsString =
                  "duration_type, attribute_proxy, end_time"
                      + SketchSummaryColumn.getCommaSeparatedColumns(columns);
              final String statement = makeSelectSummaryQuery(keyspaceName, columnsString);
              return session.prepare(statement);
            });

    // Bind the prepared statement with the provided values.
    final var boundStatement =
        preparedStatement.bind(
            timeIndex,
            streamNameProxy,
            durationType.getProxy(),
            attributeProxy,
            firstRowTime,
            lastRowTime);
    logger.debug(
        "makeSelectSummaryStatement: {}, {}, {}, {}, {}, {}",
        timeIndex,
        streamNameProxy,
        durationType.getProxy(),
        attributeProxy,
        firstRowTime,
        lastRowTime);
    return boundStatement;
  }

  public BoundStatement makeSelectSummaryContextStatement(
      TenantId tenantId,
      Set<SketchSummaryColumn> columns,
      int streamNameProxy,
      short attributeProxy,
      long specificEndTime) {
    final String keyspaceName = tenantToKeyspaceMap.get(tenantId);

    // Find the prepared statement for this set of columns in the cache, and if not found,
    // create one and add it to the cache.
    final var selectSummaryStatementCache = selectSummaryContextStatements.get(tenantId);
    final var preparedStatement =
        selectSummaryStatementCache.computeIfAbsent(
            columns,
            k -> {
              final String columnsString =
                  "attribute_proxy, end_time"
                      + SketchSummaryColumn.getCommaSeparatedColumns(columns);
              final String statement = makeSelectSummaryContextQuery(keyspaceName, columnsString);
              return session.prepare(statement);
            });

    // Bind the prepared statement with the provided values.
    final var boundStatement = preparedStatement.bind(streamNameProxy, attributeProxy);
    logger.debug("{}", keyspaceName);
    logger.debug(
        "makeSelectSummaryStatement: {}, {}, {}", streamNameProxy, attributeProxy, specificEndTime);
    return boundStatement;
  }

  private static String makeSelectSummaryQuery(String keyspaceName, String columnsString) {
    final var query =
        String.format(
            "SELECT %s FROM %s.%s WHERE"
                + " time_index=? AND stream_name_proxy=? AND duration_type=?"
                + " AND attribute_proxy=? AND end_time>=? AND end_time<=?"
                + " ORDER BY stream_name_proxy, duration_type, attribute_proxy, end_time",
            columnsString, keyspaceName, TABLE_SKETCH_SUMMARY);
    logger.debug("makeSelectSummaryQuery: {}", query);
    return query;
  }

  private static String makeSelectSummaryContextQuery(String keyspaceName, String columnsString) {
    final var query =
        String.format(
            "SELECT %s FROM %s.%s WHERE" + " stream_name_proxy=? AND attribute_proxy=?",
            columnsString, keyspaceName, TABLE_SKETCH_SUMMARY_CONTEXT);
    return query;
  }

  public BoundStatement makeSelectBlobStatement(
      TenantId tenantId,
      Set<DataSketchType> sketchTypes,
      long timeIndex,
      int streamNameProxy,
      DataSketchDuration durationType,
      short attributeProxy,
      long firstRowTime,
      long lastRowTime) {
    final var preparedStatement = selectBlobStatements.get(tenantId);
    final var sketchTypesList = new ArrayList<Byte>();
    sketchTypes.forEach(s -> sketchTypesList.add(s.getProxy()));

    // Bind the prepared statement with the provided values.
    final var boundStatement =
        preparedStatement.bind(
            timeIndex,
            streamNameProxy,
            durationType.getProxy(),
            attributeProxy,
            sketchTypesList,
            firstRowTime,
            lastRowTime);
    return boundStatement;
  }

  public BoundStatement makeSelectBlobContextStatement(
      TenantId tenantId,
      Set<DataSketchType> sketchTypes,
      int streamNameProxy,
      short attributeProxy,
      long specificEndTime) {
    final var preparedStatement = selectBlobContextStatements.get(tenantId);
    final var sketchTypesList = new ArrayList<Byte>();
    sketchTypes.forEach(s -> sketchTypesList.add(s.getProxy()));

    // Bind the prepared statement with the provided values.
    final var boundStatement =
        preparedStatement.bind(streamNameProxy, attributeProxy, sketchTypesList);
    return boundStatement;
  }

  private static String makeSelectBlobQuery(String keyspaceName) {
    return String.format(
        "SELECT time_index, stream_name_proxy, duration_type, attribute_proxy,"
            + " sketch_type, end_time, count, size, sketch_header, sketch_data FROM %s.%s WHERE"
            + " time_index=? AND stream_name_proxy=? AND duration_type=?"
            + " AND attribute_proxy=? AND sketch_type IN ? AND end_time>=? AND end_time<=?"
            + " ORDER BY stream_name_proxy, duration_type, attribute_proxy, sketch_type, end_time",
        keyspaceName, TABLE_SKETCH_BLOB);
  }

  private static String makeSelectBlobContextQuery(String keyspaceName) {
    return String.format(
        "SELECT stream_name_proxy, attribute_proxy,"
            + " sketch_type, end_time, count, size, sketch_header, sketch_data FROM %s.%s WHERE"
            + " stream_name_proxy=? AND attribute_proxy=? AND sketch_type IN ?"
            + " ORDER BY attribute_proxy, sketch_type",
        keyspaceName, TABLE_SKETCH_BLOB_CONTEXT);
  }

  public long getSummaryTimeIndex(long startTime) {
    long timeIndex = Utils.floor(startTime, summaryTimeIndexWidth);
    return timeIndex;
  }

  public long getBlobTimeIndex(long startTime) {
    long timeIndex = Utils.floor(startTime, blobTimeIndexWidth);
    return timeIndex;
  }

  public void clearTenant(TenantId tenantId) {
    tenantToKeyspaceMap.remove(tenantId);
    insertSummaryStatements.remove(tenantId);
    insertSummaryContextStatements.remove(tenantId);
    deleteSummaryContextStatements.remove(tenantId);
    insertBlobStatements.remove(tenantId);
    insertBlobContextStatements.remove(tenantId);
    deleteBlobContextStatements.remove(tenantId);
    selectBlobStatements.remove(tenantId);
    selectBlobContextStatements.remove(tenantId);
  }
}
