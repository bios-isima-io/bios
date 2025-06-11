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
package io.isima.bios.data.impl.maintenance;

import static io.isima.bios.admin.v1.AdminConstants.ROOT_FEATURE;
import static java.lang.Boolean.TRUE;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.annotations.VisibleForTesting;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Range;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostProcessScheduler {

  private static final Logger logger = LoggerFactory.getLogger(PostProcessScheduler.class);

  public static final String FEATURE_DELAY_MARGIN_KEY = "prop.featureDelayMarginMillis";
  private static final long FEATURE_DELAY_MARGIN_DEFAULT = 5000; // 5 seconds
  public static final String FAST_FEATURE_DELAY_MARGIN_KEY = "prop.maintenance.fastTrackMargin";
  private static final long FAST_FEATURE_DELAY_MARGIN_DEFAULT = 1000; // 1 second

  private static final String FAST_TRACK_REQUIRED_INTERVAL =
      "prop.maintenance.fastTrackRequiredInterval";
  private static final long DEFAULT_FAST_TRACK_REQUIRED_INTERVAL = 30000;

  // ONLY FOR TEST: scheduler table name
  public static String PROPERTY_TABLE_NAME = "com.fractals.tfos.data.impl.maintenance.tableName";
  public static String PROPERTY_SKETCH_RECORDS_TABLE_NAME =
      "com.fractals.tfos.data.impl.maintenance.sketchRecordsTableName";

  public static final String INFERENCE_INTERVAL_KEY = "prop.inferenceInterval";
  public static final Long INFERENCE_INTERVAL_DEFAULT = 500000L;

  private static long MAX_REFETCH_TIME = 20000;

  private final String tableName;
  private final String sketchTableName;

  private static String makeCreateTableQuery(String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s"
            + " ("
            + "   tenant text,"
            + "   signal text,"
            + "   signal_version bigint,"
            + "   substream text,"
            + "   substream_version bigint,"
            + "   done_since timestamp,"
            + "   done_until timestamp,"
            + "   requested boolean,"
            + "   PRIMARY KEY ((tenant, signal, signal_version), substream, substream_version))"
            + " WITH CLUSTERING ORDER BY (substream ASC, substream_version DESC)"
            + " AND comment = 'post-process record table version 2'",
        CassandraConstants.KEYSPACE_ADMIN, tableName);
  }

  private static String makeFetchRecordsQuery(String tableName) {
    return String.format(
        "SELECT substream, substream_version, done_since, done_until, requested"
            + " FROM %s.%s WHERE tenant = ? AND signal = ? AND signal_version = ?",
        CassandraConstants.KEYSPACE_ADMIN, tableName);
  }

  private static String makeLastRecordQuery(String tableName) {
    return String.format(
        "SELECT done_since, done_until, requested FROM %s.%s"
            + " WHERE tenant = ? AND signal = ? AND signal_version = ?"
            + " AND substream = ? AND substream_version = ?",
        CassandraConstants.KEYSPACE_ADMIN, tableName);
  }

  private static String makeInsertRecordQuery(String tableName) {
    return String.format(
        "INSERT INTO %s.%s (tenant, signal, signal_version, substream, substream_version,"
            + " done_since, done_until, requested) values (?, ?, ?, ?, ?, ?, ?, false)",
        CassandraConstants.KEYSPACE_ADMIN, tableName);
  }

  private static String makeRequestRefreshQuery(String tableName) {
    return String.format(
        "INSERT INTO %s.%s (tenant, signal, signal_version, substream, substream_version,"
            + " requested) values (?, ?, ?, ?, ?, true)",
        CassandraConstants.KEYSPACE_ADMIN, tableName);
  }

  private static String makeCreateSketchTableQuery(String sketchTableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s"
            + " ("
            + "   tenant text,"
            + "   stream_name_proxy int,"
            + "   duration_type tinyint,"
            + "   attribute_proxy smallint,"
            + "   sketch_type tinyint,"
            + "   base_version timestamp,"
            + "   done_since timestamp,"
            + "   done_until timestamp,"
            + "   attribute text,"
            + "   stream text,"
            + "   PRIMARY KEY ((tenant), stream_name_proxy, duration_type, attribute_proxy,"
            + "   sketch_type))"
            + " WITH CLUSTERING ORDER BY (stream_name_proxy ASC, duration_type ASC,"
            + "   attribute_proxy ASC, sketch_type ASC)"
            + " AND comment = 'data sketch records table'",
        CassandraConstants.KEYSPACE_ADMIN, sketchTableName);
  }

  private static String makeFetchSketchRecordsQuery(String sketchTableName) {
    return String.format(
        "SELECT duration_type, attribute_proxy, sketch_type, done_since, done_until"
            + " FROM %s.%s WHERE tenant = ? AND stream_name_proxy = ?",
        CassandraConstants.KEYSPACE_ADMIN, sketchTableName);
  }

  private static String makeSketchLastRecordQuery(String sketchTableName) {
    return String.format(
        "SELECT done_since, done_until FROM %s.%s"
            + " WHERE tenant = ? AND stream_name_proxy = ? AND duration_type = ?"
            + " AND attribute_proxy = ? AND sketch_type = ?",
        CassandraConstants.KEYSPACE_ADMIN, sketchTableName);
  }

  private static String makeSketchEarliestForStreamQuery(String sketchTableName) {
    return String.format(
        "SELECT min(done_until) FROM %s.%s WHERE tenant = ? AND stream_name_proxy = ?",
        CassandraConstants.KEYSPACE_ADMIN, sketchTableName);
  }

  private static String makeInsertSketchRecordQuery(String sketchTableName) {
    return String.format(
        "INSERT INTO %s.%s (tenant, stream_name_proxy, duration_type, attribute_proxy,"
            + " sketch_type, base_version, done_since, done_until, attribute, stream)"
            + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        CassandraConstants.KEYSPACE_ADMIN, sketchTableName);
  }

  private final CassandraConnection cassandraConnection;
  private final DataEngineImpl dataEngine;

  private PreparedStatement statementFetchRecords;
  private PreparedStatement statementFetchLastRecord;
  private PreparedStatement statementInsert;
  private PreparedStatement statementFetchSketchRecords;
  private PreparedStatement statementFetchSketchLastRecord;
  private PreparedStatement statementFetchSketchEarliestForStream;
  private PreparedStatement statementInsertSketch;
  private PreparedStatement statementRequestRefresh;

  /**
   * FOR TEST ONLY: Epoch milliseconds to hard stop rollup. The value can be some future time. Value
   * 0 means no hard stop configuration.
   */
  @Getter private long rollupStopTime;

  /** The class constructor. */
  public PostProcessScheduler(CassandraConnection cassandraConnection, DataEngineImpl dataEngine) {
    this(
        cassandraConnection,
        dataEngine,
        System.getProperty(PROPERTY_TABLE_NAME) != null
            ? System.getProperty(PROPERTY_TABLE_NAME)
            : CassandraConstants.TABLE_POSTPROCESS_RECORDS,
        System.getProperty(PROPERTY_SKETCH_RECORDS_TABLE_NAME) != null
            ? System.getProperty(PROPERTY_SKETCH_RECORDS_TABLE_NAME)
            : CassandraConstants.TABLE_DATA_SKETCH_RECORDS);
  }

  private PostProcessScheduler(
      CassandraConnection cassandraConnection,
      DataEngineImpl dataEngine,
      String tableName,
      String sketchTableName) {
    this.cassandraConnection = Objects.requireNonNull(cassandraConnection);
    this.dataEngine = Objects.requireNonNull(dataEngine);
    final String className = this.getClass().getSimpleName();
    logger.debug("{} initializing ###", className);
    this.tableName = Objects.requireNonNull(tableName);
    this.sketchTableName = sketchTableName;
    try {
      final boolean hasTable1 =
          cassandraConnection.verifyTable(CassandraConstants.KEYSPACE_ADMIN, tableName);

      if (!hasTable1) {
        final String createTableQuery = makeCreateTableQuery(tableName);
        logger.debug("creating postprocess record table: {}", createTableQuery);
        execute(createTableQuery);
      }

      if (!cassandraConnection.verifyTable(CassandraConstants.KEYSPACE_ADMIN, tableName, 30)) {
        throw new ApplicationException(
            String.format(
                "Failed to creating table %s.%s", CassandraConstants.KEYSPACE_ADMIN, tableName));
      }

      final boolean hasSketchTable =
          cassandraConnection.verifyTable(CassandraConstants.KEYSPACE_ADMIN, sketchTableName);

      if (!hasSketchTable) {
        final String createTableQuery = makeCreateSketchTableQuery(sketchTableName);
        logger.debug("creating data sketch records table: {}", createTableQuery);
        execute(createTableQuery);
      }

      if (!cassandraConnection.verifyTable(
          CassandraConstants.KEYSPACE_ADMIN, sketchTableName, 30)) {
        throw new ApplicationException(
            String.format(
                "Failed to creating table %s.%s",
                CassandraConstants.KEYSPACE_ADMIN, sketchTableName));
      }

      final RetryHandler retryHandler =
          new RetryHandler("Prepared statements for postprocess record table", logger, "");
      while (true) {
        final var session = cassandraConnection.getSession();
        try {
          statementFetchRecords = session.prepare(makeFetchRecordsQuery(tableName));
          statementFetchLastRecord = session.prepare(makeLastRecordQuery(tableName));
          statementInsert = session.prepare(makeInsertRecordQuery(tableName));
          statementFetchSketchRecords =
              session.prepare(makeFetchSketchRecordsQuery(sketchTableName));
          statementFetchSketchLastRecord =
              session.prepare(makeSketchLastRecordQuery(sketchTableName));
          statementFetchSketchEarliestForStream =
              session.prepare(makeSketchEarliestForStreamQuery(sketchTableName));
          statementInsertSketch = session.prepare(makeInsertSketchRecordQuery(sketchTableName));
          statementRequestRefresh = session.prepare(makeRequestRefreshQuery(tableName));
          break;
        } catch (DriverException e) {
          retryHandler.handleError(e);
        }
      }

    } catch (Throwable t) {
      logger.error("Caught an exception during initialization of " + className, t);
      throw new RuntimeException(t);
    }
    logger.debug("### {} initialized", className);
  }

  /**
   * Schedules post processes that are ready to execute at current time.
   *
   * @param streamDesc Target signal stream
   * @param margin Explicit margin time in milliseconds. Ignored if the value is zero
   * @return Post process specifiers to execute. The return value is always non-null.
   * @throws ApplicationException When an unexpected error happens.
   * @throws NoSuchTenantException When the tenant is removed during the operation.
   */
  public CompletableFuture<PostProcessSpecifiers> schedule(
      StreamDesc streamDesc, long executionTime, long margin, ExecutionState state) {
    return schedule(streamDesc, executionTime, margin, new PostProcessSpecifiers(), state);
  }

  /**
   * Schedules post processes that are ready to execute at current time.
   *
   * @param streamDesc Target signal stream
   * @param margin Explicit margin time in milliseconds. Ignored if the value is zero
   * @return Post process specifiers to execute. The return value is always non-null.
   * @throws ApplicationException When an unexpected error happens.
   * @throws NoSuchTenantException When the tenant is removed during the operation.
   */
  public CompletableFuture<PostProcessSpecifiers> schedule(
      StreamDesc streamDesc,
      long executionTime,
      long margin,
      PostProcessSpecifiers prevSpecs,
      ExecutionState state) {
    return buildSpecifiers(streamDesc, executionTime, margin, state)
        .thenComposeAsync(
            (specs) -> scheduleCore(streamDesc, specs, prevSpecs, state), state.getExecutor())
        .exceptionally(
            (t) -> {
              if (t != null) {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                if (cause instanceof NoSuchTenantException) {
                  return null; // then post process finishes on the next stage
                }
                // rethrow otherwise
                throw new CompletionException(cause);
              }
              return null;
            });
  }

  private CompletableFuture<PostProcessSpecifiers> buildSpecifiers(
      StreamDesc streamDesc, long executionTime, long margin, ExecutionState state) {

    final var specifiers = new PostProcessSpecifiers();
    specifiers.setProcessExecutionTime(executionTime);
    long rollupPoint = 0;
    // set rollup point
    if (rollupStopTime > 0) {
      rollupPoint = rollupStopTime;
    } else if (margin > 0) {
      rollupPoint = executionTime - margin;
    }

    boolean isFastLaneRequired = false;
    // TODO(Naoki): Also check context features
    if (streamDesc.getType() == StreamType.SIGNAL && streamDesc.getPostprocesses() != null) {
      long shortestInterval = Long.MAX_VALUE;
      for (var postProcess : streamDesc.getPostprocesses()) {
        if (postProcess.getRollups() == null) {
          continue;
        }
        for (var rollup : postProcess.getRollups()) {
          final var interval = rollup.getInterval().getValueInMillis();
          shortestInterval = Math.min(shortestInterval, interval);
        }
      }
      final var fastTrackRequiredInterval =
          SharedProperties.getCached(
              FAST_TRACK_REQUIRED_INTERVAL, DEFAULT_FAST_TRACK_REQUIRED_INTERVAL);
      isFastLaneRequired = shortestInterval < fastTrackRequiredInterval;
      specifiers.setShortestInterval(shortestInterval);
    }
    specifiers.setFastTrackRequired(isFastLaneRequired);

    final CompletableFuture<Long> rollupPointFuture;
    if (rollupPoint > 0) {
      rollupPointFuture = CompletableFuture.completedFuture(rollupPoint);
    } else {
      rollupPointFuture =
          getFeatureMarginTime(isFastLaneRequired, state)
              .thenApply((configuredMargin) -> executionTime - configuredMargin);
    }
    return rollupPointFuture.thenApply(
        (finalRollupPoint) -> {
          specifiers.setRollupPoint(finalRollupPoint);
          return specifiers;
        });
  }

  public CompletableFuture<Long> getFeatureMarginTime(boolean isFastTrack, ExecutionState state) {
    final String propKey;
    final long defaultMargin;
    if (isFastTrack) {
      propKey = FAST_FEATURE_DELAY_MARGIN_KEY;
      defaultMargin = FAST_FEATURE_DELAY_MARGIN_DEFAULT;
    } else {
      propKey = FEATURE_DELAY_MARGIN_KEY;
      defaultMargin = FEATURE_DELAY_MARGIN_DEFAULT;
    }
    return BiosModules.getSharedProperties().getPropertyLongAsync(propKey, defaultMargin, state);
  }

  public CompletableFuture<PostProcessSpecifiers> scheduleForTest(
      StreamDesc streamDesc, long rollupPoint, ExecutionState state) {
    final var specs = new PostProcessSpecifiers();
    specs.setRollupPoint(rollupPoint);
    return scheduleCore(streamDesc, specs, new PostProcessSpecifiers(), state);
  }

  /**
   * This method implements {@link #schedule} method.
   *
   * <p>The method should be called only internally or for testing. The exception from this method
   * should be handled as an ApplicationException.
   *
   * @param streamDesc Target stream
   * @param specs Carries post process parameters for the next execution
   * @param prevSpecs Carries post process parameters for the previous execution
   * @return Post process specifiers to execute.
   * @throws DriverException for any errors from Cassandra driver.
   */
  private CompletableFuture<PostProcessSpecifiers> scheduleCore(
      StreamDesc streamDesc,
      PostProcessSpecifiers specs,
      PostProcessSpecifiers prevSpecs,
      ExecutionState state) {
    if (streamDesc == null) {
      throw new IllegalArgumentException("streamDesc may not be null");
    }

    final var rollupPoint = specs.getRollupPoint();
    specs.setStreamDesc(streamDesc);

    final long indexingInterval = streamDesc.getIndexingInterval();

    final String tenant = streamDesc.getParent().getName();
    final String stream = streamDesc.getName();
    final Long streamVersion = streamDesc.getVersion();
    final Long schemaVersion = streamDesc.getSchemaVersion();
    final Map<String, DigestSpecifier> views = new HashMap<>();
    final Map<String, DigestSpecifier> rollups = new HashMap<>();
    final Map<String, DigestSpecifier> contextIndexes = new HashMap<>();
    final Map<String, DigestSpecifier> contextFeatures = new HashMap<>();
    final Map<DataSketchKey, DataSketchSpecifier> sketches = new HashMap<>();

    return CompletableFuture.allOf(
            fetchRecords(
                tenant,
                stream,
                streamVersion,
                schemaVersion,
                views,
                rollups,
                contextIndexes,
                contextFeatures,
                state),
            fetchSketchRecords(tenant, streamDesc, sketches, state))
        .thenApplyAsync(
            (none) -> {
              boolean refreshRequested = false;
              if (streamDesc.getType() == StreamType.CONTEXT) {
                scheduleContextIndexing(
                    streamDesc, tenant, stream, streamVersion, contextIndexes, rollupPoint, specs);
                scheduleContextFeatureCalculation(streamDesc, rollupPoint, contextFeatures, specs);
                final var rootFeature = contextFeatures.get(ROOT_FEATURE);
                refreshRequested =
                    specs.getContextFeatures().stream().anyMatch((feature) -> feature.requested)
                        || (rootFeature != null && rootFeature.getRequested() == TRUE);
              } else {
                scheduleIndexing(
                    streamDesc, tenant, stream, streamVersion, views, rollupPoint, specs);
              }
              scheduleSummarization(
                  streamDesc,
                  tenant,
                  stream,
                  streamVersion,
                  views,
                  sketches,
                  indexingInterval,
                  rollupPoint,
                  specs,
                  refreshRequested);

              setTimeRange(specs);

              return specs;
            },
            state.getExecutor());
  }

  /** Schedules next signal tag inference based on the previous execution. */
  public PostProcessSpecifiers scheduleTagInference(PostProcessSpecifiers prevSpecs) {
    final var streamDesc = prevSpecs.getStreamDesc();
    if (streamDesc.getType() != StreamType.SIGNAL) {
      return null;
    }
    final var lastInferenceTime = prevSpecs.getLastInferenceTime();
    final long executionTime;
    if (lastInferenceTime == 0) {
      // first time
      executionTime = System.currentTimeMillis();
    } else {
      final long inferenceInterval =
          SharedProperties.getCached(INFERENCE_INTERVAL_KEY, INFERENCE_INTERVAL_DEFAULT);
      executionTime = lastInferenceTime + inferenceInterval;
    }
    return PostProcessSpecifiers.inferSignalTags(streamDesc, executionTime);
  }

  private void setTimeRange(PostProcessSpecifiers specifiers) {
    setTimeRange("indexes", specifiers, specifiers.getIndexes());
    setTimeRange("rollup", specifiers, specifiers.getRollups());
    setTimeRange("sketches", specifiers, specifiers.getSketches());
  }

  private void setTimeRange(
      String digestName,
      PostProcessSpecifiers specs,
      List<? extends DigestSpecifierBase> specifiers) {
    if (specifiers == null || specifiers.isEmpty()) {
      return;
    }

    final boolean debugLogEnabled =
        specs
            .getStreamDesc()
            .getName()
            .equalsIgnoreCase(BiosModules.getSharedConfig().getProperty("prop.rollupDebugSignal"));

    long earliest = specs.getTimeRange() != null ? specs.getTimeRange().getBegin() : Long.MAX_VALUE;
    long latest = specs.getTimeRange() != null ? specs.getTimeRange().getEnd() : Long.MIN_VALUE;
    for (var spec : specifiers) {
      final var start = spec.getStartTime();
      final var end = spec.getEndTime();
      if (start <= 0 || end <= 0) {
        continue;
      }
      long refetchTime = spec.getRefetchTime();
      earliest = Math.min(earliest, start - refetchTime);
      latest = Math.max(latest, end);
    }
    if (debugLogEnabled) {
      logger.info(
          "  time range after {}: [{} : {}]",
          digestName,
          StringUtils.tsToIso8601(earliest),
          StringUtils.tsToIso8601(latest));
    }
    if (earliest < latest) {
      specs.setTimeRange(new Range(earliest, latest));
    }
  }

  private CompletableFuture<Void> fetchRecords(
      String tenant,
      String signal,
      Long version,
      Long schemaVersion,
      Map<String, DigestSpecifier> views,
      Map<String, DigestSpecifier> rollups,
      Map<String, DigestSpecifier> contextIndexes,
      Map<String, DigestSpecifier> contextFeatures,
      ExecutionState state) {
    assert !ExecutorManager.isInIoThread();
    return cassandraConnection
        .executeAsync(statementFetchRecords.bind(tenant, signal, schemaVersion), state)
        .thenAcceptAsync(
            (results) -> {
              while (!results.isExhausted()) {
                final Row row = results.one();
                final String substream = row.getString("substream");
                final long substreamVersion = row.getLong("substream_version");
                final long since = row.getLong("done_since");
                final long until = row.getLong("done_until");
                final boolean requested = row.getBool("requested");
                boolean isRoot = substream.equals(ROOT_FEATURE);
                final StreamDesc subStreamDesc =
                    BiosModules.getAdminInternal().getStreamOrNull(tenant, substream, version);
                if ((subStreamDesc == null
                        || !subStreamDesc.getSchemaVersion().equals(substreamVersion))
                    && !isRoot) {
                  // old version
                  continue;
                }
                if (subStreamDesc != null) {
                  logger.trace(
                      "RECORD tenant={} signal={} version={}({}) substream={} version={}({})",
                      tenant,
                      signal,
                      schemaVersion,
                      StringUtils.tsToIso8601(schemaVersion),
                      substream,
                      subStreamDesc.getSchemaVersion(),
                      StringUtils.tsToIso8601(subStreamDesc.getSchemaVersion()));
                }
                DigestSpecifier spec =
                    new DigestSpecifier(
                        substream, substreamVersion, -1, -1, since, until, null, requested);
                if (isRoot) {
                  contextFeatures.put(substream, spec);
                } else {
                  switch (subStreamDesc.getType()) {
                    case VIEW:
                      views.put(substream, spec);
                      break;
                    case CONTEXT_INDEX:
                      contextIndexes.put(substream, spec);
                      break;
                    case CONTEXT_FEATURE:
                      contextFeatures.put(substream, spec);
                      break;
                    default:
                      rollups.put(substream, spec);
                      break;
                  }
                }
              }
            },
            state.getExecutor())
        .toCompletableFuture();
  }

  private CompletableFuture<Void> fetchSketchRecords(
      String tenant,
      StreamDesc streamDesc,
      Map<DataSketchKey, DataSketchSpecifier> sketches,
      ExecutionState state) {
    final int streamNameProxy = streamDesc.getStreamNameProxy();
    final boolean debugLogEnabled =
        streamDesc
            .getName()
            .equalsIgnoreCase(BiosModules.getSharedConfig().getProperty("prop.rollupDebugSignal"));
    return cassandraConnection
        .executeAsync(statementFetchSketchRecords.bind(tenant, streamNameProxy), state)
        .thenAcceptAsync(
            (results) -> {
              while (!results.isExhausted()) {
                final Row row = results.one();
                final DataSketchDuration durationType =
                    DataSketchDuration.fromProxy(row.getByte(0));
                final short attributeProxy = row.getShort(1);
                final DataSketchType sketchType = DataSketchType.fromProxy(row.getByte(2));
                final long since = row.getLong(3);
                final long until = row.getLong(4);
                if (debugLogEnabled) {
                  logger.info(
                      "RECORD tenant={} streamNameProxy={} durationType={} attributeProxy={}"
                          + " sketchType={} since={} until={}",
                      tenant,
                      streamNameProxy,
                      durationType,
                      attributeProxy,
                      sketchType,
                      StringUtils.tsToIso8601(since),
                      StringUtils.tsToIso8601(until));
                }
                sketches.put(
                    new DataSketchKey(durationType, attributeProxy, sketchType),
                    new DataSketchSpecifier(
                        durationType, attributeProxy, sketchType, -1, -1, since, until));
              }
            },
            state.getExecutor())
        .toCompletableFuture();
  }

  /*
   * PostProcessRecords only stores the timestamp of the latest sketch. Among those latest (current)
   * sketches, we want to find the earliest item, so that we do not delete that and later items.
   * Currently all the sketches in one set have the same timestamp, but if we implement different
   * schedules for different attributes/sketches, they will differ. We only want to delete sketches
   * older than that earliest one among the latest (current) set of sketches.
   */
  public Long getTimestampOfLatestSketches(String tenant, int streamNameProxy) {
    assert !ExecutorManager.isInIoThread();
    final Long until;
    final ResultSet results =
        execute(statementFetchSketchEarliestForStream.bind(tenant, streamNameProxy));
    if (!results.isExhausted()) {
      final Row row = results.one();
      until = row.getLong(0);
    } else {
      until = null;
    }
    return until;
  }

  private void scheduleIndexing(
      StreamDesc signalDesc,
      String tenant,
      String signal,
      Long version,
      Map<String, DigestSpecifier> views,
      long rollupPoint,
      PostProcessSpecifiers specifiers) {
    if (signalDesc.getViews() == null || signalDesc.getViews().isEmpty()) {
      // This signal does not have rollup configuration.
      return;
    }
    final var signalCassStream = dataEngine.getCassStream(signalDesc);
    if (signalCassStream == null) {
      // Deleted alrady
      return;
    }

    final boolean debugLogEnabled =
        signalDesc
            .getName()
            .equalsIgnoreCase(BiosModules.getSharedConfig().getProperty("prop.rollupDebugSignal"));

    if (debugLogEnabled) {
      logger.info("  Scheduling indexing; signal={}", signalDesc.getName());
    }

    var longestInterval = 5 * 60 * 1000; // at least five minutes
    var minimumInterval = Long.MAX_VALUE;
    final Map<String, Rollup> viewToRollup;
    final var postprocesses = signalDesc.getPostprocesses();
    if (postprocesses == null) {
      viewToRollup = null;
    } else {
      viewToRollup = new HashMap<>();
      for (var pp : postprocesses) {
        for (var rollup : pp.getRollups()) {
          viewToRollup.put(pp.getView().toLowerCase(), rollup);
          final var interval = rollup.getInterval().getValueInMillis();
          longestInterval = Math.max(longestInterval, interval);
          minimumInterval = Math.min(minimumInterval, interval);
        }
      }
    }

    // analyze view records
    var viewNamesInStoredRecords = new HashSet<>(views.keySet());
    final long signalOrigin = signalDesc.getSchemaVersion();
    if (debugLogEnabled) {
      logger.info("  signalOrigin: {}", StringUtils.tsToIso8601(signalOrigin));
    }
    long indexBegin = Long.MIN_VALUE;
    final Map<String, Long> origins = new HashMap<>();
    for (ViewDesc view : signalDesc.getViews()) {
      final String viewName = AdminUtils.makeViewStreamName(signal, view.getName());
      final StreamDesc viewStreamDesc =
          BiosModules.getAdminInternal().getStreamOrNull(tenant, viewName, version);
      if (viewStreamDesc == null) {
        continue;
      }
      if (viewToRollup == null && view.getIndexTableEnabled() != TRUE) {
        continue;
      }
      final long interval;
      final var rollup =
          viewToRollup != null ? viewToRollup.get(view.getName().toLowerCase()) : null;
      if (rollup != null) {
        interval = rollup.getInterval().getValueInMillis();
      } else if (view.getIndexTableEnabled() == Boolean.TRUE) {
        interval =
            Objects.requireNonNullElse(
                view.getTimeIndexInterval(), viewStreamDesc.getIndexingInterval());
      } else {
        interval = viewStreamDesc.getIndexingInterval();
      }
      DigestSpecifier spec = views.get(viewName);
      final long doneUntil;
      if (spec != null) {
        doneUntil = spec.getDoneUntil();
        spec.setInterval(interval);
        viewNamesInStoredRecords.remove(spec.getName());
      } else {
        doneUntil = rollupPoint / interval * interval - interval;
        spec =
            new DigestSpecifier(
                viewName,
                viewStreamDesc.getSchemaVersion(),
                -1,
                -1,
                doneUntil,
                doneUntil,
                interval);
        views.put(spec.getName(), spec);
      }

      final long availableSegmentEnd = rollupPoint / interval * interval;
      if (debugLogEnabled) {
        logger.info(
            "  saved (or new) spec: [{}:{}], doneUntil={}, availableSegmentEnd={}, behind={} [{}]",
            StringUtils.tsToIso8601(spec.getDoneSince()),
            StringUtils.tsToIso8601(spec.getDoneUntil()),
            StringUtils.tsToIso8601(doneUntil),
            StringUtils.tsToIso8601(availableSegmentEnd),
            StringUtils.shortReadableDuration(rollupPoint - doneUntil),
            view.getName());
      }
      if (doneUntil + interval <= availableSegmentEnd && availableSegmentEnd > signalOrigin) {
        final var endTime = doneUntil + interval;
        final var specOut =
            new DigestSpecifier(
                viewName,
                spec.getVersion(),
                spec.getDoneUntil(),
                endTime,
                spec.getDoneSince(),
                endTime);
        specOut.calculateDoneCoverage(signalOrigin);
        specOut.setInterval(interval);
        if (debugLogEnabled) {
          logger.info(
              "  specOut: range=[{} : {}] done=[{} : {}] ({}) [{}]",
              StringUtils.tsToIso8601(specOut.getStartTime()),
              StringUtils.tsToIso8601(specOut.getEndTime()),
              StringUtils.tsToIso8601(specOut.getDoneSince()),
              StringUtils.tsToIso8601(specOut.getDoneUntil()),
              StringUtils.shortReadableDuration(specOut.getEndTime() - specOut.getStartTime()),
              view.getName());
        }
        specifiers.addIndex(specOut);
      }

      long signalOriginForView = viewStreamDesc.getPostProcessOrigin();
      origins.put(viewStreamDesc.getName(), signalOriginForView);
      if (signalOriginForView < spec.getDoneSince()) {
        indexBegin = Math.max(indexBegin, spec.getDoneSince());
      }
    }
    // Records remaining at this point are not valid anymore, removing them from the views
    viewNamesInStoredRecords.forEach((name) -> views.remove(name));

    // Limit indexBegin not to go beyond the signal table TTL where only tombstones exist.
    final var ttl = signalCassStream.getTtlInTable();
    final var limitedByTtl = rollupPoint - ttl + longestInterval * 2;
    if (indexBegin < limitedByTtl) {
      indexBegin = limitedByTtl;
    }

    // If the index spec is empty, you can do retroactive rollups
    if (specifiers.isEmpty()) {
      final long beginPoint = (indexBegin / longestInterval - 1) * longestInterval;
      for (Map.Entry<String, DigestSpecifier> entry : views.entrySet()) {
        final String name = entry.getKey();
        final DigestSpecifier spec = entry.getValue();
        final Long signalOriginForView = origins.get(name);
        if (debugLogEnabled) {
          logger.info(
              "signalOriginForView: {}, beginPoint: {}, doneSince: {}",
              StringUtils.tsToIso8601(signalOriginForView),
              StringUtils.tsToIso8601(beginPoint),
              StringUtils.tsToIso8601(spec.getDoneSince()));
        }
        if (signalOriginForView < spec.getDoneSince() && beginPoint < spec.getDoneSince()) {
          final var specOut =
              new DigestSpecifier(
                  name,
                  spec.getVersion(),
                  beginPoint,
                  spec.getDoneSince(),
                  beginPoint,
                  spec.getDoneUntil(),
                  spec.getInterval());
          specOut.calculateDoneCoverage(signalOrigin);
          specifiers.addIndex(specOut);
        }
      }
    }
    alignSpecs(specifiers.getIndexes(), signalOrigin, rollupPoint, 900000);
    if (debugLogEnabled) {
      for (var spec : specifiers.getIndexes()) {
        logger.info(
            "  finalSpec: range=[{} : {}] done=[{} : {}] ({}) [{}]",
            StringUtils.tsToIso8601(spec.getStartTime()),
            StringUtils.tsToIso8601(spec.getEndTime()),
            StringUtils.tsToIso8601(spec.getDoneSince()),
            StringUtils.tsToIso8601(spec.getDoneUntil()),
            StringUtils.shortReadableDuration(spec.getEndTime() - spec.getStartTime()),
            spec.getName());
      }
    }
  }

  protected static void alignSpecs(
      List<DigestSpecifier> indexSpecs,
      long signalOriginTime,
      long rollupPoint,
      long maximumAllowedInterval) {
    if (indexSpecs.size() < 2) {
      return;
    }
    long leftMost = Long.MAX_VALUE;
    long rightMost = Long.MIN_VALUE;
    long longestInterval = 0;
    for (var spec : indexSpecs) {
      leftMost = Math.min(spec.getStartTime(), leftMost);
      rightMost = Math.max(spec.getEndTime(), rightMost);
      longestInterval = Math.max(spec.getInterval(), longestInterval);
    }

    // Try to extend rightMost coverage to accelerate catching up rollups if they are behind
    final long rightExtensionPoint = Math.min(rollupPoint, leftMost + maximumAllowedInterval);
    if (rightExtensionPoint > rightMost) {
      final var extension = (rightExtensionPoint - rightMost) / longestInterval * longestInterval;
      rightMost += extension;
    }

    // Try to extend leftMost coverage to accelerate catching up rollups if they are behind
    final long leftExtensionPoint = Math.max(signalOriginTime, rightMost - maximumAllowedInterval);
    if (leftExtensionPoint < leftMost) {
      final var extension = (leftMost - leftExtensionPoint) / longestInterval * longestInterval;
      leftMost -= extension;
    }

    final var specsToAppend = new ArrayList<DigestSpecifier>();
    for (var spec : indexSpecs) {
      final var interval = spec.getInterval();

      long doneSince = spec.getDoneSince();
      long doneUntil = spec.getDoneUntil();

      // extend specs to the right edge
      long right = spec.getEndTime();
      while (right < rightMost && right + interval > doneUntil) {
        right += interval;
        doneUntil = right;
        final var additionalSpec =
            new DigestSpecifier(
                spec.getName(),
                spec.getVersion(),
                right - interval,
                right,
                doneSince,
                doneUntil,
                interval);
        specsToAppend.add(additionalSpec);
      }

      // extend specs to the left edge
      long left = spec.getStartTime();
      while (left > leftMost && left - interval < doneSince) {
        left -= interval;
        doneSince = left;
        final var additionalSpec =
            new DigestSpecifier(
                spec.getName(),
                spec.getVersion(),
                left,
                left + interval,
                doneSince,
                doneUntil,
                interval);
        specsToAppend.add(additionalSpec);
      }
    }
    indexSpecs.addAll(specsToAppend);
  }

  private void scheduleContextIndexing(
      StreamDesc contextDesc,
      String tenant,
      String context,
      Long version,
      Map<String, DigestSpecifier> contextIndexes,
      long rollupPoint,
      PostProcessSpecifiers specifiers) {
    logger.debug("  Scheduling indexing; tenant={}, context={}", tenant, context);
    if (!contextDesc.getAuditEnabled() || (contextDesc.getFeatures() == null)) {
      return;
    }
    var longestInterval = 5L * 60 * 1000; // at least five minutes
    var minimumInterval = Long.MAX_VALUE;
    for (var feature : contextDesc.getFeatures()) {
      final var interval = feature.getFeatureInterval();
      longestInterval = Math.max(longestInterval, interval);
      minimumInterval = Math.min(minimumInterval, interval);
    }

    final long contextOrigin = contextDesc.getSchemaVersion();
    logger.debug("  contextOrigin: {}", StringUtils.tsToIso8601(contextOrigin));

    for (var feature : contextDesc.getFeatures()) {
      if (feature.getIndexed() != TRUE) {
        continue;
      }
      final String indexName =
          AdminUtils.makeContextIndexStreamName(contextDesc.getName(), feature.getName());
      final StreamDesc indexStreamDesc =
          BiosModules.getAdminInternal().getStreamOrNull(tenant, indexName, version);
      if (indexStreamDesc == null) {
        continue;
      }
      final long interval = feature.getFeatureInterval();
      DigestSpecifier spec = contextIndexes.get(indexName);
      final long doneUntil;
      if (spec != null) {
        doneUntil = spec.getDoneUntil();
        spec.setInterval(interval);
        final long availableSegmentEnd = rollupPoint / interval * interval;
        logger.debug(
            "  saved spec: [{}:{}], doneUntil={}, availableSegmentEnd={}",
            StringUtils.tsToIso8601(spec.getDoneSince()),
            StringUtils.tsToIso8601(spec.getDoneUntil()),
            StringUtils.tsToIso8601(doneUntil),
            StringUtils.tsToIso8601(availableSegmentEnd));
        if (doneUntil <= availableSegmentEnd && availableSegmentEnd > contextOrigin) {
          final var endTime = doneUntil + interval;
          final var specOut =
              new DigestSpecifier(
                  indexName,
                  spec.getVersion(),
                  spec.getDoneUntil(),
                  endTime,
                  spec.getDoneSince(),
                  endTime);
          specOut.calculateDoneCoverage(contextOrigin);
          specOut.setInterval(interval);
          logger.debug(
              "  context-index rollup specOut: range=[{}:{}] done=[{}:{}]",
              StringUtils.tsToIso8601(specOut.getStartTime()),
              StringUtils.tsToIso8601(specOut.getEndTime()),
              StringUtils.tsToIso8601(specOut.getDoneSince()),
              StringUtils.tsToIso8601(specOut.getDoneUntil()));
          specifiers.addIndex(specOut);
        }
      } else {
        // Do retroactive rollups
        final var endTime = rollupPoint / interval * interval;
        final var specOut =
            new DigestSpecifier(indexName, indexStreamDesc.getVersion(), -1, endTime, -1, endTime);
        specOut.setInterval(interval);
        specOut.calculateDoneCoverage(contextOrigin);
        logger.debug(
            "  Retroactive context-index rollup specOut: range=[{}:{}] done=[{}:{}]",
            StringUtils.tsToIso8601(specOut.getStartTime()),
            StringUtils.tsToIso8601(specOut.getEndTime()),
            StringUtils.tsToIso8601(specOut.getDoneSince()),
            StringUtils.tsToIso8601(specOut.getDoneUntil()));
        specifiers.addIndex(specOut);
      }
    }
  }

  private void scheduleContextFeatureCalculation(
      StreamDesc contextDesc,
      long rollupPoint,
      Map<String, DigestSpecifier> contextFeatureSpecifiers,
      PostProcessSpecifiers specifiers) {
    if (Objects.requireNonNull(contextDesc).getType() != StreamType.CONTEXT) {
      throw new IllegalArgumentException(
          "contextDesc stream type is wrongfully " + contextDesc.getType());
    }
    if (contextDesc.getFeatures() == null) {
      return;
    }

    final var tenantName = contextDesc.getParent().getName();
    final var contextName = contextDesc.getName();
    for (var feature : contextDesc.getFeatures()) {
      final var featureName = feature.getName();
      final var featureStreamName = AdminUtils.makeRollupStreamName(contextName, featureName);
      final StreamDesc featureStreamDesc =
          BiosModules.getAdminInternal().getStreamOrNull(tenantName, featureStreamName);
      if (featureStreamDesc == null
          || !featureStreamDesc.getSchemaVersion().equals(feature.getBiosVersion())) {
        continue;
      }
      DigestSpecifier spec = contextFeatureSpecifiers.get(featureStreamName);
      if (spec == null) {
        specifiers.addContextFeature(
            new DigestSpecifier(
                featureStreamName, feature.getBiosVersion(), -1, -1, -1, -1, null, true));
      } else {
        specifiers.addContextFeature(
            new DigestSpecifier(
                spec.getName(),
                spec.getVersion(),
                spec.getDoneSince(),
                spec.getDoneUntil(),
                -1,
                -1,
                spec.getInterval(),
                spec.getRequested()));
      }
    }
  }

  /** Schedules rollups and data sketches. */
  private void scheduleSummarization(
      StreamDesc streamDesc,
      String tenant,
      String signal,
      Long signalVersion,
      Map<String, DigestSpecifier> views,
      Map<DataSketchKey, DataSketchSpecifier> sketches,
      long indexingInterval,
      long rollupPoint,
      PostProcessSpecifiers specifiers,
      boolean refreshRequested) {

    // Schedule data sketches. Use the map of sketches in the stream because some default sketches
    // may have been added in addition to the explicit sketches specified in the stream config.
    for (final var attributeMapEntry : streamDesc.getSketches().entrySet()) {
      final var attributeName = attributeMapEntry.getKey();
      final short attributeProxy = streamDesc.getAttributeProxy(attributeName);
      final long attributeVersion = streamDesc.getAttributeBaseVersion(attributeName);
      for (final var typeMapEntry : attributeMapEntry.getValue().entrySet()) {
        final var sketchType = typeMapEntry.getKey();
        for (final var durationMapEntry : typeMapEntry.getValue().entrySet()) {
          final var durationType = durationMapEntry.getKey();

          final DataSketchSpecifier sketchSpec =
              sketches.get(new DataSketchKey(durationType, attributeProxy, sketchType));
          final var nextSketchRange =
              calculateNextRange(
                  sketchSpec,
                  attributeVersion,
                  durationType.getMilliseconds(),
                  rollupPoint,
                  streamDesc,
                  refreshRequested);
          if (nextSketchRange != null) {
            final var specOut =
                new DataSketchSpecifier(
                    durationType,
                    attributeProxy,
                    sketchType,
                    nextSketchRange.startTime,
                    nextSketchRange.endTime,
                    nextSketchRange.doneSince,
                    nextSketchRange.doneUntil);
            specOut.setRequested(nextSketchRange.getRequested());
            specOut.calculateDoneCoverage(attributeVersion);
            specOut.setAttributeName(attributeName);
            specOut.setAttributeType(streamDesc.findAnyAttribute(attributeName).getAttributeType());
            specifiers.addSketch(specOut);
          }
        }
      }
    }

    // For contexts, we only calculate the sketches here; features are handled in another function.
    if (streamDesc.getType() == StreamType.CONTEXT) {
      return;
    }

    if ((streamDesc.getPostprocesses() == null) || (indexingInterval <= 0)) {
      return;
    }

    // bundle index specs by view name to make them easier to find
    final var indexSpecs = new HashMap<String, List<DigestSpecifier>>();
    specifiers
        .getIndexes()
        .forEach(
            (spec) -> {
              final var specsForView =
                  indexSpecs.computeIfAbsent(spec.getName(), (n) -> new ArrayList<>());
              specsForView.add(spec);
            });

    final long signalOrigin = streamDesc.getPostProcessOrigin();
    for (PostprocessDesc desc : streamDesc.getPostprocesses()) {
      final String viewName = AdminUtils.makeViewStreamName(signal, desc.getView());
      final List<DigestSpecifier> indexSpecsForView = indexSpecs.get(viewName);
      if (indexSpecsForView == null) {
        // This is a view that was added recently. Skip this.
        continue;
      }
      for (Rollup rollup : desc.getRollups()) {
        final String rollupName = rollup.getName();
        final StreamDesc rollupStreamDesc =
            BiosModules.getAdminInternal().getStreamOrNull(tenant, rollupName, signalVersion);
        if (rollupStreamDesc == null) {
          continue;
        }
        final long rollupVersion = rollupStreamDesc.getSchemaVersion();
        final long rollupInterval = rollup.getInterval().getValueInMillis();
        long refetchTime = 0;
        if (rollupStreamDesc.getViews().get(0).getSnapshot() == TRUE) { // accumulating count
          // Since Cassandra is an eventually-consistent database, events may appear late after
          // a time window is processed. It would cause the accumulating counter getting out of sync
          // since counter update interval is often short (e.g. 5 seconds). In order to mitigate
          // this issue, the post processor fetches events in previous time window(s). This
          // parameter determines how long we take events before the window start time.
          refetchTime =
              Math.min(rollupStreamDesc.getRollupInterval().getValueInMillis(), MAX_REFETCH_TIME);
        }
        for (var indexSpec : indexSpecsForView) {
          final var specOut =
              new DigestSpecifier(
                  rollupName,
                  rollupVersion,
                  indexSpec.getStartTime(),
                  indexSpec.getEndTime(),
                  indexSpec.getDoneSince(),
                  indexSpec.getDoneUntil());
          specOut.calculateDoneCoverage(signalOrigin);
          specOut.setInterval(rollupInterval);
          specOut.setRefetchTime(refetchTime);
          specifiers.addRollup(specOut);
        }
      }
    }
  }

  /**
   * Calculates the next range of times that digestion should be done for sketches. It figures out
   * where the digest is behind the index and tries to fill that gap, giving preference to staying
   * up-to-date on the current time (leading edge) as opposed to filling in the missing past
   * (trailing edge).
   *
   * <p>For contexts, sketches are always computed fully - no incremental time ranges.
   *
   * @return Returns null if there is nothing to do.
   */
  private DigestSpecifierBase calculateNextRange(
      DigestSpecifierBase existingDigestSpec,
      long digestVersion,
      long digestInterval,
      long rollupPoint,
      StreamDesc streamDesc,
      boolean refreshRequested) {
    DigestSpecifierBase outSpec = null;
    long digestSince;
    long digestUntil;

    // For contexts, we always compute the full sketch.
    if (streamDesc.getType() == StreamType.CONTEXT) {
      final long startTime = existingDigestSpec != null ? existingDigestSpec.getDoneSince() : -1;
      final long endTime = existingDigestSpec != null ? existingDigestSpec.getDoneUntil() : -1;
      outSpec = new DigestSpecifierBase(startTime, endTime, rollupPoint, rollupPoint);
      outSpec.setRequested(existingDigestSpec == null || refreshRequested);
      return outSpec;
    }

    // If the signal does not have any features, but it has default data sketches,
    // we may not have any index specs.
    // Even if an index spec exists, a data sketch may be able to go back beyond the beginning of
    // the index based on the attribute's base version. So if we have not scheduled it yet,
    // try again without considering the index spec.
    final var signalCassStream = dataEngine.getCassStream(streamDesc);
    if (signalCassStream == null) {
      // the signal possibly has been deleted
      return null;
    }
    final var ttl = signalCassStream.getTtlInTable();
    final long latestPossible = rollupPoint / digestInterval * digestInterval;
    final long earliestPossible =
        Math.max(
            digestVersion / digestInterval * digestInterval, rollupPoint - ttl + digestInterval);
    if (existingDigestSpec != null) {
      digestSince = existingDigestSpec.getDoneSince();
      digestUntil = existingDigestSpec.getDoneUntil();
    } else {
      digestUntil = latestPossible;
      digestSince = latestPossible;
    }
    if (digestUntil < latestPossible) {
      final long endPoint = digestUntil + digestInterval;
      outSpec = new DigestSpecifierBase(digestUntil, endPoint, digestSince, endPoint);
    } else if (digestSince > earliestPossible) {
      final long beginPoint = digestSince - digestInterval;
      outSpec = new DigestSpecifierBase(beginPoint, digestSince, beginPoint, digestUntil);
    }
    return outSpec;
  }

  /**
   * Gets current coverage of a rollup.
   *
   * @param signalStream Target signal description.
   * @param subStream Target sub-stream description.
   * @return Covered time range as DigestSpecifier. The method never returns null.
   */
  public CompletableFuture<DigestSpecifier> getDigestionCoverage(
      StreamDesc signalStream, StreamDesc subStream, ExecutionState state) {
    if (signalStream == null) {
      throw new IllegalArgumentException("streamDesc may not be null");
    }
    final String tenant = signalStream.getParent().getName();
    final String signal = signalStream.getSchemaName();
    final Long version = signalStream.getSchemaVersion();
    final String subStreamName;
    final Long subStreamVersion;
    if (subStream != null) {
      subStreamName = subStream.getSchemaName();
      subStreamVersion = subStream.getSchemaVersion();
    } else {
      subStreamName = ROOT_FEATURE;
      subStreamVersion = version;
    }
    final long rollupIntervalMillis;
    if (subStream != null) {
      if (subStream.getRollupInterval() != null) {
        rollupIntervalMillis = subStream.getRollupInterval().getValueInMillis();
      } else {
        rollupIntervalMillis = subStream.getIndexingInterval();
      }
    } else {
      rollupIntervalMillis = 300000;
    }

    final var future = new CompletableFuture<DigestSpecifier>();
    cassandraConnection.executeAsync(
        statementFetchLastRecord.bind(tenant, signal, version, subStreamName, subStreamVersion),
        state,
        (results) -> {
          if (!results.isExhausted()) {
            final Row row = results.one();
            final long doneSince = row.getLong("done_since");
            final long doneUntil = row.getLong("done_until");
            final boolean requested = row.getBool("requested");
            future.complete(
                new DigestSpecifier(
                    subStreamName,
                    subStreamVersion,
                    -1,
                    -1,
                    doneSince,
                    doneUntil,
                    null,
                    requested));
          } else {
            // Special case of no record -- no rollups have ever run.
            long initialCheckpoint = (version / rollupIntervalMillis + 1) * rollupIntervalMillis;
            future.complete(
                new DigestSpecifier(
                    subStreamName, subStreamVersion, -1, -1, initialCheckpoint, initialCheckpoint));
          }
        },
        future::completeExceptionally);
    return future;
  }

  /**
   * Records post-process completion asynchronously.
   *
   * @param signal Target signal descriptor.
   * @param specifier Specifier of the completed post process.
   */
  public CompletableFuture<Void> recordCompletion(
      StreamDesc signal, DigestSpecifierBase specifier, ExecutionState state) {
    if (signal == null || specifier == null) {
      throw new IllegalArgumentException("parameters may not be null");
    }
    if (specifier instanceof DigestSpecifier) {
      final DigestSpecifier spec = (DigestSpecifier) specifier;
      return cassandraConnection
          .executeAsync(
              statementInsert.bind(
                  signal.getParent().getName(),
                  signal.getSchemaName(),
                  signal.getSchemaVersion(),
                  spec.getName(),
                  spec.getVersion(),
                  spec.getDoneSince(),
                  spec.getDoneUntil()),
              state)
          .thenAccept((result) -> {})
          .toCompletableFuture();
    } else if (specifier instanceof DataSketchSpecifier) {
      final DataSketchSpecifier spec = (DataSketchSpecifier) specifier;
      return cassandraConnection
          .executeAsync(
              statementInsertSketch.bind(
                  signal.getParent().getName(),
                  signal.getStreamNameProxy(),
                  spec.getDataSketchDuration().getProxy(),
                  spec.getAttributeProxy(),
                  spec.getDataSketchType().getProxy(),
                  signal.getAttributeBaseVersion(spec.getAttributeName()),
                  spec.getDoneSince(),
                  spec.getDoneUntil(),
                  spec.getAttributeName(),
                  signal.getName()),
              state)
          .thenAccept((result) -> {})
          .toCompletableFuture();
    } else {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException(
              "Saving digest specifier of type " + specifier.getClass().getName()));
    }
  }

  /**
   * Requests refreshing the specified feature.
   *
   * @param stream Stream to which the target sub-stream (e.g. feature) belongs.
   * @param subStream Target subStream e.g. feature.
   */
  public CompletableFuture<Void> requestRefresh(
      StreamDesc stream, StreamDesc subStream, ExecutionState state) {
    if (stream == null) {
      throw new CompletionException(
          new IllegalArgumentException("Parameters stream and subStream may not be null"));
    }
    final String tenant = stream.getParent().getName();
    final String streamName = stream.getSchemaName();
    final Long version = stream.getSchemaVersion();
    final String subStreamName;
    final Long subStreamVersion;
    if (subStream != null) {
      subStreamName = subStream.getSchemaName();
      subStreamVersion = subStream.getSchemaVersion();
    } else {
      subStreamName = ROOT_FEATURE;
      subStreamVersion = version;
    }
    final var future = new CompletableFuture<Void>();
    cassandraConnection.executeAsync(
        statementRequestRefresh.bind(tenant, streamName, version, subStreamName, subStreamVersion),
        state,
        (results) -> future.complete(null),
        future::completeExceptionally);
    return future;
  }

  public void clearTenant(String tenantName) {
    execute(
        String.format(
            "DELETE from %s.%s WHERE tenant = ?",
            CassandraConstants.KEYSPACE_ADMIN, sketchTableName),
        tenantName);
  }

  ResultSet execute(String statement, Object... params) {
    assert !ExecutorManager.isInIoThread();
    return cassandraConnection.getSession().execute(statement, params);
  }

  ResultSet execute(Statement statement) {
    assert !ExecutorManager.isInIoThread();
    return cassandraConnection.getSession().execute(statement);
  }

  /** Used only for test. */
  @VisibleForTesting
  public void clearRecords(StreamDesc signalDesc) {
    execute(
        String.format(
            "DELETE from %s.%s WHERE tenant = ? AND signal = ? AND signal_version = ?",
            CassandraConstants.KEYSPACE_ADMIN, tableName),
        signalDesc.getParent().getName(),
        signalDesc.getSchemaName(),
        signalDesc.getSchemaVersion());
    execute(
        String.format(
            "DELETE from %s.%s WHERE tenant = ? AND stream_name_proxy = ?",
            CassandraConstants.KEYSPACE_ADMIN, sketchTableName),
        signalDesc.getParent().getName(),
        signalDesc.getStreamNameProxy());
  }

  @VisibleForTesting
  public void dropTables() {
    execute(String.format("DROP TABLE %s.%s", CassandraConstants.KEYSPACE_ADMIN, tableName));
    execute(String.format("DROP TABLE %s.%s", CassandraConstants.KEYSPACE_ADMIN, sketchTableName));
  }

  /**
   * FOR TEST ONLY: Set stop time for rollup.
   *
   * <p>If the value is larger than zero, the rollup scheduler ignores current time and uses this
   * value as "the current time" to determine rollup time window.
   *
   * <p>The rollup would stop at this time stamp. After that, you need to increment the value
   * manually to resume the rollup.
   *
   * @param millis Milliseconds since epoch time to stop rollup.
   */
  @VisibleForTesting
  public void setRollupStopTime(long millis) {
    rollupStopTime = millis;
  }
}
