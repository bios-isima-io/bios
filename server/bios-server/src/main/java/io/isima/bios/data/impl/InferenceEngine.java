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

import static io.isima.bios.data.impl.InferAttributeTags.ALREADY_SPECIFIED_PREFIX;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_INFERENCE_RECORDS;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.ibm.asyncutil.iteration.AsyncTrampoline;
import io.isima.bios.admin.Admin;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.maintenance.InferenceOverallState;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeTags;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Unit;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import io.isima.bios.utils.StringUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString
public class InferenceEngine {

  private static final Logger logger = LoggerFactory.getLogger(InferenceEngine.class);

  public static final String INFERENCE_INTERVAL_KEY = "prop.inferenceInterval";
  public static final Long INFERENCE_INTERVAL_DEFAULT = 1000L * 60 * 60 * 24; // 1 day

  // Only the first 5 columns are used for the record logic.
  // The remaining columns are written but not read, and are retained for diagnostic purposes.
  private static String makeCreateInferenceTableQuery() {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s"
            + " ("
            + "   tenant text,"
            + "   stream_name_proxy int,"
            + "   attribute_proxy smallint,"
            + "   stream_version bigint,"
            + "   last_done timestamp,"
            + "   category text,"
            + "   kind text,"
            + "   unit text,"
            + "   type text,"
            + "   distinctcount text,"
            + "   count text,"
            + "   used_as_key text,"
            + "   stream text,"
            + "   attribute text,"
            + "   PRIMARY KEY ((tenant), stream_name_proxy, attribute_proxy))"
            + " WITH CLUSTERING ORDER BY (stream_name_proxy ASC, attribute_proxy ASC)"
            + " AND comment = 'tags inference records table'",
        CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS);
  }

  private static String makeInsertInferenceQuery() {
    return String.format(
        "INSERT INTO %s.%s (tenant, stream_name_proxy, attribute_proxy, stream_version, last_done,"
            + " category, kind, unit, type, distinctcount, count, used_as_key,"
            + " stream, attribute)"
            + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS);
  }

  private static String makeFetchLastInferenceTimeQuery() {
    return String.format(
        "SELECT max(stream_version), max(last_done)"
            + " FROM %s.%s WHERE tenant = ? AND stream_name_proxy = ?",
        CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS);
  }

  private static String makeFetchLastInferenceCountQuery() {
    return String.format(
        "SELECT count(last_done) as attributes_done"
            + " FROM %s.%s WHERE tenant = ? AND stream_name_proxy = ? AND stream_version = ? AND "
            + "last_done = ? ALLOW FILTERING",
        CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS);
  }

  protected final DataEngine dataEngine;
  private final CassandraConnection cassandraConnection;
  private final Session session;
  private final SharedConfig sharedConfig;

  private PreparedStatement statementInsertInference;
  private PreparedStatement statementFetchLastInferenceTime;
  private PreparedStatement statementFetchLastInferenceCount;

  public InferenceEngine(
      DataEngine dataEngine, CassandraConnection cassandraConnection, SharedConfig sharedConfig) {
    if (cassandraConnection == null) {
      throw new NullPointerException("cassandraConnection must not be null.");
    }
    this.dataEngine = dataEngine;
    this.cassandraConnection = cassandraConnection;
    this.session = cassandraConnection.getSession();
    this.sharedConfig = sharedConfig;
    final String className = this.getClass().getSimpleName();

    try {
      final boolean hasTable =
          cassandraConnection.verifyTable(
              CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS);

      if (!hasTable) {
        final String createTableQuery = makeCreateInferenceTableQuery();
        logger.debug("Creating inference records table: {}", createTableQuery);
        session.execute(createTableQuery);
      }

      if (!cassandraConnection.verifyTable(
          CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS, 30)) {
        throw new ApplicationException(
            String.format(
                "Failed to create table %s.%s",
                CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS));
      }

      final RetryHandler retryHandler =
          new RetryHandler("Prepared statements for inference records table", logger, "");
      while (true) {
        try {
          statementInsertInference = session.prepare(makeInsertInferenceQuery());
          statementFetchLastInferenceTime = session.prepare(makeFetchLastInferenceTimeQuery());
          statementFetchLastInferenceCount = session.prepare(makeFetchLastInferenceCountQuery());
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

  public CompletableFuture<Void> inferAttributeTags(
      InferenceOverallState overallState, StreamDesc streamDesc, ExecutionState parentState) {
    final var state = new GenericExecutionState("inferAttributeTags", parentState);
    final String tenant = streamDesc.getParent().getName();
    final String stream = streamDesc.getName();
    logger.trace(
        "Thread {} will infer for tenant={}, stream={} ",
        Thread.currentThread().getName(),
        tenant,
        stream);

    final String lockTarget = "infer." + tenant + "." + stream;

    final var startTime = System.currentTimeMillis();
    return BiosModules.getWorkerLock()
        .lockAsync(lockTarget, state.getExecutor())
        .thenComposeAsync(
            (lock) -> {
              if (lock == null) {
                return CompletableFuture.completedFuture(null);
              }
              logger.trace("Start inference; tenant={}, stream={}", tenant, stream);

              return inferAttributeTagsCore(streamDesc, state)
                  .thenAcceptAsync(
                      (inferredMap) -> {
                        if (inferredMap.size() > 0) {
                          overallState.incrementProcessedStreams();
                          overallState.incrementProcessedAttributes(inferredMap.size());
                          final long elapsedTotal = System.currentTimeMillis() - startTime;
                          logger.info(
                              "Done inference; attributes={}, time={}, tenant={}, stream={}",
                              inferredMap.size(),
                              elapsedTotal,
                              tenant,
                              stream);
                        }
                      },
                      state.getExecutor())
                  .whenComplete((r, t) -> lock.releaseAsync(state.getExecutor()));
            },
            state.getExecutor())
        .toCompletableFuture();
  }

  /**
   * If it has been a long time since tags have been inferred for this stream, infer them.
   *
   * @return A map of attribute to inferredTags only for attributes where something has changed in
   *     the inferredTags. If nothing has changed for an attribute, that attribute will not be
   *     present in the map.
   */
  private CompletableFuture<Map<Short, AttributeTags>> inferAttributeTagsCore(
      StreamDesc streamDesc, ExecutionState state) {
    final var currentTime = System.currentTimeMillis();
    final var map = new HashMap<Short, AttributeTags>();
    final var tenant = streamDesc.getParent().getName();
    final var attributes = streamDesc.getAttributes();
    final var additionalAttributes = streamDesc.getAdditionalAttributes();
    assert !ExecutorManager.isInIoThread();

    final CompletableFuture<Boolean> firstStage =
        cassandraConnection
            .executeAsync(
                statementFetchLastInferenceTime.bind(tenant, streamDesc.getStreamNameProxy()),
                state)
            .thenComposeAsync(
                (results) -> {
                  if (!results.isExhausted()) {
                    final Row row = results.one();
                    final long versionDone = row.getLong(0);
                    final long lastDoneTime = row.getLong(1);
                    logger.trace(
                        "Inference check: tenant={}, stream={}, versionDone={}, lastDoneTime={}",
                        tenant,
                        streamDesc.getName(),
                        versionDone,
                        StringUtils.tsToIso8601(lastDoneTime));
                    final var statement =
                        statementFetchLastInferenceCount.bind(
                            tenant, streamDesc.getStreamNameProxy(), versionDone, lastDoneTime);
                    return cassandraConnection
                        .executeAsync(statement, state)
                        .thenApplyAsync(
                            (results2) -> {
                              if (!results2.isExhausted()) {
                                final Row row2 = results2.one();
                                final long attributesDone = row2.getLong(0);
                                int totalAttributes = attributes.size();
                                if (additionalAttributes != null) {
                                  totalAttributes += additionalAttributes.size();
                                }
                                // This may block the thread but it's OK. Blocking happens only once
                                // in a while.
                                final long inferenceInterval =
                                    SharedProperties.getCached(
                                        INFERENCE_INTERVAL_KEY, INFERENCE_INTERVAL_DEFAULT);
                                if ((attributesDone >= totalAttributes)
                                    && (versionDone == streamDesc.getVersion())
                                    && (lastDoneTime + inferenceInterval > currentTime)) {
                                  return true;
                                }
                              }
                              return false;
                            },
                            state.getExecutor());
                  }
                  return CompletableFuture.completedFuture(false);
                },
                state.getExecutor())
            .toCompletableFuture();

    final CompletableFuture<Void> secondStage =
        firstStage.thenComposeAsync(
            (allAttributesAlreadyInferred) -> {
              if (allAttributesAlreadyInferred) {
                return CompletableFuture.completedFuture(null);
              }

              // There is at least one attribute for which we have not inferred tags within the
              // desired
              // interval. Infer tags for all the attributes now.
              return AsyncTrampoline.asyncWhile(
                      (iterator) -> iterator.hasNext(),
                      (iterator) -> {
                        final var attributeDesc = iterator.next();
                        return inferSingleAttributeTags(
                                streamDesc, attributeDesc, currentTime, state)
                            .thenApplyAsync(
                                (inferredTags) -> {
                                  if (inferredTags != null) {
                                    map.put(
                                        streamDesc.getAttributeProxy(attributeDesc.getName()),
                                        inferredTags);
                                  }
                                  return iterator;
                                },
                                state.getExecutor());
                      },
                      attributes.iterator())
                  .thenComposeAsync(
                      (none) -> {
                        if (additionalAttributes == null) {
                          return CompletableFuture.completedFuture(null);
                        }
                        return AsyncTrampoline.asyncWhile(
                            (iterator) -> iterator.hasNext(),
                            (iterator) -> {
                              final var attributeDesc = iterator.next();
                              return inferSingleAttributeTags(
                                      streamDesc, attributeDesc, currentTime, state)
                                  .thenApplyAsync(
                                      (inferredTags) -> {
                                        if (inferredTags != null) {
                                          map.put(
                                              streamDesc.getAttributeProxy(attributeDesc.getName()),
                                              inferredTags);
                                        }
                                        return iterator;
                                      },
                                      state.getExecutor());
                            },
                            additionalAttributes.iterator());
                      },
                      state.getExecutor())
                  .thenComposeAsync(
                      (none) -> {
                        if (map.size() == 0) {
                          return CompletableFuture.completedFuture(null);
                        }
                        // If there are any newly inferred tags, inform admin component so that it
                        // can
                        // update itself and other servers.
                        final Admin admin = BiosModules.getAdmin();
                        // Too complicated to make this method asynchronous, we borrow a sideline
                        // thread.
                        return CompletableFuture.runAsync(
                            () ->
                                ExecutionHelper.run(
                                    () -> {
                                      admin.updateInferredTags(
                                          tenant, streamDesc.getName(), RequestPhase.INITIAL, map);
                                    }),
                            ExecutorManager.getSidelineExecutor());
                      },
                      state.getExecutor());
            },
            state.getExecutor());

    return secondStage.thenApply((none) -> map);
  }

  /**
   * If it has been a long time since tags have been inferred for this stream, infer them.
   *
   * @return A map of attribute to inferredTags only for attributes where something has changed in
   *     the inferredTags. If nothing has changed for an attribute, that attribute will not be
   *     present in the map.
   */
  private Map<Short, AttributeTags> inferAttributeTagsCoreOrig(StreamDesc streamDesc)
      throws Throwable {
    final var currentTime = System.currentTimeMillis();
    final var map = new HashMap<Short, AttributeTags>();
    final var tenant = streamDesc.getParent().getName();
    final var attributes = streamDesc.getAttributes();
    final var additionalAttributes = streamDesc.getAdditionalAttributes();
    boolean allAttributesAlreadyInferred = false;
    assert !ExecutorManager.isInIoThread();
    final ResultSet results =
        session.execute(
            statementFetchLastInferenceTime.bind(tenant, streamDesc.getStreamNameProxy()));
    if (!results.isExhausted()) {
      final Row row = results.one();
      final long versionDone = row.getLong(0);
      final long lastDoneTime = row.getLong(1);
      logger.trace(
          "Inference check: tenant={}, stream={}, versionDone={}, lastDoneTime={}",
          tenant,
          streamDesc.getName(),
          versionDone,
          StringUtils.tsToIso8601(lastDoneTime));
      assert !ExecutorManager.isInIoThread();
      final ResultSet results2 =
          session.execute(
              statementFetchLastInferenceCount.bind(
                  tenant, streamDesc.getStreamNameProxy(), versionDone, lastDoneTime));
      if (!results2.isExhausted()) {
        final Row row2 = results2.one();
        final long attributesDone = row2.getLong(0);
        int totalAttributes = attributes.size();
        if (additionalAttributes != null) {
          totalAttributes += additionalAttributes.size();
        }
        final long inferenceInterval =
            SharedProperties.getCached(INFERENCE_INTERVAL_KEY, INFERENCE_INTERVAL_DEFAULT);
        if ((attributesDone >= totalAttributes)
            && (versionDone == streamDesc.getVersion())
            && (lastDoneTime + inferenceInterval > currentTime)) {
          allAttributesAlreadyInferred = true;
        }
      }
    }
    if (allAttributesAlreadyInferred) {
      return map;
    }

    // There is at least one attribute for which we have not inferred tags within the desired
    // interval. Infer tags for all the attributes now.
    for (final var attributeDesc : attributes) {
      final var inferredTags =
          inferSingleAttributeTags(streamDesc, attributeDesc, currentTime, null).get();
      if (inferredTags != null) {
        map.put(streamDesc.getAttributeProxy(attributeDesc.getName()), inferredTags);
      }
    }
    if (additionalAttributes != null) {
      for (final var attributeDesc : additionalAttributes) {
        final var inferredTags =
            inferSingleAttributeTags(streamDesc, attributeDesc, currentTime, null).get();
        if (inferredTags != null) {
          map.put(streamDesc.getAttributeProxy(attributeDesc.getName()), inferredTags);
        }
      }
    }

    // If there are any newly inferred tags, inform admin component so that it can update itself
    // and other servers.
    if (map.size() > 0) {
      final Admin admin = BiosModules.getAdmin();
      admin.updateInferredTags(tenant, streamDesc.getName(), RequestPhase.INITIAL, map);
    }

    return map;
  }

  /**
   * Infers attribute tags for a single attribute. If the newly inferred tags are different from the
   * current inferred tags, returns the newly inferred tags; otherwise returns null.
   */
  private CompletableFuture<AttributeTags> inferSingleAttributeTags(
      StreamDesc streamDesc, AttributeDesc attributeDesc, long currentTime, ExecutionState state) {
    final var tenant = streamDesc.getParent().getName();
    final var stream = streamDesc.getName();
    final var attribute = attributeDesc.getName();

    final var inferAttributeTags =
        new InferAttributeTags(
            dataEngine, cassandraConnection, sharedConfig, streamDesc, attributeDesc);
    return inferAttributeTags
        .infer(state)
        .thenComposeAsync(
            (tagsToStore) -> {
              final String distinctCount;
              if (tagsToStore.distinctCount != null) {
                distinctCount = String.format("%f", tagsToStore.distinctCount);
              } else {
                distinctCount = "";
              }
              final String count;
              if (tagsToStore.count != null) {
                count = String.format("%d", tagsToStore.count);
              } else {
                count = "";
              }
              final String usedAsKey;
              if ((tagsToStore.usedAsKey != null) && tagsToStore.usedAsKey) {
                usedAsKey = "true";
              } else {
                usedAsKey = "";
              }

              // Write the inferred attributes to the table. We need to write even if there is no
              // change
              // to the inferred tags in order to update the timestamp of latest inference.
              final var statement =
                  statementInsertInference.bind(
                      tenant,
                      streamDesc.getStreamNameProxy(),
                      streamDesc.getAttributeProxy(attribute),
                      streamDesc.getVersion(),
                      currentTime,
                      tagsToStore.categoryToStore,
                      tagsToStore.kindToStore,
                      tagsToStore.unitToStore,
                      attributeDesc.getAttributeType().toString(),
                      distinctCount,
                      count,
                      usedAsKey,
                      stream,
                      attribute);
              return cassandraConnection
                  .executeAsync(statement, state)
                  .thenApplyAsync(
                      (none) -> {
                        // Compare the newly inferred tags with the current inferred tags.
                        final var newInferredTags =
                            buildInferredTags(
                                tagsToStore.categoryToStore,
                                tagsToStore.kindToStore,
                                tagsToStore.unitToStore,
                                attributeDesc.getTags());
                        if ((newInferredTags != null)
                            && !newInferredTags.equals(attributeDesc.getInferredTags())) {
                          return newInferredTags;
                        } else {
                          return null;
                        }
                      },
                      state.getExecutor());
            },
            state.getExecutor());
  }

  /**
   * Builds an AttributeTags object based on the provided inferred pieces (category, kind, unit) as
   * well as user-specified tags. If the user has already specified some specific piece, then we
   * don't add an inference for that piece.
   *
   * @return null if there is nothing to be inferred beyond what the user has specified.
   */
  private AttributeTags buildInferredTags(
      final String category, final String kind, final String unit, AttributeTags userTags) {
    if (userTags == null) {
      // Create an empty object for userTags to simplify the null checks below.
      userTags = new AttributeTags();
    }

    final var inferredTags = new AttributeTags();
    if ((userTags.getCategory() == null)
        && !category.equals("")
        && !category.contains(ALREADY_SPECIFIED_PREFIX)) {
      inferredTags.setCategory(AttributeCategory.forValue(category));
    }
    if ((userTags.getKind() == null)
        && !kind.equals("")
        && !kind.contains(ALREADY_SPECIFIED_PREFIX)) {
      inferredTags.setKind(AttributeKind.forValue(kind));
    }
    if ((userTags.getUnit() == null)
        && (userTags.getUnitDisplayName() == null)
        && (userTags.getKind() != AttributeKind.OTHER_KIND)
        && !unit.equals("")
        && !unit.contains(ALREADY_SPECIFIED_PREFIX)) {
      inferredTags.setUnit(Unit.forValue(unit));
    }

    // Infer tags that can be inferred statically based on tags already known.
    final var effectiveTags = AttributeTags.getEffectiveTags(null, userTags, inferredTags);
    final var effectiveCategory = effectiveTags.getCategory();
    final var effectiveKind = effectiveTags.getKind();
    final var effectiveUnit = effectiveTags.getUnit();
    assert (effectiveCategory != null);
    if (effectiveCategory == AttributeCategory.QUANTITY) {
      if ((userTags.getUnitDisplayName() == null)) {
        inferredTags.setUnitDisplayName(Unit.getDisplayName(effectiveUnit));
      }
      if ((userTags.getUnitDisplayPosition() == null)) {
        inferredTags.setUnitDisplayPosition(AttributeKind.getUnitDisplayPosition(effectiveKind));
      }
      if ((userTags.getPositiveIndicator() == null)) {
        inferredTags.setPositiveIndicator(AttributeKind.getPositiveIndicator(effectiveKind));
      }
    }
    if ((userTags.getFirstSummary() == null)) {
      inferredTags.setFirstSummary(Unit.getFirstSummary(effectiveCategory, effectiveUnit));
    }
    if ((userTags.getSecondSummary() == null)) {
      inferredTags.setSecondSummary(Unit.getSecondSummary(effectiveCategory, effectiveUnit));
    }

    final var emptyInferredTags = new AttributeTags();
    if (!inferredTags.equals(emptyInferredTags)) {
      return inferredTags;
    } else {
      return null;
    }
  }

  public void clearTenant(String tenantName) {
    // Delete all inference records for a tenant to ensure old records don't conflict with a new
    // tenant with the same name.
    logger.info("Clearing inference table for tenant={}", tenantName);
    session.execute(
        String.format(
            "DELETE from %s.%s WHERE tenant = ?",
            CassandraConstants.KEYSPACE_ADMIN, TABLE_INFERENCE_RECORDS),
        tenantName);
  }
}
