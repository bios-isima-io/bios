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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.CompiledAggregate;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.sketch.DataSketch;
import io.isima.bios.data.impl.sketch.SingleAttributeSketches;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.data.impl.sketch.SketchSummaryColumn;
import io.isima.bios.data.impl.storage.AsyncQueryExecutor;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SketchesExtractor {
  static final Logger logger = LoggerFactory.getLogger(SketchesExtractor.class);

  private final Session session;
  private final SketchStore sketchStore;

  public SketchesExtractor(Session session, SketchStore sketchStore) {
    this.session = session;
    this.sketchStore = sketchStore;
  }

  /**
   * Organizes and holds the collection of queries necessary for a single summarize request. Here, a
   * query means the specific set of parameters needed to build a query. The first level is a map
   * with key durationType.
   *
   * <p>We can have different sketches with different duration types e.g. moments and quantiles
   * every 5 minutes and frequentItems every 1 day, and the query has an interval of 1 day. Summary
   * data for the moments and quantiles sketches will be in one set of rows (with duration type 5
   * minutes) and summary data for the frequentItems sketch will be in another set of rows (with
   * duration type 1 day).
   */
  public static class Queries extends HashMap<DataSketchDuration, QueriesForDuration> {}

  /**
   * The second level is a map with key AttributeProxy and value a set of columns. The specific set
   * of columns needed for any attribute depends on the aggregates requested.
   */
  static class QueriesForDuration extends HashMap<Short, QueryUnit> {}

  /**
   * This is not a third level of lookup; it is just the value stored in the second level map. It
   * includes the set of columns to retrieve from sketch summary rows, as well as the set of sketch
   * blobs to retrieve.
   */
  static class QueryUnit {
    final HashSet<SketchSummaryColumn> summaryColumns = new HashSet<>();
    final HashSet<DataSketchType> sketchBlobs = new HashSet<>();
  }

  public long summarize(
      SummarizeState state,
      AsyncQueryExecutor executor,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler,
      long startTime)
      throws TfosException, ApplicationException {

    state.addHistory("(via_sketch{(parseRequest");
    logger.debug("Using sketches for summarization");
    final SummarizeRequest request = state.getInput();
    assert request.getAggregates() != null && !request.getAggregates().isEmpty();

    // Not yet supported for sketches.
    assert request.getGroup() == null || request.getGroup().isEmpty();
    assert request.getFilter() == null || request.getFilter().isEmpty();
    assert request.getSort() == null;
    assert request.getLimit() == null;

    final StreamDesc streamDesc = state.getStreamDesc();
    final TenantId tenantId = streamDesc.getParent().getId();
    final int streamNameProxy = streamDesc.getStreamNameProxy();
    final long interval = request.getInterval();
    final Queries queries = new Queries();
    List<CompiledAggregate> compiledAggregates = new ArrayList<>();

    interpretRequest(
        streamDesc, request.getAggregates(), startTime, interval, compiledAggregates, queries);

    state.setQueries(queries);
    state.setCompiledAggregates(compiledAggregates);

    state.addHistory(")(setUpQuery");
    // results accumulates the DB query results.
    final var results = new QueryResults();
    final var totalCount = new AtomicInteger(0);
    final var canceled = new AtomicBoolean(false);
    final long endTime = request.getEndTime();

    // Build executable statements for each of the query units.
    long lastCoverage = -1;
    int index = 0;
    for (final var queriesForDuration : queries.entrySet()) {
      final var duration = queriesForDuration.getKey();
      final long durationMs = duration.getMilliseconds();
      assert (startTime % durationMs == 0);
      long firstRowTime = startTime + durationMs;
      long lastRowTime = Utils.ceiling(endTime, durationMs);
      lastCoverage = firstRowTime;
      for (final var queryUnit : queriesForDuration.getValue().entrySet()) {
        final short attributeProxy = queryUnit.getKey();

        // Get summary columns.
        final var columns = queryUnit.getValue().summaryColumns;
        if (!columns.isEmpty()) {
          // Loop through all timeindex values applicable to this range (firstRowTime, lastRowTime)
          long firstTimeIndex = sketchStore.getSummaryTimeIndex(firstRowTime);
          long lastTimeIndex = sketchStore.getSummaryTimeIndex(lastRowTime);
          for (long timeIndex = firstTimeIndex;
              timeIndex <= lastTimeIndex;
              timeIndex += sketchStore.getSummaryTimeIndexWidth()) {
            final BoundStatement boundStatement =
                sketchStore.makeSelectSummaryStatement(
                    tenantId,
                    columns,
                    timeIndex,
                    streamNameProxy,
                    duration,
                    attributeProxy,
                    firstRowTime,
                    lastRowTime);
            executor.addStage(
                SketchSummarizeStage.forSignal(
                    state,
                    acceptor,
                    errorHandler,
                    columns,
                    null,
                    boundStatement,
                    results,
                    totalCount,
                    canceled,
                    index++));
          }
        }

        // Get sketch blobs.
        final var blobSketchTypes = queryUnit.getValue().sketchBlobs;
        if (!blobSketchTypes.isEmpty()) {
          // Loop through all timeindex values applicable to this range (firstRowTime, lastRowTime)
          long firstTimeIndex = sketchStore.getBlobTimeIndex(firstRowTime);
          long lastTimeIndex = sketchStore.getBlobTimeIndex(lastRowTime);
          for (long timeIndex = firstTimeIndex;
              timeIndex <= lastTimeIndex;
              timeIndex += sketchStore.getBlobTimeIndexWidth()) {
            final BoundStatement boundStatement =
                sketchStore.makeSelectBlobStatement(
                    tenantId,
                    blobSketchTypes,
                    timeIndex,
                    streamNameProxy,
                    duration,
                    attributeProxy,
                    firstRowTime,
                    lastRowTime);
            executor.addStage(
                SketchSummarizeStage.forSignal(
                    state,
                    acceptor,
                    errorHandler,
                    null,
                    blobSketchTypes,
                    boundStatement,
                    results,
                    totalCount,
                    canceled,
                    index++));
          }
        }
      }
    }

    return lastCoverage;
  }

  public CompletionStage<List<Event>> summarizeContext(
      SelectContextRequest request,
      List<Aggregate> aggregates,
      long queryTime,
      ContextOpState state) {

    state.addHistory("(via_sketch{(parseRequest");
    logger.debug("Using sketches for summarization");
    assert aggregates != null && !aggregates.isEmpty();

    // Not yet supported for sketches.
    assert request.getGroupBy() == null || request.getGroupBy().isEmpty();
    assert request.getWhere() == null || request.getWhere().isEmpty();
    assert request.getOrderBy() == null;
    assert request.getLimit() == null;

    final StreamDesc streamDesc = state.getStreamDesc();
    final TenantId tenantId = streamDesc.getParent().getId();
    final int streamNameProxy = streamDesc.getStreamNameProxy();

    final Queries queries = new Queries();
    List<CompiledAggregate> compiledAggregates = new ArrayList<>();
    try {
      interpretRequest(streamDesc, aggregates, 0, 0, compiledAggregates, queries);
    } catch (InvalidRequestException e) {
      return CompletableFuture.failedStage(e);
    }

    state.addHistory(")(setUpQuery");

    final var future = new CompletableFuture<List<Event>>();
    BiosModules.getSharedProperties()
        .getPropertyCachedIntAsync(
            DataEngine.MAX_EXTRACT_CONCURRENCY,
            DataEngine.DEFAULT_MAX_EXTRACT_CONCURRENCY,
            state,
            (maxConcurrency) -> {
              // Set upt the query executor
              final var queryExecutor =
                  new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());

              final var totalCount = new AtomicInteger();
              final var canceled = new AtomicBoolean();

              int index = 0;
              final var results = new SketchesExtractor.SingleDurationResults();
              final var latestTimestamp = new AtomicLong(0);
              for (var queriesForDuration : queries.entrySet()) {
                for (final var queryUnit : queriesForDuration.getValue().entrySet()) {
                  final short attributeProxy = queryUnit.getKey();

                  // Get summary columns
                  final var columns = queryUnit.getValue().summaryColumns;
                  if (!columns.isEmpty()) {
                    final BoundStatement boundStatement =
                        sketchStore.makeSelectSummaryContextStatement(
                            tenantId, columns, streamNameProxy, attributeProxy, queryTime);
                    queryExecutor.addStage(
                        SketchSummarizeStage.forContext(
                            state,
                            request,
                            compiledAggregates,
                            columns,
                            null,
                            boundStatement,
                            totalCount,
                            canceled,
                            index++,
                            queryTime,
                            latestTimestamp,
                            results,
                            future));
                  }

                  // Get sketch blobs
                  final var blobSketchTypes = queryUnit.getValue().sketchBlobs;
                  if (!blobSketchTypes.isEmpty()) {
                    final BoundStatement boundStatement =
                        sketchStore.makeSelectBlobContextStatement(
                            tenantId, blobSketchTypes, streamNameProxy, attributeProxy, queryTime);
                    queryExecutor.addStage(
                        SketchSummarizeStage.forContext(
                            state,
                            request,
                            compiledAggregates,
                            null,
                            blobSketchTypes,
                            boundStatement,
                            totalCount,
                            canceled,
                            index++,
                            queryTime,
                            latestTimestamp,
                            results,
                            future));
                  }
                }
              }
              state.addHistory(")");

              // The preparation is done, execute the query
              queryExecutor.execute();
            },
            (throwable) -> {
              state.markError();
              future.completeExceptionally(throwable);
            });
    return future;
  }

  /**
   * Gathers the list of sketches and columns needed for each of the aggregates. Organize them into
   * query units - each query unit will need one query per timeindex. Also, compiles the list of
   * aggregates for efficient access later.
   */
  private void interpretRequest(
      StreamDesc streamDesc,
      List<Aggregate> aggregates,
      long startTime,
      long interval,
      List<CompiledAggregate> compiledAggregates,
      Queries queries)
      throws InvalidRequestException {
    for (final var aggregate : aggregates) {
      // Identify the sketch needed for this aggregate - combination of function and attribute.
      final var function = aggregate.getFunction();
      final var sketchType = function.getDataSketchType();
      final var attributeName = aggregate.getBy();
      final short attributeProxy;
      DataSketchDuration selectedDuration = null;
      // (Hacky) Special case for count() - pick any attribute that is available.
      if ((function == MetricFunction.COUNT) && (attributeName == null)) {
        final var selected = findSketchToUseForSimpleCount(streamDesc, interval, startTime);
        attributeProxy = selected.getLeft();
        selectedDuration = selected.getRight();
      } else {
        attributeProxy = streamDesc.getAttributeProxy(attributeName);
        final var sketchTypeMap = streamDesc.getSketches().get(attributeName);
        if (sketchTypeMap == null) {
          // There are no sketches for this attribute.
          throw new InvalidRequestException(
              "No feature or sketch found for the specified windowed query with attribute %s",
              attributeName);
        }
        final var sketchDurationMap = sketchTypeMap.get(sketchType);
        if (sketchDurationMap == null) {
          // There are no sketches for this attribute and sketch type combination.
          throw new InvalidRequestException(
              "No feature or sketch found for the specified windowed query with attribute %s,"
                  + " function %s, sketchType %s",
              attributeName, function.name().toLowerCase(), sketchType.name());
        }

        // Pick the sketch with highest duration that is aligned with (1) the requested interval,
        // and (2) the requested start time.
        StreamDesc.SketchStatus selectedSketch = null;
        for (final var sketch : sketchDurationMap.entrySet()) {
          final long sketchInterval = sketch.getKey().getMilliseconds();
          if ((interval % sketchInterval == 0) && (startTime % sketchInterval == 0)) {
            if ((selectedDuration == null)
                || (sketchInterval > selectedDuration.getMilliseconds())) {
              // TODO do not pick a recently added sketch with low coverage (BIOS-1490).
              selectedDuration = sketch.getKey();
              selectedSketch = sketch.getValue();
            }
          }
        }
        if (selectedDuration == null) {
          throw new InvalidRequestException(
              "No feature or sketch found for the specified windowed query with attribute %s,"
                  + " function %s, sketchType %s, aligned with interval %s and startTime %s",
              attributeName,
              function.name().toLowerCase(),
              sketchType.name(),
              StringUtils.shortReadableDuration(interval),
              StringUtils.tsToIso8601(startTime));
        }
        logger.debug(
            "Picked sketch: attributeProxy={}, sketchType={}, duration={} for interval={},"
                + " startTime={}, function={}, attribute={}; status={}",
            attributeProxy,
            sketchType,
            selectedDuration,
            StringUtils.shortReadableDuration(interval),
            StringUtils.tsToIso8601(startTime),
            function,
            attributeName,
            selectedSketch);
      }

      // We found the sketch we want to use.
      // Get the summary columns or sketch blobs needed and add them to the query unit for this
      // combination of duration and attribute.
      // If a query unit for this duration and attribute is not yet created, create one now.
      final var queriesForDuration =
          queries.computeIfAbsent(selectedDuration, k -> new QueriesForDuration());
      final var queryUnit =
          queriesForDuration.computeIfAbsent(attributeProxy, k -> new QueryUnit());
      final long numInputSketchesPerOutputWindow =
          (long) Math.ceil(interval * 1.0 / selectedDuration.getMilliseconds());
      if (!sketchType.hasBlob() || function.presentInSummaryRow()) {
        // If the sketch doesn't have a blob, we only need the summary table.
        // If the function is present in the summary row, we may not need the full sketch blob if
        // it turns out that there is only a single input sketch. Note that we may have only a
        // single input sketch even if numInputSketchesPerOutputWindow > 1 because there aren't
        // enough sketches available in the requested time window.
        final var columnsNeeded = DataSketch.columnsNeededForFunction(function);
        queryUnit.summaryColumns.addAll(columnsNeeded);
      }
      if (!function.presentInSummaryRow()
          || (sketchType.hasBlob() && numInputSketchesPerOutputWindow > 1)) {
        // If the function requires a blob (function is not in summary row), get the blob.
        // If sketch has a blob and we may need to merge multiple sketch entries, get the blob.
        queryUnit.sketchBlobs.add(sketchType);
      }

      // Save a compiled aggregate for easy lookup later.
      // Count() function may not have an attribute, so use LONG datatype by default.
      compiledAggregates.add(
          new CompiledAggregate(
              function,
              attributeProxy,
              (attributeName != null) ? streamDesc.findAnyAttribute(attributeName) : null,
              aggregate.getOutputAttributeName(),
              selectedDuration));
    }
  }

  private Pair<Short, DataSketchDuration> findSketchToUseForSimpleCount(
      StreamDesc streamDesc, long interval, long startTime) throws InvalidRequestException {
    DataSketchDuration selectedDuration = null;
    long selectedBaseVersion = Long.MAX_VALUE;
    StreamDesc.SketchStatus selectedSketch = null;
    Short selectedAttributeProxy = null;
    // Loop over attributes.
    for (final var sketchTypeMapEntry : streamDesc.getSketches().entrySet()) {
      final String attributeName = sketchTypeMapEntry.getKey();
      final long attributeBaseVersion = streamDesc.getAttributeBaseVersion(attributeName);
      final var sketchTypeMap = sketchTypeMapEntry.getValue();
      // Loop over sketch types.
      for (final var sketchDurationMapEntry : sketchTypeMap.entrySet()) {
        final var sketchType = sketchDurationMapEntry.getKey();
        // Make sure the sketch populates count column of summary row.
        if ((sketchType != DataSketchType.MOMENTS)
            && (sketchType != DataSketchType.QUANTILES)
            && (sketchType != DataSketchType.DISTINCT_COUNT)
            && (sketchType != DataSketchType.SAMPLE_COUNTS)) {
          continue;
        }
        final var sketchDurationMap = sketchDurationMapEntry.getValue();
        // Loop over durations.
        // Pick the sketch with highest duration that is aligned with (1) the requested interval,
        // and (2) the requested start time. If there are multiple such candidates, pick the one
        // with the oldest attribute, since it may have a longer time period of coverage.
        for (final var sketchEntry : sketchDurationMap.entrySet()) {
          final long sketchInterval = sketchEntry.getKey().getMilliseconds();
          if ((interval % sketchInterval == 0) && (startTime % sketchInterval == 0)) {
            if ((selectedDuration == null)
                || (sketchInterval > selectedDuration.getMilliseconds())
                || ((sketchInterval == selectedDuration.getMilliseconds())
                    && (attributeBaseVersion < selectedBaseVersion))) {
              // TODO do not pick a recently added sketch with low coverage (BIOS-1490).
              selectedDuration = sketchEntry.getKey();
              selectedSketch = sketchEntry.getValue();
              selectedBaseVersion = attributeBaseVersion;
              selectedAttributeProxy = streamDesc.getAttributeProxy(sketchTypeMapEntry.getKey());
            }
          }
        }
      }
    }
    if (selectedDuration == null) {
      throw new InvalidRequestException(
          "No feature or sketch found for the specified windowed query with count,"
              + " aligned with interval %s and startTime %s",
          StringUtils.shortReadableDuration(interval), StringUtils.tsToIso8601(startTime));
    }
    logger.debug(
        "Picked sketch for simple count: attributeProxy={}, duration={} for interval={},"
            + " startTime={}; status={}",
        selectedAttributeProxy,
        selectedDuration,
        StringUtils.shortReadableDuration(interval),
        StringUtils.tsToIso8601(startTime),
        selectedSketch);

    return Pair.of(selectedAttributeProxy, selectedDuration);
  }

  /**
   * Organizes and holds the collection of results from DB queries. This organization is optimized
   * for efficient summarization after results are received. The data structures here need to be
   * thread-safe because multiple queries can write to them in parallel from different threads.
   *
   * <p>The first level is a map based on output time windows - the key is window begin time.
   */
  static class QueryResults extends ConcurrentHashMap<Long, OutputWindow> {}

  /** The second level is a map based on duration of input sketches. */
  static class OutputWindow extends ConcurrentHashMap<DataSketchDuration, SingleDurationResults> {}

  /**
   * The third level is a map based on attribute - the key is attributeProxy. The value is a
   * collection of sketch summary rows and blobs for a single attribute.
   */
  static class SingleDurationResults extends ConcurrentHashMap<Short, SingleAttributeSketches> {}
}
