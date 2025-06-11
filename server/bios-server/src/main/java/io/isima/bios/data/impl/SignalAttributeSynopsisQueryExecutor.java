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

import static io.isima.bios.utils.Utils.offsetFloor;
import static io.isima.bios.utils.Utils.round;

import com.google.common.util.concurrent.AtomicDouble;
import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.models.AttributeSummary;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.PositiveIndicator;
import io.isima.bios.models.Unit;
import io.isima.bios.models.UnitDisplayPosition;
import io.isima.bios.models.v1.Aggregate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalAttributeSynopsisQueryExecutor extends ComplexQueryExecutor {
  static final Logger logger = LoggerFactory.getLogger(SignalAttributeSynopsisQueryExecutor.class);

  private final String attributeName;

  private static final int RESPONSE_METADATA = 0;
  private static final int RESPONSE_DETAILED_METRICS = 1;
  private static final int RESPONSE_CURRENT_METRICS = 2;
  private static final int RESPONSE_PREV_METRICS = 3;
  private static final int RESPONSE_SAMPLES = 4;
  private static final int RESPONSE_TIMESTAMP_LAG = 5;
  private static final int TOTAL_RESPONSES = 6;

  private static final String ATTRIBUTE_TYPE = "attributeType";
  private static final String UNIT_DISPLAY_NAME = "unitDisplayName";
  private static final String UNIT_DISPLAY_POSITION = "unitDisplayPosition";
  private static final String POSITIVE_INDICATOR = "positiveIndicator";
  private static final String FIRST_SUMMARY = "firstSummary";
  private static final String SECOND_SUMMARY = "secondSummary";
  private static final String FIRST_SUMMARY_INDEX = "firstSummaryIndex";
  private static final String SECOND_SUMMARY_INDEX = "secondSummaryIndex";
  private static final String TIMESTAMP_LAG = "timestampLag";
  private static final String TIMESTAMP_COUNT = "timestampCount";
  private static final String PREV_TIMESTAMP_LAG = "prevTimestampLag";
  private static final String PREV_TIMESTAMP_COUNT = "prevTimestampCount";

  public SignalAttributeSynopsisQueryExecutor(
      DataEngine dataEngine,
      ComplexQueryState state,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler,
      String attributeName) {
    super(dataEngine, state, acceptor, errorHandler, TOTAL_RESPONSES);
    this.attributeName = attributeName;
  }

  @Override
  public void executeAsyncCore() {

    state.addHistory("(attributeSynopsis{(first");
    final var tempQueryLoggerItems = QueryLogger.newTempItems(4);

    final List<MetricFunction> defaultFunctions =
        List.of(
            MetricFunction.SUM,
            MetricFunction.AVG,
            MetricFunction.MIN,
            MetricFunction.MAX,
            MetricFunction.STDDEV,
            MetricFunction.SKEWNESS,
            MetricFunction.KURTOSIS,
            MetricFunction.P1,
            MetricFunction.P25,
            MetricFunction.MEDIAN,
            MetricFunction.P75,
            MetricFunction.P99);

    // The list of metrics depends on the type of the attribute, and summary functions chosen
    // by the user or inference (if any).
    final var attribute = state.getStreamDesc().findAnyAttribute(attributeName);
    final var attributeType = attribute.getAttributeType();
    final var tags = attribute.getEffectiveTags();
    final var firstSummaryFunction =
        (tags.getFirstSummary() == null)
            ? null
            : tags.getFirstSummary().getMetricFunctionForSingleValue();
    final var secondSummaryFunction =
        (tags.getSecondSummary() == null)
            ? null
            : tags.getSecondSummary().getMetricFunctionForSingleValue();

    // Add metadata indicating this attribute's type and the functions being returned.
    final var metadataResponse = new HashMap<Long, List<Event>>();
    final List<Event> eventList = new ArrayList<>();
    metadataResponse.put(0L, eventList);
    responsesArray[RESPONSE_METADATA] = new ComplexQuerySingleResponse(metadataResponse, false);
    final var eventFactory = state.generateEventFactory();
    final var metadataEvent = eventFactory.create();
    metadataEvent.set(ATTRIBUTE_TYPE, attributeType.getBiosAttributeType().stringify());
    metadataEvent.set(UNIT_DISPLAY_NAME, ObjectUtils.defaultIfNull(tags.getUnitDisplayName(), ""));
    metadataEvent.set(
        UNIT_DISPLAY_POSITION,
        ObjectUtils.defaultIfNull(tags.getUnitDisplayPosition(), UnitDisplayPosition.SUFFIX)
            .stringify());
    metadataEvent.set(
        POSITIVE_INDICATOR,
        ObjectUtils.defaultIfNull(tags.getPositiveIndicator(), PositiveIndicator.HIGH).stringify());
    metadataEvent.set(
        FIRST_SUMMARY,
        ObjectUtils.defaultIfNull(tags.getFirstSummary(), AttributeSummary.NONE).name());
    metadataEvent.set(
        SECOND_SUMMARY,
        ObjectUtils.defaultIfNull(tags.getSecondSummary(), AttributeSummary.NONE).name());
    eventList.add(metadataEvent);
    incrementAndCheckCompletion();
    // we know it's not the end, just continue

    state.addHistory(")(prepareDetailedMetrics");
    // Add metrics for the original windows: w2.
    final var metricsRequest = createSummarizeForOriginalPeriod(w2);
    final var aggregates = new ArrayList<Aggregate>();
    aggregates.add(new Aggregate(MetricFunction.COUNT, null));
    final var metrics = new ArrayList<MetricFunction>();
    metrics.add(MetricFunction.DISTINCTCOUNT);
    if (attributeType.isAddable()) {
      metrics.addAll(defaultFunctions);
    }
    // Add the chosen summaries if they are not already present.
    if (firstSummaryFunction != null) {
      if (!defaultFunctions.contains(firstSummaryFunction)) {
        metrics.add(firstSummaryFunction);
      }
      metadataEvent.set(FIRST_SUMMARY_INDEX, (long) metrics.indexOf(firstSummaryFunction) + 1);
    } else {
      metadataEvent.set(FIRST_SUMMARY_INDEX, (long) -1);
    }
    if (secondSummaryFunction != null) {
      if (!defaultFunctions.contains(secondSummaryFunction)) {
        metrics.add(secondSummaryFunction);
      }
      metadataEvent.set(SECOND_SUMMARY_INDEX, (long) metrics.indexOf(secondSummaryFunction) + 1);
    } else {
      metadataEvent.set(SECOND_SUMMARY_INDEX, (long) -1);
    }
    final var aggregatesWithAttribute =
        metrics.stream()
            .map(metric -> new Aggregate(metric, attributeName))
            .collect(Collectors.toList());
    aggregates.addAll(aggregatesWithAttribute);
    metricsRequest.setAggregates(aggregates);
    state.addHistory(")");
    final var detailedMetricsFuture =
        runAsync(
            "detailedMetrics",
            RESPONSE_DETAILED_METRICS,
            true,
            metricsRequest,
            tempQueryLoggerItems.get(0),
            state);

    state.addHistory("(prepareCurrentMetrics");
    // Add metrics for the full original period: w1.
    final var currentMetricsRequest = createSummarizeForOriginalPeriod(w1);
    final var currentMetrics = new ArrayList<MetricFunction>();
    currentMetrics.add(MetricFunction.DISTINCTCOUNT);
    if ((firstSummaryFunction != null) && (firstSummaryFunction != MetricFunction.DISTINCTCOUNT)) {
      currentMetrics.add(firstSummaryFunction);
    }
    if ((secondSummaryFunction != null)
        && (secondSummaryFunction != MetricFunction.DISTINCTCOUNT)) {
      currentMetrics.add(secondSummaryFunction);
    }
    currentMetricsRequest.setAggregates(
        currentMetrics.stream()
            .map(metric -> new Aggregate(metric, attributeName))
            .collect(Collectors.toList()));
    state.addHistory(")");
    final var currentMetricsFuture =
        runAsync(
            "currentMetrics",
            RESPONSE_CURRENT_METRICS,
            false,
            currentMetricsRequest,
            tempQueryLoggerItems.get(1),
            state);

    // Add metrics for the full previous (cyclical) period.
    state.addHistory("(preparePrevMetrics");
    final var prevMetricsRequest = createSummarizeForPreviousPeriod(w1);
    final var prevMetrics = new ArrayList<MetricFunction>();
    prevMetrics.add(MetricFunction.DISTINCTCOUNT);
    if (firstSummaryFunction != null) {
      prevMetrics.add(firstSummaryFunction);
    }
    if (secondSummaryFunction != null) {
      prevMetrics.add(secondSummaryFunction);
    }
    prevMetricsRequest.setAggregates(
        prevMetrics.stream()
            .map(metric -> new Aggregate(metric, attributeName))
            .collect(Collectors.toList()));
    state.addHistory(")");
    final var prevMetricsFuture =
        runAsync(
            "prevMetrics",
            RESPONSE_PREV_METRICS,
            false,
            prevMetricsRequest,
            tempQueryLoggerItems.get(2),
            state);

    // Add samples and counts for the full original period.
    state.addHistory("(prepareSamples");
    final var samplesRequest = createSummarizeForOriginalPeriod(w1);
    samplesRequest.setAggregates(
        List.of(new Aggregate(MetricFunction.SAMPLECOUNTS, attributeName)));
    state.addHistory(")");
    final var samplesFuture =
        runAsync(
            "samples", RESPONSE_SAMPLES, true, samplesRequest, tempQueryLoggerItems.get(3), state);

    final CompletableFuture<Void> synopsisFuture;

    // Add timestamp lag response and populate it if relevant.
    state.addHistory("(AttributeSynopsis1{(prepare");
    final var startPostDbTime = Instant.now();
    final var timestampLagResponse = new LinkedHashMap<Long, List<Event>>();
    responsesArray[RESPONSE_TIMESTAMP_LAG] =
        new ComplexQuerySingleResponse(timestampLagResponse, false);
    if (((tags.getFirstSummary() == AttributeSummary.TIMESTAMP_LAG)
            || (tags.getSecondSummary() == AttributeSummary.TIMESTAMP_LAG))
        && ((tags.getUnit() == Unit.UNIX_SECOND) || (tags.getUnit() == Unit.UNIX_MILLISECOND))) {

      final long scale;
      if (tags.getUnit() == Unit.UNIX_SECOND) {
        scale = 1000;
      } else {
        assert (tags.getUnit() == Unit.UNIX_MILLISECOND);
        scale = 1;
      }
      final long originalStartTime = state.getInput().getStartTime();
      final var aggregates2 = new ArrayList<Aggregate>();
      final Aggregate aggregateCount = new Aggregate(MetricFunction.COUNT);
      final Aggregate aggregateAvg = new Aggregate(MetricFunction.AVG, attributeName);
      aggregates2.add(aggregateCount);
      aggregates2.add(aggregateAvg);
      final var timestampRequest = createSummarizeForOriginalPeriod(w3);
      timestampRequest.setAggregates(aggregates2);
      final var summarizeState =
          DataEngineImpl.createSummarizeState(
              "AttributeSynopsis1", timestampRequest, dataEngine, state.getStreamDesc(), state);
      state.addHistory(")(wait");
      synopsisFuture =
          dataEngine
              .summarize(summarizeState)
              .thenCompose(
                  (results) -> {
                    if (results == null || results.isEmpty()) {
                      state.addHistory(")(checkResults-empty)})");
                      return CompletableFuture.completedStage(null);
                    }
                    state.addHistory(")(checkResults-nonEmpty)})(AttributeSynopsis2{(prepare");
                    // For each time window in the response, calculate the lag from the window's
                    // midpoint.
                    // Keep a running count+sum at two levels: original window size (w1) and full
                    // period
                    // (w2).
                    final Map<Long, AtomicLong> w2Counts = new HashMap<>();
                    final Map<Long, AtomicDouble> w2WeightedSums = new HashMap<>();
                    long w1Count = 0;
                    double w1WeightedSum = 0;
                    long earliestW2Start = Long.MAX_VALUE;
                    for (final var entry : results.entrySet()) {
                      final Pair<Long, Double> w3CountAndWeightedLag =
                          getW3CountAndWeightedLag(scale, aggregateCount, aggregateAvg, entry);
                      if ((w3CountAndWeightedLag == null)
                          || (w3CountAndWeightedLag.getLeft() <= 0)) {
                        continue;
                      }

                      w1Count += w3CountAndWeightedLag.getLeft();
                      w1WeightedSum += w3CountAndWeightedLag.getRight();
                      final long w2Start = offsetFloor(entry.getKey(), w2, originalStartTime);
                      earliestW2Start = Math.min(earliestW2Start, w2Start);
                      final AtomicLong w2Count =
                          w2Counts.computeIfAbsent(w2Start, k -> new AtomicLong());
                      final AtomicDouble w2WeightedSum =
                          w2WeightedSums.computeIfAbsent(w2Start, k -> new AtomicDouble());
                      w2Count.getAndAdd(w3CountAndWeightedLag.getLeft());
                      w2WeightedSum.getAndAdd(w3CountAndWeightedLag.getRight());
                    }
                    // Add the overall lag to the metadata result to be returned.
                    final double w1Lag = getLag(w1Count, w1WeightedSum, w3);
                    metadataEvent.set(TIMESTAMP_LAG, w1Lag);
                    metadataEvent.set(TIMESTAMP_COUNT, w1Count);

                    // Return a sorted list of time windows populated with the lag.
                    final var eventFactory2 = state.generateEventFactory();
                    for (long w2Start = earliestW2Start;
                        w2Start < state.getInput().getEndTime();
                        w2Start += w2) {
                      final List<Event> eventList2 = new ArrayList<>();
                      timestampLagResponse.put(w2Start, eventList2);
                      final var lagEvent = eventFactory2.create();
                      final var currentCount =
                          w2Counts.computeIfAbsent(w2Start, k -> new AtomicLong());
                      final var currentWeightedSum =
                          w2WeightedSums.computeIfAbsent(w2Start, k -> new AtomicDouble());
                      final Double currentLag =
                          getLag(currentCount.get(), currentWeightedSum.get(), w3);
                      lagEvent.set(TIMESTAMP_LAG, currentLag);
                      lagEvent.set(TIMESTAMP_COUNT, currentCount.get());
                      eventList2.add(lagEvent);
                    }

                    // Get one timestampLag number for the previous period.
                    final var prevTimestampRequest = createSummarizeForPreviousPeriod(w3);
                    prevTimestampRequest.setAggregates(aggregates2);
                    final var prevSummarizeState =
                        DataEngineImpl.createSummarizeState(
                            "AttributeSynopsis2",
                            prevTimestampRequest,
                            dataEngine,
                            state.getStreamDesc(),
                            state);
                    state.addHistory(")(wait");
                    return dataEngine
                        .summarize(prevSummarizeState)
                        .thenAccept(
                            (prevResults) -> {
                              state.addHistory(")(checkResults");
                              if ((prevResults != null) && (prevResults.size() >= 1)) {
                                // For each time window in the response, calculate the lag from the
                                // window's
                                // midpoint.
                                long prevW1Count = 0;
                                double prevW1WeightedSum = 0;
                                for (final var entry : prevResults.entrySet()) {
                                  final Pair<Long, Double> w3CountAndWeightedLag =
                                      getW3CountAndWeightedLag(
                                          scale, aggregateCount, aggregateAvg, entry);
                                  if ((w3CountAndWeightedLag == null)
                                      || (w3CountAndWeightedLag.getLeft() <= 0)) {
                                    continue;
                                  }
                                  prevW1Count += w3CountAndWeightedLag.getLeft();
                                  prevW1WeightedSum += w3CountAndWeightedLag.getRight();
                                }
                                // Add the overall lag to the metadata result to be returned.
                                final double prevW1Lag = getLag(prevW1Count, prevW1WeightedSum, w3);
                                metadataEvent.set(PREV_TIMESTAMP_LAG, prevW1Lag);
                                metadataEvent.set(PREV_TIMESTAMP_COUNT, prevW1Count);
                              }
                              state.addHistory(")");
                            });
                  })
              .toCompletableFuture();
    } else {
      synopsisFuture = CompletableFuture.completedFuture(null);
    }

    CompletableFuture.allOf(
            detailedMetricsFuture,
            currentMetricsFuture,
            prevMetricsFuture,
            samplesFuture,
            synopsisFuture.thenRun(this::incrementAndCheckCompletion))
        .thenRun(
            () -> {
              var timeTakenMicros =
                  Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;

              // Update the query logging metrics.
              // TODO(Naoki): Will the caller miss these records?
              state
                  .getQueryLoggerItem()
                  .ifPresent((item) -> item.incorporateTempItems(tempQueryLoggerItems));
              state
                  .getQueryLoggerItem()
                  .ifPresent(
                      (item) ->
                          item.addPostDb(
                              timeTakenMicros,
                              timestampLagResponse.size(),
                              timestampLagResponse.size()));
            })
        .exceptionally(
            (t) -> {
              errorHandler.accept(t);
              return null;
            });
  }

  private Pair<Long, Double> getW3CountAndWeightedLag(
      final long scale,
      final Aggregate aggregateCount,
      final Aggregate aggregateAvg,
      final Map.Entry<Long, List<Event>> entry) {
    final var w3EventList = entry.getValue();
    if ((w3EventList == null) || (w3EventList.size() != 1)) {
      logger.warn(
          "Unexpected result to query for window={}, result={}", entry.getKey(), w3EventList);
      return null;
    }
    final var w3Event = w3EventList.get(0);
    final long w3Count = (Long) w3Event.get(aggregateCount.getOutputAttributeName());
    final double w3Avg =
        ((Number) w3Event.get(aggregateAvg.getOutputAttributeName())).doubleValue() * scale;
    final long w3Middle = entry.getKey() + (w3 / 2);
    final double w3Lag = w3Middle - w3Avg;
    final double w3WeightedLag = w3Lag * w3Count;

    return Pair.of(w3Count, w3WeightedLag);
  }

  /**
   * Calculates the timestamp lag from the count and weighted sum, with appropriate rounding to
   * avoid a false sense of precision.
   */
  private double getLag(final long count, final double weightedSum, final long precision) {
    if (count == 0) {
      return Double.NaN;
    }
    return round(weightedSum / count, precision);
  }
}
