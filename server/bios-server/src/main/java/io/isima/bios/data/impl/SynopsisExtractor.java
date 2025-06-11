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

import static io.isima.bios.data.impl.sketch.SketchSampleCounts.ATTRIBUTE_NAME_SAMPLE;
import static io.isima.bios.data.impl.sketch.SketchSampleCounts.ATTRIBUTE_NAME_SAMPLE_COUNT;
import static io.isima.bios.data.impl.sketch.SketchSampleCounts.ATTRIBUTE_NAME_SAMPLE_LENGTH;
import static io.isima.bios.models.MetricFunction.AVG;
import static io.isima.bios.models.MetricFunction.COUNT;
import static io.isima.bios.models.MetricFunction.DISTINCTCOUNT;
import static io.isima.bios.models.MetricFunction.KURTOSIS;
import static io.isima.bios.models.MetricFunction.MAX;
import static io.isima.bios.models.MetricFunction.MEDIAN;
import static io.isima.bios.models.MetricFunction.MIN;
import static io.isima.bios.models.MetricFunction.P1;
import static io.isima.bios.models.MetricFunction.P25;
import static io.isima.bios.models.MetricFunction.P75;
import static io.isima.bios.models.MetricFunction.P99;
import static io.isima.bios.models.MetricFunction.SKEWNESS;
import static io.isima.bios.models.MetricFunction.STDDEV;
import static io.isima.bios.models.MetricFunction.SUM;
import static java.lang.Boolean.TRUE;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.dto.AllContextSynopses;
import io.isima.bios.dto.AttributeSynopsis;
import io.isima.bios.dto.ContextSynopsis;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SynopsisRequest;
import io.isima.bios.errors.exception.NoFeatureFoundException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AttributeOrigin;
import io.isima.bios.models.AttributeSummary;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.GenericMetric;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.PositiveIndicator;
import io.isima.bios.models.Sort;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.UnitDisplayPosition;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataEngine component used for extracting context synopsis.
 *
 * <p>TODO(Naoki): Migrate signal synopsis extraction to this class, too.
 */
@AllArgsConstructor
public class SynopsisExtractor {
  private static final Logger logger = LoggerFactory.getLogger(SynopsisExtractor.class);

  private static final long ONE_HOUR = 3600L * 1000;
  private static final long ONE_DAY = ONE_HOUR * 24;
  private static final long FIVE_MINUTES = 300000;

  private final DataEngineImpl dataEngine;
  private final SketchesExtractor sketchesExtractor;

  public CompletionStage<AllContextSynopses> getAllContextSynopses(
      SynopsisRequest request, ExecutionState state) {
    final var tenantName = state.getTenantName();
    if (tenantName == null) {
      throw new IllegalArgumentException("The state object must have tenantName");
    }
    final long currentTime =
        Objects.requireNonNullElse(request.getCurrentTime(), System.currentTimeMillis());
    final var admin = BiosModules.getAdminInternal();
    final List<String> contextNames;
    try {
      contextNames = admin.listStreams(tenantName, StreamType.CONTEXT);
    } catch (NoSuchTenantException e) {
      // should not happen but...
      return CompletableFuture.failedStage(e);
    }

    final var synopses = new ContextSynopsis[contextNames.size()];
    final var futures = new CompletableFuture<?>[contextNames.size() * 3];
    for (int i = 0; i < contextNames.size(); ++i) {
      final var synopsis = new ContextSynopsis();
      final var contextName = contextNames.get(i);
      synopsis.setContextName(contextName);
      final var contextDesc = admin.getStreamOrNull(tenantName, contextName);

      final var metrics = List.of(new GenericMetric(COUNT, null, null));
      futures[i * 3] =
          retrieveSketches(contextDesc, metrics, state)
              .thenAcceptAsync(
                  (events) -> {
                    if (events.isEmpty()) {
                      synopsis.setCount(0L);
                      return;
                    }
                    final var event = events.get(0);
                    final var count = (Long) event.get("count()");
                    synopsis.setCount(count);
                    synopsis.setLastUpdated(event.getIngestTimestamp().getTime());
                  },
                  state.getExecutor())
              .toCompletableFuture();
      if (contextDesc.getAuditEnabled() == TRUE) {
        final var ctxCassStream = (ContextCassStream) dataEngine.getCassStream(contextDesc);
        state.setStreamDesc(ctxCassStream.getAuditSignalDesc());
        final var auditCassStream = dataEngine.getCassStream(ctxCassStream.getAuditSignalDesc());
        final long fiveMinBoundary = System.currentTimeMillis() / FIVE_MINUTES * FIVE_MINUTES;
        futures[i * 3 + 1] =
            fetchTrendLine(ctxCassStream, auditCassStream, fiveMinBoundary, state)
                .thenAccept(
                    (trendLine) -> {
                      long sum = 0;
                      for (var count : trendLine) {
                        sum += count;
                      }
                      synopsis.setDailyChanges(sum);
                      synopsis.setTrendLineData(new ContextSynopsis.TrendLineData(trendLine));
                    });

        futures[i * 3 + 2] =
            fetchDailyOperations(ctxCassStream, auditCassStream, fiveMinBoundary, state)
                .thenAccept((dailyOperations) -> synopsis.setDailyOperations(dailyOperations));
      } else {
        futures[i * 3 + 1] = CompletableFuture.completedFuture(null);
        futures[i * 3 + 2] = CompletableFuture.completedFuture(null);
      }
      synopses[i] = synopsis;
    }
    return CompletableFuture.allOf(futures)
        .thenApplyAsync(
            (none) -> {
              final var result = new AllContextSynopses();
              result.setTenantName(state.getTenantName());
              result.setContexts(List.of(synopses));
              return result;
            });
  }

  private CompletionStage<List<Event>> retrieveSketches(
      StreamDesc contextDesc, List<GenericMetric> metrics, ExecutionState state) {
    final var aggregates =
        metrics.stream()
            .map((metric) -> new Aggregate(metric.getFunction(), metric.getOf(), metric.getAs()))
            .collect(Collectors.toList());
    final var selectRequest = new SelectContextRequest();
    selectRequest.setContext(contextDesc.getName());
    selectRequest.setMetrics(metrics);
    final var contextOpState = new ContextOpState("Synopsis " + contextDesc.getName(), state);
    contextOpState.setContextDesc(contextDesc);
    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(contextDesc, null, state)
        .thenComposeAsync(
            (spec) -> {
              final var queryTime = spec != null ? spec.getDoneUntil() : System.currentTimeMillis();
              return sketchesExtractor.summarizeContext(
                  selectRequest, aggregates, queryTime, contextOpState);
            },
            state.getExecutor());
  }

  /**
   * Fetches numbers of write operations over last 24 hours with one-hour time window by querying
   * for context audit signal.
   */
  private CompletableFuture<List<Long>> fetchTrendLine(
      ContextCassStream ctxCassStream,
      CassStream auditCassStream,
      long fiveMinBoundary,
      ExecutionState state) {
    final var summarizeState =
        new SummarizeState("dailyChanges", EventFactory.DEFAULT_FACTORY, state);
    summarizeState.setStreamDesc(ctxCassStream.getAuditSignalDesc());
    summarizeState.setStreamName(ctxCassStream.getAuditSignalName());
    final var summarizeRequest = new SummarizeRequest();
    summarizeRequest.setAggregates(List.of(new Count().as("dailyChanges")));
    summarizeRequest.setInterval(ONE_HOUR);
    summarizeRequest.setHorizon(ONE_HOUR);
    summarizeRequest.setSnappedStartTime(fiveMinBoundary - ONE_DAY);
    summarizeRequest.setStartTime(summarizeRequest.getSnappedStartTime());
    summarizeRequest.setEndTime(fiveMinBoundary);
    summarizeRequest.setOrigEndTime(fiveMinBoundary);
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);
    final var future = new CompletableFuture<List<Long>>();
    try {
      dataEngine
          .getSignalSummarizer()
          .summarize(
              summarizeState,
              auditCassStream,
              (result) -> {
                final var trendLine = new ArrayList<Long>();
                if (!result.isEmpty()) {
                  for (var entry : result.entrySet()) {
                    final var events = entry.getValue();
                    if (!events.isEmpty()) {
                      final var value = (Long) events.get(0).get("dailyChanges");
                      trendLine.add(value);
                    }
                  }
                }
                future.complete(trendLine);
              },
              (t) -> {
                if (t instanceof NoFeatureFoundException) {
                  logger.warn(
                      "Failed to fetch trend line for context synopsis;"
                          + " tenant={}, signal={}, start={}, end={}, interval={}",
                      auditCassStream.getStreamDesc().getParent().getName(),
                      summarizeState.getStreamName(),
                      summarizeRequest.getStartTime(),
                      summarizeRequest.getEndTime(),
                      summarizeRequest.getInterval());
                  future.complete(List.of());
                } else {
                  future.completeExceptionally(t);
                }
              });
    } catch (TfosException | ApplicationException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  /**
   * Fetches types of operations with count over last 24 hours by querying for the context audit.
   */
  private CompletableFuture<List<ContextSynopsis.DailyOperation>> fetchDailyOperations(
      ContextCassStream ctxCassStream,
      CassStream auditCassStream,
      long fiveMinBoundary,
      ExecutionState state) {
    final var summarizeState =
        new SummarizeState("operations", EventFactory.DEFAULT_FACTORY, state);
    summarizeState.setStreamDesc(ctxCassStream.getAuditSignalDesc());
    summarizeState.setStreamName(ctxCassStream.getAuditSignalName());
    final var summarizeRequest = new SummarizeRequest();
    summarizeRequest.setAggregates(List.of(new Count()));
    summarizeRequest.setInterval(ONE_DAY);
    summarizeRequest.setHorizon(ONE_DAY);
    summarizeRequest.setSnappedStartTime(fiveMinBoundary - ONE_DAY);
    summarizeRequest.setStartTime(summarizeRequest.getSnappedStartTime());
    summarizeRequest.setEndTime(fiveMinBoundary);
    summarizeRequest.setOrigEndTime(fiveMinBoundary);
    final var dimensions = new ArrayList<String>();
    dimensions.add("_operation");
    summarizeRequest.setGroup(dimensions);
    summarizeRequest.setSort(new Sort("count()", true));
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);
    final var future = new CompletableFuture<List<ContextSynopsis.DailyOperation>>();
    try {
      dataEngine
          .getSignalSummarizer()
          .summarize(
              summarizeState,
              auditCassStream,
              (result) -> {
                final var dailyOperations = new ArrayList<ContextSynopsis.DailyOperation>();
                if (!result.isEmpty()) {
                  result.forEach(
                      (timestamp, events) -> {
                        events.forEach(
                            (event) -> {
                              dailyOperations.add(
                                  new ContextSynopsis.DailyOperation(
                                      (String) event.get("_operation"),
                                      (Long) event.get("count()")));
                            });
                      });
                }
                future.complete(dailyOperations);
              },
              (t) -> {
                if (t instanceof NoFeatureFoundException) {
                  logger.warn(
                      "Failed to fetch daily operations for context synopsis;"
                          + " tenant={}, signal={}, start={}, end={}, snappedStart={}, origEnd={},"
                          + " interval={}",
                      auditCassStream.getStreamDesc().getParent().getName(),
                      summarizeState.getStreamName(),
                      summarizeRequest.getStartTime(),
                      summarizeRequest.getEndTime(),
                      summarizeRequest.getSnappedStartTime(),
                      summarizeRequest.getOrigEndTime(),
                      summarizeRequest.getInterval());
                  future.complete(List.of());
                } else {
                  future.completeExceptionally(t);
                }
              });
    } catch (TfosException | ApplicationException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  public CompletionStage<ContextSynopsis> getContextSynopsis(
      SynopsisRequest request, ExecutionState state) {
    try {
      return getContextSynopsisCore(request, state);
    } catch (NoSuchStreamException | NoSuchTenantException | ApplicationException e) {
      return CompletableFuture.failedStage(e);
    }
  }

  CompletionStage<ContextSynopsis> getContextSynopsisCore(
      SynopsisRequest request, ExecutionState state)
      throws NoSuchTenantException, NoSuchStreamException, ApplicationException {
    final var tenantName = state.getTenantName();
    if (tenantName == null) {
      throw new IllegalArgumentException("The state object must have tenantName");
    }
    final var contextName = state.getStreamName();
    if (contextName == null) {
      throw new IllegalArgumentException("The state object must have streamName");
    }
    final var admin = BiosModules.getAdminInternal();
    final StreamDesc contextDesc;
    contextDesc = admin.getStream(tenantName, contextName);
    final long currentTime =
        Objects.requireNonNullElse(request.getCurrentTime(), System.currentTimeMillis());
    final var numAttributes = contextDesc.getAttributes().size();
    final var numAdditionalAttributes =
        Objects.requireNonNullElse(contextDesc.getAdditionalAttributes(), List.of()).size();

    final var ctxToMetrics = new HashMap<String, List<GenericMetric>>();
    final List<MetricFunction> numericFunctions =
        List.of(SUM, AVG, MIN, MAX, STDDEV, SKEWNESS, KURTOSIS, P1, P25, MEDIAN, P75, P99);
    final var unsupported = Set.of(AttributeType.BOOLEAN, AttributeType.BLOB);
    final var allParams = new ArrayList<GetAttributeSynopsisParams>();
    final var ctxToParams = new HashMap<String, List<GetAttributeSynopsisParams>>();
    for (var attributeDesc : contextDesc.getAttributes()) {
      final var params =
          new GetAttributeSynopsisParams(
              contextDesc,
              attributeDesc,
              attributeDesc.getName(),
              unsupported,
              numericFunctions,
              ctxToMetrics);
      startGettingAttributeSynopsis(currentTime, AttributeOrigin.ORIGINAL, params, state);
      allParams.add(params);
      ctxToParams.computeIfAbsent(contextDesc.getName(), (k) -> new ArrayList<>()).add(params);
    }

    if (contextDesc.getContextEnrichments() != null) {
      for (var enrichment : contextDesc.getContextEnrichments()) {
        for (var enrichedAttribute : enrichment.getEnrichedAttributes()) {
          final var value = valueOrDefault(enrichedAttribute.getValue(), "<null>");
          final var elements = value.split("\\.");
          if (elements.length != 2) {
            throw new ApplicationException(
                String.format(
                    "Context config invalid regarding context enrichment, "
                        + "wrong enrichment value syntax; value=%s; tenant=%s, context=%s",
                    value, tenantName, contextName));
          }
          final var remoteContextName = elements[0];
          final var remoteAttributeName = elements[1];
          final var remoteContext = admin.getStream(tenantName, remoteContextName);
          final var attributeDesc = remoteContext.findAttribute(remoteAttributeName);
          final var params =
              new GetAttributeSynopsisParams(
                  remoteContext,
                  attributeDesc,
                  enrichedAttribute.getAs(),
                  unsupported,
                  numericFunctions,
                  ctxToMetrics);
          startGettingAttributeSynopsis(currentTime, AttributeOrigin.ENRICHED, params, state);
          allParams.add(params);
          ctxToParams.computeIfAbsent(remoteContextName, (k) -> new ArrayList<>()).add(params);
        }
      }
    }

    final var futures = new CompletableFuture[allParams.size() + ctxToMetrics.size()];
    int index;
    for (index = 0; index < allParams.size(); ++index) {
      futures[index] = allParams.get(index).getSamplesFuture();
    }

    for (var entry : ctxToMetrics.entrySet()) {
      final var currentContextName = entry.getKey();
      final var metrics = entry.getValue();
      final var paramsForContext = ctxToParams.get(currentContextName);
      futures[index++] = getAttributeSynopses(currentContextName, metrics, paramsForContext, state);
    }

    return CompletableFuture.allOf(futures)
        .thenApplyAsync(
            (none) -> {
              final var contextSynopsis = new ContextSynopsis();
              contextSynopsis.setContextName(contextName);
              contextSynopsis.setAttributes(
                  allParams.stream()
                      .map((params) -> params.getSynopsis())
                      .collect(Collectors.toList()));
              return contextSynopsis;
            },
            state.getExecutor());
  }

  @Getter
  private static class GetAttributeSynopsisParams {
    // shared
    private final Collection<AttributeType> unsupported;
    private final List<MetricFunction> numericFunctions;
    private final Map<String, List<GenericMetric>> streamToMetrics;

    // for attribute
    private final StreamDesc contextDesc;
    private final AttributeDesc attributeDesc;
    private final String outAttributeName;
    private final AttributeSynopsis synopsis = new AttributeSynopsis();
    @Setter private CompletableFuture<Void> samplesFuture;

    public GetAttributeSynopsisParams(
        StreamDesc contextDesc,
        AttributeDesc attributeDesc,
        String outAttributeName,
        Collection<AttributeType> unsupported,
        List<MetricFunction> numericFunctions,
        Map<String, List<GenericMetric>> streamToMetrics) {
      this.contextDesc = contextDesc;
      this.attributeDesc = attributeDesc;
      this.outAttributeName = outAttributeName;
      this.streamToMetrics = streamToMetrics;
      this.unsupported = unsupported;
      this.numericFunctions = numericFunctions;
    }

    public List<GenericMetric> getMetrics(String contextName) {
      return streamToMetrics.computeIfAbsent(contextName, (key) -> new ArrayList<>());
    }
  }

  private void startGettingAttributeSynopsis(
      long currentTime,
      AttributeOrigin origin,
      GetAttributeSynopsisParams params,
      ExecutionState state) {
    final var contextDesc = params.getContextDesc();
    final var attributeDesc = params.getAttributeDesc();
    final var sourceAttributeName = attributeDesc.getName();
    final var attributeType = attributeDesc.getAttributeType().getBiosAttributeType();
    final var synopsis = params.getSynopsis();
    synopsis.setAttributeName(params.getOutAttributeName());
    synopsis.setAttributeType(attributeType);
    synopsis.setAttributeOrigin(origin);
    final var tags = attributeDesc.getEffectiveTags();
    synopsis.setUnitDisplayName(valueOrDefault(tags.getUnitDisplayName(), ""));
    synopsis.setUnitDisplayPosition(
        valueOrDefault(tags.getUnitDisplayPosition(), UnitDisplayPosition.SUFFIX));
    synopsis.setPositiveIndicator(
        valueOrDefault(tags.getPositiveIndicator(), PositiveIndicator.HIGH));
    synopsis.setFirstSummary(
        valueOrDefault(
            attributeType == AttributeType.BOOLEAN ? AttributeSummary.NONE : tags.getFirstSummary(),
            AttributeSummary.NONE));
    synopsis.setSecondSummary(valueOrDefault(tags.getSecondSummary(), AttributeSummary.NONE));
    if (params.getUnsupported().contains(attributeType)) {
      params.setSamplesFuture(CompletableFuture.completedFuture(null));
      return;
    }
    final var contextName = contextDesc.getName();
    final var metrics = params.getMetrics(contextName);
    metrics.add(
        new GenericMetric(
            DISTINCTCOUNT, sourceAttributeName, DISTINCTCOUNT + "_" + sourceAttributeName));
    if (attributeDesc.getAttributeType().isAddable()) {
      params
          .getNumericFunctions()
          .forEach(
              (func) ->
                  metrics.add(
                      new GenericMetric(
                          func, sourceAttributeName, func + "_" + sourceAttributeName)));
    }
    final var firstSummaryFunction = synopsis.getFirstSummary().getMetricFunctionForSingleValue();
    if (firstSummaryFunction != null
        && !params.getNumericFunctions().contains(firstSummaryFunction)) {
      metrics.add(
          new GenericMetric(
              firstSummaryFunction,
              sourceAttributeName,
              firstSummaryFunction + "_" + sourceAttributeName));
    }
    final var secondSummaryFunction = synopsis.getSecondSummary().getMetricFunctionForSingleValue();
    if (secondSummaryFunction != null
        && !params.getNumericFunctions().contains(secondSummaryFunction)) {
      metrics.add(
          new GenericMetric(
              secondSummaryFunction,
              sourceAttributeName,
              secondSummaryFunction + "_" + sourceAttributeName));
    }
    final var samplesRequest = new SelectContextRequest();
    samplesRequest.setContext(contextName);
    samplesRequest.setMetrics(
        List.of(new GenericMetric(MetricFunction.SAMPLECOUNTS, sourceAttributeName, null)));
    final var samplesSelectState = new ContextOpState("samples " + contextName, state);
    samplesSelectState.setContextDesc(contextDesc);
    final var samplesMetrics =
        List.of(new GenericMetric(MetricFunction.SAMPLECOUNTS, sourceAttributeName, null));
    params.setSamplesFuture(
        retrieveSketches(contextDesc, samplesMetrics, state)
            .thenAccept(
                (events) -> {
                  final var samples = new ArrayList<>();
                  final var sampleCounts = new ArrayList<Long>();
                  final var sampleLengths = new ArrayList<Long>();
                  for (var entry : events) {
                    samples.add(entry.get(ATTRIBUTE_NAME_SAMPLE));
                    sampleCounts.add(((Number) entry.get(ATTRIBUTE_NAME_SAMPLE_COUNT)).longValue());
                    final var length = entry.get(ATTRIBUTE_NAME_SAMPLE_LENGTH);
                    if (length != null) {
                      sampleLengths.add(((Number) length).longValue());
                    }
                  }
                  synopsis.setSamples(samples);
                  synopsis.setSampleCounts(sampleCounts);
                  if (sampleLengths.size() == samples.size()) {
                    synopsis.setSampleLengths(sampleLengths);
                  }
                })
            .toCompletableFuture());
  }

  private CompletableFuture<Void> getAttributeSynopses(
      String contextName,
      List<GenericMetric> metrics,
      List<GetAttributeSynopsisParams> paramsForContext,
      ExecutionState state)
      throws NoSuchTenantException, NoSuchStreamException {
    final var selectRequest = new SelectContextRequest();
    selectRequest.setContext(contextName);
    metrics.add(new GenericMetric(COUNT, null, COUNT + "_"));
    selectRequest.setMetrics(metrics);
    final var contextOpState = new ContextOpState("Synopsis " + contextName, state);
    final var contextDesc =
        BiosModules.getAdminInternal().getStream(state.getTenantName(), contextName);
    contextOpState.setContextDesc(contextDesc);
    return retrieveSketches(contextDesc, metrics, state)
        .thenAcceptAsync(
            (events) -> {
              final Event record = events.isEmpty() ? new EventJson() : events.get(0);
              for (var params : paramsForContext) {
                final var synopsis = params.getSynopsis();
                final var attributeDesc = params.getAttributeDesc();
                final var sourceAttributeName = attributeDesc.getName();
                final var attributeType = attributeDesc.getAttributeType();
                synopsis.setCount(valueOrDefault(fetchSketchValue(record, COUNT, ""), List.of(0L)));
                if (record.getIngestTimestamp() != null) {
                  synopsis.setLastUpdated(record.getIngestTimestamp().getTime());
                }
                if (params.getUnsupported().contains(attributeType.getBiosAttributeType())) {
                  continue;
                }
                synopsis.setDistinctCount(
                    valueOrDefault(
                        fetchSketchValue(record, DISTINCTCOUNT, sourceAttributeName), List.of(0L)));
                if (attributeType.isAddable()) {
                  synopsis.setSum(fetchSketchValue(record, SUM, sourceAttributeName));
                  synopsis.setAvg(fetchSketchValue(record, AVG, sourceAttributeName));
                  synopsis.setMin(fetchSketchValue(record, MIN, sourceAttributeName));
                  synopsis.setMax(fetchSketchValue(record, MAX, sourceAttributeName));
                  synopsis.setStddev(fetchSketchValue(record, STDDEV, sourceAttributeName));
                  synopsis.setSkewness(fetchSketchValue(record, SKEWNESS, sourceAttributeName));
                  synopsis.setKurtosis(fetchSketchValue(record, KURTOSIS, sourceAttributeName));
                  synopsis.setP1(fetchSketchValue(record, P1, sourceAttributeName));
                  synopsis.setP25(fetchSketchValue(record, P25, sourceAttributeName));
                  synopsis.setMedian(fetchSketchValue(record, MEDIAN, sourceAttributeName));
                  synopsis.setP75(fetchSketchValue(record, P75, sourceAttributeName));
                  synopsis.setP99(fetchSketchValue(record, P99, sourceAttributeName));
                }
                // Data trend is not supported for context yet, summary is just the latest value
                synopsis.setDistinctCountCurrent(synopsis.getDistinctCount().get(0));
                final var firstSummaryFunction =
                    synopsis.getFirstSummary().getMetricFunctionForSingleValue();
                if (firstSummaryFunction != null) {
                  synopsis.setFirstSummaryCurrent(
                      record.get(firstSummaryFunction + "_" + sourceAttributeName));
                }
                final var secondSummaryFunction =
                    synopsis.getSecondSummary().getMetricFunctionForSingleValue();
                if (secondSummaryFunction != null) {
                  synopsis.setSecondSummaryCurrent(
                      record.get(secondSummaryFunction + "_" + sourceAttributeName));
                }
              }
            },
            state.getExecutor())
        .toCompletableFuture();
  }

  private static <T> T valueOrDefault(T value, T defaultValue) {
    return value != null ? value : defaultValue;
  }

  private <T> List<T> fetchSketchValue(Event record, MetricFunction function, String name) {
    final var value = record.get(function + "_" + name);
    return value != null ? List.of((T) value) : null;
  }
}
