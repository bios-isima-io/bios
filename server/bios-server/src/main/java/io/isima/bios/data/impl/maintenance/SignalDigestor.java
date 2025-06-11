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

import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_COUNT_ATTRIBUTE;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MAX_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MIN_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_SUM_SUFFIX;
import static io.isima.bios.common.TfosConfig.biosDomainName;
import static java.lang.Boolean.TRUE;

import com.datastax.driver.core.utils.UUIDs;
import com.ibm.asyncutil.iteration.AsyncTrampoline;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.anomalydetector.factories.AnomalyEventProcessorFactory;
import io.isima.bios.anomalydetector.processor.AnomalyEventProcessor;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.StreamId;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.data.impl.storage.StorageDataUtils;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.AtomicOperationContext;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.Count;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.Sum;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.evaluator.ExpressionEvaluator;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalDigestor {
  private static final Logger logger = LoggerFactory.getLogger(SignalDigestor.class);

  private static final AnomalyEventProcessor anomalyEventProcessor =
      AnomalyEventProcessorFactory.getAnomalyDetector();

  private static final WebhookNotifier webhookNotifier =
      new WebhookNotifier(MonitoringSystemConfigs.getMonitoringNotificationUrl());

  private final DataEngineMaintenance dataEngineMaintenance;
  private final DataEngine dataEngine;
  private final SharedProperties sharedProperties;
  private final PostProcessScheduler postProcessScheduler;

  private final SignalIndexer signalIndexer;
  private final SignalSketchesCalculator signalSketchesCalculator;
  private final LastNCollector lastNCollector;

  private final Boolean isAlertNotificationEnabled =
      MonitoringSystemConfigs.getAlertNotificationsEnabled();
  private static final Integer notificationRetryCount =
      MonitoringSystemConfigs.getMonitoringNotificationRetryCount();

  private final Map<StreamId, AccumulatingCounter> counters = new ConcurrentHashMap<>();

  public SignalDigestor(
      DataEngineMaintenance dataEngineMaintenance,
      DataEngine dataEngine,
      SharedConfig sharedConfig,
      SharedProperties sharedProperties,
      PostProcessScheduler postProcessScheduler,
      SketchStore sketchStore) {
    this.dataEngineMaintenance = dataEngineMaintenance;
    this.dataEngine = dataEngine;
    this.sharedProperties = sharedProperties;
    this.postProcessScheduler = postProcessScheduler;

    this.lastNCollector = new LastNCollector(dataEngine, dataEngineMaintenance);
    this.signalIndexer =
        new SignalIndexer(dataEngineMaintenance, dataEngine, postProcessScheduler, sharedConfig);
    this.signalSketchesCalculator =
        new SignalSketchesCalculator(dataEngineMaintenance, postProcessScheduler, sketchStore);
  }

  public CompletableFuture<Boolean> execute(PostProcessSpecifiers specs, DigestState state) {
    if (specs == null || (specs.isEmpty() && !specs.isToDoInfer())) {
      return CompletableFuture.completedFuture(true);
    }
    final var signalDesc = specs.getStreamDesc();
    final String tenant = signalDesc.getParent().getName();
    final String signal = signalDesc.getName();

    if (!specs.getIndexes().isEmpty()) {
      logger.debug(
          "Start post-process; tenant={}, signal={}, timeRange={} : {}",
          tenant,
          signal,
          state.getStartTime(),
          state.getEndTime());
      logger.debug("index={}", specs.getIndexes());
      logger.debug("rollup={}", specs.getRollups());
      logger.debug("dataSketches={}", specs.getSketches());
    }
    state.setScheduleEnd(System.currentTimeMillis());
    state.setStarted(true);
    state.addHistory("(fetchEvents");
    return digest(signalDesc, specs, state);
  }

  private CompletableFuture<Boolean> digest(
      StreamDesc signalDesc, PostProcessSpecifiers specs, DigestState state) {
    final var tenantName = signalDesc.getParent().getName();
    final var signalName = signalDesc.getName();

    final var targetSignal =
        BiosModules.getAdminInternal()
            .getStreamOrNull(tenantName, signalName, signalDesc.getVersion());
    if (targetSignal == null) {
      logger.warn(
          "The signal is stale, canceling to calculate signal digestion;"
              + " tenant={}, signal={}, version={}, schemaVersion={}",
          tenantName,
          signalName,
          signalDesc.getVersion(),
          signalDesc.getSchemaVersion());
      return CompletableFuture.completedFuture(false);
    }

    if (specs.isEmpty()) {
      // no digestion is specified
      return CompletableFuture.completedFuture(true);
    }
    return DataMaintenanceUtils.extractEventsAsync(
            signalDesc,
            specs.getTimeRange().getBegin(),
            specs.getTimeRange().getEnd(),
            null,
            null,
            null,
            "signalPostProcess",
            state)
        .thenComposeAsync(
            (events) -> {
              state.setFetchEventsEnd(System.currentTimeMillis());
              return signalIndexer
                  .index(signalDesc, specs.getIndexes(), state, events, state)
                  .thenComposeAsync(
                      (none) -> {
                        // inform any interested publishers. Publisher will check if it is
                        // interested
                        // in the data or not.
                        state.addHistory(")(publishData");
                        final List<Event> eventsAtThisPoint = Collections.unmodifiableList(events);
                        dataEngineMaintenance
                            .getDataPublishers()
                            .forEach((p) -> p.publishRawData(signalDesc, eventsAtThisPoint));
                        state.addHistory(")(digestEvents");
                        return calculateFeatures(events, signalDesc, specs.getRollups(), state);
                      },
                      state.getExecutor())
                  .thenComposeAsync(
                      (none) -> {
                        state.setRollupEnd(System.currentTimeMillis());
                        return signalSketchesCalculator.execute(
                            events, signalDesc, specs.getSketches(), specs.getIndexes(), state);
                      },
                      state.getExecutor())
                  .whenComplete((r, t) -> state.setSketchEnd(System.currentTimeMillis()));
            },
            state.getExecutor());
  }

  private CompletableFuture<Void> calculateFeatures(
      List<Event> extractedEvents,
      StreamDesc signalDesc,
      List<DigestSpecifier> rollupSpecifiers,
      DigestState state) {

    if (rollupSpecifiers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    state.addHistory("(digest");
    return DataMaintenanceUtils.isRollupDebugLogEnabled(signalDesc, state)
        .thenComposeAsync(
            (debugLogEnabled) ->
                ExecutionHelper.supply(
                    () -> {
                      if (debugLogEnabled) {
                        logger.info("Digesting events for signal {}", signalDesc.getName());
                        rollupSpecifiers.forEach(
                            (spec) ->
                                logger.info(
                                    "  rollup={}, range=[{} : {}]",
                                    spec.getName(),
                                    StringUtils.tsToIso8601(spec.getStartTime()),
                                    StringUtils.tsToIso8601(spec.getEndTime())));
                        extractedEvents.forEach(
                            (event) ->
                                logger.info(
                                    "     {}: {}",
                                    StringUtils.tsToIso8601Millis(
                                        event.getIngestTimestamp().getTime()),
                                    event));
                      }

                      final SignalCassStream cassStream =
                          (SignalCassStream) dataEngine.getCassStream(signalDesc);
                      final var atomicOperationContext =
                          new AtomicOperationContext(UUID.randomUUID(), 0);

                      return AsyncTrampoline.asyncWhile(
                              (i) -> i < rollupSpecifiers.size(),
                              (i) -> {
                                final var specifier = rollupSpecifiers.get(i);
                                return handleRollupSpecifier(
                                        specifier,
                                        signalDesc,
                                        cassStream,
                                        extractedEvents,
                                        atomicOperationContext,
                                        state,
                                        debugLogEnabled)
                                    .thenApply((none) -> i + 1);
                              },
                              0)
                          .thenComposeAsync(
                              (none) -> dataEngine.commit(atomicOperationContext, state),
                              state.getExecutor());
                    }),
            state.getExecutor())
        .thenAccept(
            (none) -> {
              state.addHistory(")");
            });
  }

  private CompletableFuture<Void> handleRollupSpecifier(
      DigestSpecifier specifier,
      StreamDesc signalDesc,
      SignalCassStream cassStream,
      List<Event> extractedEvents,
      AtomicOperationContext atomicOperationContext,
      DigestState state,
      boolean debugLogEnabled) {
    final StreamDesc rollupStream =
        BiosModules.getAdminInternal()
            .getStreamOrNull(
                signalDesc.getParent().getName(), specifier.getName(), signalDesc.getVersion());
    if (rollupStream == null) {
      // already removed, skipping
      return CompletableFuture.completedFuture(null);
    }
    final Rollup rollup = signalDesc.findRollup(specifier.getName());
    final ViewDesc viewDesc = rollupStream.getViews().get(0);
    if (viewDesc == null) {
      // This happens when the digest has started while the AdminInternal is still creating the
      // target view, thus the view table is not ready. We should skip digesting such a view.
      logger.error(
          "Failed to fetch view; tenant={}, signal={}, rollup={}",
          signalDesc.getParent().getName(),
          signalDesc.getName(),
          rollup);
      return CompletableFuture.completedFuture(null);
    }

    final CompletableFuture<List<Event>> postProcessFuture;
    if ((viewDesc.getDataSketches() != null
            && viewDesc.getDataSketches().contains(DataSketchType.LAST_N))
        || viewDesc.getLastN() == TRUE) {
      // The rollup view may have its attributes trimmed due to the data type.
      // Last N should use the original view
      final var originalView = signalDesc.getView(viewDesc.getName());
      postProcessFuture =
          lastNCollector.update(extractedEvents, signalDesc, specifier, originalView, state);
    } else if (viewDesc.getSnapshot() == TRUE) {
      postProcessFuture =
          accumulatingCount(
                  extractedEvents,
                  specifier,
                  signalDesc,
                  rollupStream,
                  atomicOperationContext,
                  state)
              .thenApply((none) -> List.of());
    } else {
      state.addHistory("(aggregateEvents");
      postProcessFuture =
          aggregateEvents(
                  extractedEvents,
                  signalDesc,
                  specifier,
                  rollupStream,
                  rollup,
                  viewDesc,
                  cassStream,
                  state,
                  debugLogEnabled)
              .whenComplete(
                  (r, t) -> {
                    if (t == null) {
                      state.addHistory(")");
                    }
                  });
    }

    return postProcessFuture.thenComposeAsync(
        (processedEvents) -> {
          state.getProcessedSpecifiers().add(specifier);
          return postProcessScheduler
              .recordCompletion(signalDesc, specifier, state)
              .thenRunAsync(
                  () -> {
                    if ((logger.isDebugEnabled())
                        || (processedEvents.size() > 0
                            && sharedProperties.getPropertyCachedBoolean(
                                    DataEngineMaintenance.KEY_ROLLUP_DETAIL_LOG_ENABLED)
                                == TRUE)) {
                      final String log =
                          String.format(
                              "Summarized; items=%d,"
                                  + " range=[%s + %s], coverage=[%s - %s] %d%%, tenant=%s, rollup=%s.v%s",
                              processedEvents.size(),
                              StringUtils.tsToIso8601(specifier.getStartTime()),
                              StringUtils.shortReadableDuration(
                                  specifier.getEndTime() - specifier.getStartTime()),
                              StringUtils.tsToIso8601(specifier.getDoneUntil()),
                              StringUtils.shortReadableDuration(
                                  specifier.getDoneUntil() - specifier.getDoneSince()),
                              (int) (specifier.getDoneCoverage()),
                              signalDesc.getParent().getName(),
                              specifier.getName(),
                              StringUtils.tsToIso8601(specifier.getVersion()));
                      if (processedEvents.size() > 0) {
                        logger.info(log);
                      } else {
                        logger.debug(log);
                      }
                    }
                  },
                  state.getExecutor());
        },
        state.getExecutor());
  }

  private CompletableFuture<Void> accumulatingCount(
      List<Event> events,
      DigestSpecifier spec,
      StreamDesc signalDesc,
      StreamDesc rollupDesc,
      AtomicOperationContext atomicOperationContext,
      ExecutionState parentState) {
    final var tenantName = signalDesc.getParent().getName();
    final var featureName = spec.getName();
    final var featureId = new StreamId(tenantName, featureName, spec.getVersion());
    final var counter =
        counters.computeIfAbsent(
            featureId, (k) -> new AccumulatingCounter(dataEngine, signalDesc, rollupDesc));

    final var state = new GenericExecutionState("accumulatingCount", parentState);
    state.setAtomicOperationContext(atomicOperationContext);
    atomicOperationContext.getRemainingOperations().incrementAndGet();
    return counter.count(spec, events, state);
  }

  private CompletableFuture<List<Event>> aggregateEvents(
      List<Event> extractedEvents,
      StreamDesc signalDesc,
      DigestSpecifier specifier,
      StreamDesc rollupStream,
      Rollup rollup,
      ViewDesc viewDesc,
      SignalCassStream cassStream,
      DigestState digestState,
      boolean debugLogEnabled) {
    final Group group = new Group(viewDesc.getGroupBy());
    final List<String> attributes = Collections.emptyList();
    final List<Aggregate> aggregates = new ArrayList<>();

    /*
     * output property names -- use internal names to avoid conflict with signal attributes.
     */
    final List<String> internalOutputNames = new ArrayList<>();
    /*
     * rollup attribute names corresponding to the output property names.
     */
    final List<String> rollupAttributes = new ArrayList<>();
    aggregates.add(new Count());
    internalOutputNames.add("count()");
    rollupAttributes.add(ROLLUP_STREAM_COUNT_ATTRIBUTE);

    for (String attribute : viewDesc.getAttributes()) {
      aggregates.add(new Sum(attribute));
      internalOutputNames.add("sum(" + attribute + ")");
      rollupAttributes.add(attribute + ROLLUP_STREAM_SUM_SUFFIX);

      aggregates.add(new Min(attribute));
      internalOutputNames.add("min(" + attribute + ")");
      rollupAttributes.add(attribute + ROLLUP_STREAM_MIN_SUFFIX);

      aggregates.add(new Max(attribute));
      internalOutputNames.add("max(" + attribute + ")");
      rollupAttributes.add(attribute + ROLLUP_STREAM_MAX_SUFFIX);
    }

    final List<Event> allEvents = new ArrayList<>();

    final var interval = specifier.getInterval();
    digestState.addHistory("(aggregateEventsCore");

    return AsyncTrampoline.asyncWhile(
            (startTime) -> startTime < specifier.getEndTime(),
            (startTime) -> {
              digestState.addHistory(String.format("(aggregateEventsSegment[%d]", startTime));
              final long endTime = startTime + interval;
              return aggregateEventsCore(
                      extractedEvents,
                      signalDesc,
                      rollupStream,
                      rollup,
                      viewDesc,
                      cassStream,
                      group,
                      attributes,
                      aggregates,
                      internalOutputNames,
                      rollupAttributes,
                      startTime,
                      endTime,
                      debugLogEnabled,
                      digestState)
                  .thenApplyAsync(
                      (ev) -> {
                        allEvents.addAll(ev);
                        digestState.addHistory(")");
                        return endTime;
                      },
                      digestState.getExecutor());
            },
            specifier.getStartTime())
        .thenApplyAsync(
            (none) -> {
              digestState.addHistory(")");
              return allEvents;
            },
            digestState.getExecutor())
        .toCompletableFuture();
  }

  private CompletableFuture<List<Event>> aggregateEventsCore(
      List<Event> extractedEvents,
      StreamDesc signalDesc,
      StreamDesc rollupStream,
      Rollup rollup,
      ViewDesc viewDesc,
      SignalCassStream cassStream,
      Group group,
      List<String> attributes,
      List<Aggregate> aggregates,
      List<String> internalOutputNames,
      List<String> rollupAttributes,
      long startTime,
      long endTime,
      boolean debugLogEnabled,
      ExecutionState maintenanceState) {

    final long viewExtractStart = System.nanoTime();

    final Function<List<Event>, List<Event>> viewFunction;
    try {
      viewFunction =
          ExtractView.createView(
              group, attributes, aggregates, cassStream, new EventFactory() {}::create);
    } catch (ApplicationException e) {
      return CompletableFuture.failedFuture(e);
    }

    final var srcEvents =
        DataMaintenanceUtils.getEventsInRange(extractedEvents, startTime, endTime);

    if (debugLogEnabled) {
      logger.info("Aggregating events for signal {}", signalDesc.getName());
      logger.info(
          "  rollup={}, range=[{} : {}]",
          rollup.getName(),
          StringUtils.tsToIso8601(startTime),
          StringUtils.tsToIso8601(endTime));
      srcEvents.forEach(
          (event) ->
              logger.info(
                  "     {}: {}",
                  StringUtils.tsToIso8601Millis(event.getIngestTimestamp().getTime()),
                  event));
    }

    final var events = viewFunction.apply(srcEvents);

    final long viewExtractEnd = System.nanoTime();
    logger.debug(
        "fetched events; items={}, range[{} + {}], signal={}, elapsedTime={}",
        events.size(),
        StringUtils.tsToIso8601(startTime),
        StringUtils.shortReadableDuration(endTime - startTime),
        signalDesc.getName(),
        StringUtils.shortReadableDuration(viewExtractEnd - viewExtractStart));

    // This method may send notifications to webhooks but does not wait for the results.
    // So we just call this and continue.
    processAlerts(rollup, events, signalDesc, viewDesc, startTime, endTime);

    logger.trace("rolled up events={}", events);

    // TODO(Naoki): HACK: We are using eventId as timestamp. So "id" would conflict among
    //  events. The rollup table is fine to have duplicate "id"s since its primary key columns
    //  include view dimensions, too.
    final UUID eventId = UUIDs.startOf(endTime);
    for (Event event : events) {
      event.setEventId(eventId);
      event.setIngestTimestamp(new Date(endTime));
      // rename internal output names to rollup attribute names.
      final Map<String, Object> attrs = event.getAttributes();
      for (int i = 0; i < internalOutputNames.size(); ++i) {
        String internalName = internalOutputNames.get(i);
        String rollupAttribute = rollupAttributes.get(i);
        attrs.put(rollupAttribute, attrs.get(internalName));
        attrs.remove(internalName);
      }
    }

    long ingestToRollupStart = System.nanoTime();

    final var state = new GenericExecutionState("aggregate", maintenanceState.getExecutor());
    maintenanceState.addBranch(state);
    return dataEngine
        .insertBulk(rollupStream, events, state)
        .thenApply(
            (none) -> {
              long ingestToRollupEnd = System.nanoTime();
              logger.debug(
                  "Time {} elapsed to ingest items={}, to rollup={}",
                  StringUtils.shortReadableDurationNanos(ingestToRollupEnd - ingestToRollupStart),
                  events.size(),
                  rollupStream.getName());
              return events;
            })
        .toCompletableFuture();
  }

  private void processAlerts(
      Rollup rollup,
      List<Event> events,
      StreamDesc signalDesc,
      ViewDesc viewDesc,
      long rangeStartTime,
      long rangeEndTime) {
    final String signalName = signalDesc.getName();
    final String tenantName = signalDesc.getParent().getName();
    final List<Event> mergedEvents = mergeGroupsbyDimensions(events, viewDesc);
    List<AttributeDesc> attrDescs = new ArrayList<>();
    for (String attrView : viewDesc.getGroupBy()) {
      for (AttributeDesc attrDescSignal : signalDesc.getAttributes()) {
        if (attrDescSignal.getName().equalsIgnoreCase(attrView)) {
          attrDescs.add(attrDescSignal);
        }
      }
    }
    // We need the effective values (output values) of attributes in order to evaluate
    // the alert condition, so replace internal values with output values.
    StorageDataUtils.convertToOutputData(mergedEvents, attrDescs, tenantName, signalName);

    // in case group by is not defined or a BUG
    if (mergedEvents.size() == 0) {
      final Event event = mergeGroups(events, viewDesc);
      StorageDataUtils.convertToOutputData(List.of(event), attrDescs, tenantName, signalName);
      processAlertsForEvent(rollup, event, tenantName, signalName, rangeStartTime, rangeEndTime);
    } else {
      for (Event event : mergedEvents) {
        processAlertsForEvent(rollup, event, tenantName, signalName, rangeStartTime, rangeEndTime);
      }
    }
  }

  /**
   * Merge groups in the events by dimensions.
   *
   * @param events Input events
   * @param viewDesc View description of the events
   * @return merged event
   */
  private List<Event> mergeGroupsbyDimensions(List<Event> events, ViewDesc viewDesc) {
    final var dimensions = new HashSet<>(viewDesc.getGroupBy());
    final Map<String, Event> mergedEvents = new HashMap<>();
    // build entries needed based on the keys
    for (var event : events) {
      Event mergedEvent = null;
      // Iterate to get the group by Keys
      var grpKey =
          new Object() {
            String value = "";
          };
      event
          .getAttributes()
          .forEach(
              (key, value) -> {
                if (!dimensions.contains(key)) {
                  return;
                }
                grpKey.value += String.valueOf(event.get(key));
              });
      if (grpKey.value.length() == 0) {
        continue;
      }
      if (!mergedEvents.containsKey(grpKey.value)) {
        mergedEvent = new EventJson();
        mergedEvents.put(grpKey.value, mergedEvent);
      }
      mergedEvent = mergedEvents.get(grpKey.value);
      final Map<String, Object> attributes = mergedEvent.getAttributes();
      event
          .getAttributes()
          .forEach(
              (key, value) -> {
                final var currentValue = attributes.get(key);
                final Object nextValue;
                if (dimensions.contains(key)) {
                  nextValue = value;
                } else if (currentValue == null) {
                  nextValue = value;
                } else if (key.equals("count()")) {
                  nextValue = (Long) currentValue + (Long) value;
                } else if (key.startsWith("sum(")) {
                  if (value instanceof Double) {
                    nextValue = (Double) currentValue + (Double) value;
                  } else {
                    nextValue = (Long) currentValue + (Long) value;
                  }
                } else if (key.startsWith("min(")) {
                  nextValue =
                      ((Comparable) currentValue).compareTo(value) <= 0 ? currentValue : value;
                } else if (key.startsWith("max(")) {
                  nextValue =
                      ((Comparable) currentValue).compareTo(value) >= 0 ? currentValue : value;
                } else {
                  throw new UnsupportedOperationException(
                      "Attribute " + key + " is not supported for merging groups");
                }
                attributes.put(key, nextValue);
              });
    }
    return new ArrayList<>(mergedEvents.values());
  }

  /**
   * Merge groups in the events.
   *
   * @param events Input events
   * @param viewDesc View description of the events
   * @return merged event
   */
  private Event mergeGroups(List<Event> events, ViewDesc viewDesc) {
    final var dimensions = new HashSet<>(viewDesc.getGroupBy());
    final Event mergedEvent = new EventJson();
    final Map<String, Object> attributes = mergedEvent.getAttributes();
    for (var event : events) {
      event
          .getAttributes()
          .forEach(
              (key, value) -> {
                final var currentValue = attributes.get(key);
                final Object nextValue;
                if (dimensions.contains(key)) {
                  nextValue = value;
                } else if (currentValue == null) {
                  nextValue = value;
                } else if (key.equals("count()")) {
                  nextValue = (Long) currentValue + (Long) value;
                } else if (key.startsWith("sum(")) {
                  if (value instanceof Double) {
                    nextValue = (Double) currentValue + (Double) value;
                  } else {
                    nextValue = (Long) currentValue + (Long) value;
                  }
                } else if (key.startsWith("min(")) {
                  nextValue =
                      ((Comparable) currentValue).compareTo(value) <= 0 ? currentValue : value;
                } else if (key.startsWith("max(")) {
                  nextValue =
                      ((Comparable) currentValue).compareTo(value) >= 0 ? currentValue : value;
                } else {
                  throw new UnsupportedOperationException(
                      "Attribute " + key + " is not supported for merging groups");
                }
                attributes.put(key, nextValue);
              });
    }
    return mergedEvent;
  }

  private void processAlertsForEvent(
      Rollup rollup,
      Event mergedEvent,
      String tenantName,
      String signalName,
      long rangeStartTime,
      long rangeEndTime) {
    try {
      List<AlertConfig> alerts = rollup.getAlerts();
      if (alerts != null) {
        logger.debug("signal={}, Event  merged={}", signalName, mergedEvent);
        if (mergedEvent.getAttributes().isEmpty()) {
          return;
        }
        for (AlertConfig alert : alerts) {
          String alertCondition = alert.getCondition();
          ExpressionParser expressionParser = new ExpressionParser();
          ExpressionTreeNode alertExpressionRoot =
              expressionParser.processExpression(alertCondition);
          try {
            ExpressionEvaluator.evaluateExpression(alertExpressionRoot, mergedEvent);
            Boolean result = (Boolean) alertExpressionRoot.getResult();
            logger.debug(
                "signal={}, alert={}, result={}, event={}",
                signalName,
                alert.getName(),
                result,
                mergedEvent);
            if (result) {
              if (isAlertNotificationEnabled) {
                webhookNotifier.setWebhookUrl(alert.getWebhookUrl());
                webhookNotifier.setAlertType(alert.getType());
                final var notificationContents =
                    new NotificationContents(mergedEvent, rangeStartTime, rangeEndTime);
                notificationContents.setDomainName(biosDomainName());
                notificationContents.setTenantName(tenantName);
                notificationContents.setSignalName(signalName);
                notificationContents.setFeatureName(rollup.getName());
                notificationContents.setAlertName(alert.getName());
                notificationContents.setCondition(alert.getCondition());
                // TODO(Naoki): Check the destination config once it's introduced.
                // The following assumes in-message authentication methodology.
                notificationContents.setUser(alert.getUserName());
                notificationContents.setPassword(alert.getPassword());
                logger.trace(
                    "Dispatching alert {} to webhook {}",
                    notificationContents,
                    alert.getWebhookUrl());
                webhookNotifier.sendNotification(
                    notificationContents, "Alert", notificationRetryCount);
              }
            }
          } catch (UnexpectedValueException | NotificationFailedException e) {
            logger.warn(
                "Exception {} while processing alert {} on" + " rollup event {}",
                e,
                alert.getName(),
                mergedEvent);
          }
          expressionParser = null;
        }
      }
    } catch (InvalidRuleException e) {
      logger.info(
          "Got exception {} while processing alerts for rollup {}",
          e.getMessage(),
          rollup.toString());
    }
  }
}
