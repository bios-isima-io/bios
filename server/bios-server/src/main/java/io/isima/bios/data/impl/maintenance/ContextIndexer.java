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

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;

import com.ibm.asyncutil.iteration.AsyncTrampoline;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.ContextIndexHelper;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextIndexer {
  private static final Logger logger = LoggerFactory.getLogger(ContextIndexer.class);

  public static final String KEY_ROLLUP_DETAIL_LOG_ENABLED = "prop.rollupDetailLogEnabled";

  private final DataEngineMaintenance dataEngineMaintenance;
  private final DataEngine dataEngine;
  private final SharedProperties sharedProperties;
  private final PostProcessScheduler postProcessScheduler;

  public ContextIndexer(
      DataEngineMaintenance dataEngineMaintenance,
      DataEngine dataEngine,
      SharedProperties sharedProperties,
      PostProcessScheduler postProcessScheduler) {
    this.dataEngineMaintenance = dataEngineMaintenance;
    this.dataEngine = dataEngine;
    this.sharedProperties = sharedProperties;
    this.postProcessScheduler = postProcessScheduler;
  }

  /**
   * Method to digest context table events into index table.
   *
   * @param contextDesc StreamConfig to digest
   * @param specifiers Digest specifiers for indexing
   * @throws Throwable The method re-throws whatever exception happened during the procedure
   */
  public CompletableFuture<Void> execute(
      StreamDesc contextDesc,
      List<DigestSpecifier> specifiers,
      List<DigestSpecifier> retroactiveSpecifiers,
      DigestState state) {

    if (specifiers.isEmpty()) {
      CompletableFuture.completedFuture(null);
    }

    final List<DigestSpecifier> indexingSpecifiers = new ArrayList<>();
    long left = Long.MAX_VALUE;
    long right = Long.MIN_VALUE;
    for (var spec : specifiers) {
      final var specStartTime = spec.getStartTime();
      if (specStartTime == -1) {
        retroactiveSpecifiers.add(spec);
      } else {
        indexingSpecifiers.add(spec);
        left = Math.min(left, spec.getStartTime());
        right = Math.max(right, spec.getEndTime());
        logger.debug(
            "  index={}, range=[{} : {}]",
            spec.getName(),
            StringUtils.tsToIso8601(spec.getStartTime()),
            StringUtils.tsToIso8601(spec.getEndTime()));
      }
    }

    final long startTime = left;
    final long endTime = right;

    if (indexingSpecifiers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    logger.debug(
        "  range to retrieve=[{} : {}]",
        StringUtils.tsToIso8601(startTime),
        StringUtils.tsToIso8601(endTime));

    logger.debug(
        "start extracting events, tenant={}, signal={}, start={} end={}",
        contextDesc.getParent().getName(),
        contextDesc.getName(),
        startTime,
        endTime);

    long signalExtractStart = System.nanoTime();
    StreamDesc auditSignalDesc =
        BiosModules.getAdminInternal()
            .getStreamOrNull(
                contextDesc.getParent().getName(),
                StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, contextDesc.getName()));
    if (auditSignalDesc == null) {
      CompletableFuture.completedFuture(null);
    }
    state.addHistory("(index(fetchEvents");

    return DataMaintenanceUtils.extractEventsAsync(
            auditSignalDesc, startTime, endTime, null, null, null, "indexContext", state)
        .thenApplyAsync(
            (extractedEvents) -> {
              state.addHistory(")");
              logger.debug("{} messages were found", extractedEvents.size());

              long signalExtractEnd = System.nanoTime();
              logger.debug(
                  "time {} elapsed to fetch items={}, range=[{} + {}], from context={}",
                  StringUtils.shortReadableDurationNanos(signalExtractEnd - signalExtractStart),
                  extractedEvents.size(),
                  StringUtils.tsToIso8601(startTime),
                  StringUtils.shortReadableDuration(endTime - startTime),
                  contextDesc.getName());

              final Function<List<Event>, List<Event>> sortFunction =
                  ContextIndexHelper.createSortFunction();
              final List<Event> events = sortFunction.apply(extractedEvents);
              return events;
            },
            state.getExecutor())
        .thenComposeAsync(
            (events) -> {
              return AsyncTrampoline.asyncWhile(
                  (iterator) -> iterator.hasNext(),
                  (iterator) -> {
                    final var specifier = iterator.next();
                    final StreamDesc indexStream =
                        BiosModules.getAdminInternal()
                            .getStreamOrNull(
                                contextDesc.getParent().getName(),
                                specifier.getName(),
                                contextDesc.getVersion());
                    if (indexStream == null) {
                      // already removed, skipping
                      return CompletableFuture.completedFuture(iterator);
                    }

                    return calculateIndex(contextDesc, indexStream, specifier, events, state)
                        .thenComposeAsync(
                            (none) -> {
                              state.getProcessedSpecifiers().add(specifier);
                              return postProcessScheduler.recordCompletion(
                                  contextDesc, specifier, state);
                            },
                            state.getExecutor())
                        .whenCompleteAsync(
                            (none, t) -> {
                              if (t != null) {
                                logger.error(
                                    "Failed to populate context index;"
                                        + " tenant={}, index={}, version={}({}):",
                                    contextDesc.getParent().getName(),
                                    specifier.getName(),
                                    specifier.getVersion(),
                                    StringUtils.tsToIso8601(specifier.getVersion()),
                                    t);
                                // log and continue, but we don't record completion
                              }
                            },
                            state.getExecutor())
                        .thenApply((none) -> iterator);
                  },
                  indexingSpecifiers.iterator());
            },
            state.getExecutor())
        .thenRun(() -> state.addHistory(")"))
        .toCompletableFuture();
  }

  private CompletableFuture<Void> calculateIndex(
      StreamDesc contextDesc,
      StreamDesc indexStream,
      DigestSpecifier specifier,
      List<Event> events,
      DigestState state) {
    if (events.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    final FeatureDesc feature = indexStream.getFeatures().get(0);
    final var auditTimeRange = new Range(specifier.getStartTime(), specifier.getEndTime());
    HashMap<String, List<Event>> eventsMap =
        ContextIndexHelper.collectIndexEntries(
            contextDesc,
            feature,
            auditTimeRange,
            events,
            dataEngineMaintenance.getActivityRecorder());
    List<Event> newEvents = eventsMap.get("new");
    List<Event> deletedEvents = eventsMap.get("deleted");

    final var executionState = new GenericExecutionState("populateContextIndex", state);
    return dataEngine
        .populateContextIndexes(contextDesc, indexStream, newEvents, deletedEvents, executionState)
        .thenComposeAsync(
            (none) -> {
              state.setIndexedEvents(newEvents.size() + deletedEvents.size());
              return sharedProperties
                  .getPropertyBooleanAsync(KEY_ROLLUP_DETAIL_LOG_ENABLED, false, state)
                  .thenAcceptAsync(
                      (detailedLogEnabled) -> {
                        if (detailedLogEnabled) {
                          logger.info(
                              "context indexed; new_items={}, deleted_items={}, range=[{} + {}],"
                                  + " coverage=[{} - {}], tenant={}, index={}, version={}({})",
                              newEvents.size(),
                              deletedEvents.size(),
                              StringUtils.tsToIso8601(specifier.getStartTime()),
                              StringUtils.shortReadableDuration(
                                  specifier.getEndTime() - specifier.getStartTime()),
                              StringUtils.tsToIso8601(specifier.getDoneUntil()),
                              StringUtils.shortReadableDuration(
                                  specifier.getDoneUntil() - specifier.getDoneSince()),
                              contextDesc.getParent().getName(),
                              specifier.getName(),
                              specifier.getVersion(),
                              StringUtils.tsToIso8601(specifier.getVersion()));
                        }
                      },
                      state.getExecutor());
            },
            state.getExecutor());
  }
}
