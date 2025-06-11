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
import static java.lang.Boolean.TRUE;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DynamicServerRecord;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextDigestor {
  private static final Logger logger = LoggerFactory.getLogger(ContextDigestor.class);

  public static final String KEY_CONTEXT_SKETCH_UPDATE_MIN_INTERVAL =
      "prop.contextSketchUpdateMinInterval";
  public static final String KEY_CONTEXT_SKETCH_UPDATE_CHECK_PROBABILITY =
      "prop.contextSketchUpdateCheckProbability";

  // Maximum number of concurrent tasks for context feature calculation.
  private static final int NUM_CONTEXT_TASK_SLOTS = TfosConfig.numContextTaskSlots();

  private final DataEngineMaintenance maintenance;
  private final DataEngineImpl dataEngine;
  private final PostProcessScheduler postProcessScheduler;
  private final ContextIndexer contextIndexer;
  private final SketchStore sketchStore;

  public ContextDigestor(
      DataEngineMaintenance maintenance,
      DataEngineImpl dataEngine,
      PostProcessScheduler postProcessScheduler,
      ContextIndexer contextIndexer,
      SketchStore sketchStore) {
    this.maintenance = maintenance;
    this.dataEngine = dataEngine;
    this.postProcessScheduler = postProcessScheduler;
    this.contextIndexer = contextIndexer;
    this.sketchStore = sketchStore;
  }

  public CompletableFuture<ContextDerivativeCalculator> execute(
      PostProcessSpecifiers specs, DigestState state) {
    if (specs == null || specs.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    final var contextDesc = specs.getStreamDesc();
    final var tenant = contextDesc.getParent().getName();
    final var context = contextDesc.getName();

    final var fullIndexBuildSpecs = new ArrayList<DigestSpecifier>();
    state.addHistory("(digestInner");
    logger.debug(
        "Start post-process; tenant={}, context={}, timeRange={} : {}",
        tenant,
        context,
        state.getStartTime(),
        state.getEndTime());
    logger.debug("index={}", specs.getIndexes());
    // try {
    state.setStarted(true);
    state.addHistory("(index");
    return contextIndexer
        .execute(contextDesc, specs.getIndexes(), fullIndexBuildSpecs, state)
        .thenComposeAsync(
            (none) -> {
              state.addHistory(")(checkWhetherToUpdateDerivatives");
              state.setIndexEnd(System.currentTimeMillis());
              final boolean checkWhetherToUpdateDerivatives =
                  !specs.getContextFeatures().isEmpty()
                      || !fullIndexBuildSpecs.isEmpty()
                      || specs.getSketches().stream()
                          .anyMatch((spec) -> spec.getRequested() == TRUE);

              state.addHistory(")(reserveDeriving->");
              if (!maintenance.reserveOp(contextDesc, PostProcessOperation.DERIVE_CONTEXT)) {
                // occupied already
                state.addHistory("occupied)");
                return CompletableFuture.completedFuture(null);
              }
              state.addHistory("reserved)(isDerivativeStale->");

              return determineIfDerivativeIsStale(tenant, contextDesc, specs, state)
                  .thenApplyAsync(
                      (updateSketchesAndFeatures) ->
                          ExecutionHelper.supply(
                              () -> {
                                state.addHistory("stale)");
                                if (checkWhetherToUpdateDerivatives || updateSketchesAndFeatures) {
                                  return new ContextDerivativeCalculator(
                                      dataEngine,
                                      postProcessScheduler,
                                      sketchStore,
                                      contextDesc,
                                      specs.getContextFeatures(),
                                      fullIndexBuildSpecs,
                                      specs.getSketches(),
                                      updateSketchesAndFeatures);
                                }
                                state.addHistory("not-stale)");
                                maintenance.concludeOp(
                                    contextDesc, PostProcessOperation.DERIVE_CONTEXT);
                                state.addHistory(")");
                                return null;
                              }),
                      state.getExecutor());
            },
            state.getExecutor());
  }

  private CompletableFuture<Boolean> determineIfDerivativeIsStale(
      String tenant, StreamDesc contextDesc, PostProcessSpecifiers specs, ExecutionState state) {
    // Find out the number of changes that happened to this context since the last time
    // we calculated the sketches.
    // If it is above a certain threshold relative to the size of the context,
    // we will update the sketches and features.
    long contextSketchUpdateMinInterval =
        SharedProperties.getCached(KEY_CONTEXT_SKETCH_UPDATE_MIN_INTERVAL, 300000L);
    final long startTime =
        Utils.floor(
            specs.getSketches().stream()
                .mapToLong(DigestSpecifierBase::getEndTime)
                .min()
                .orElse(0L),
            contextSketchUpdateMinInterval);
    final long endTime = Utils.floor(System.currentTimeMillis(), contextSketchUpdateMinInterval);

    // Even the queries used to retrieve the number of context entry changes can get expensive
    // for large time periods. So only perform these checks once in a while, not every time
    // the digestion worker runs.
    logger.debug("context={} startTime={} endTime={}", contextDesc.getName(), startTime, endTime);
    double contextSketchUpdateCheckProbability =
        SharedProperties.getCached(KEY_CONTEXT_SKETCH_UPDATE_CHECK_PROBABILITY, 0.1);
    if (endTime <= startTime || Math.random() > contextSketchUpdateCheckProbability) {
      return CompletableFuture.completedFuture(false);
    }

    logger.debug("Querying context stats for context={}", contextDesc.getName());
    final var summarizeRequest = new SummarizeRequest();
    summarizeRequest.setStartTime(startTime);
    summarizeRequest.setEndTime(endTime);
    summarizeRequest.setInterval(endTime - startTime);
    summarizeRequest.setSnappedStartTime(startTime);
    summarizeRequest.setHorizon(endTime - startTime);
    summarizeRequest.setAggregates(List.of(new Aggregate(MetricFunction.COUNT, null)));
    final String auditSignal =
        StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, contextDesc.getName());
    final StreamDesc auditSignalDesc =
        BiosModules.getAdminInternal().getStreamOrNull(tenant, auditSignal);
    if (auditSignalDesc == null) {
      // not possible to determine by this approach.
      return CompletableFuture.completedFuture(false);
    }
    final var summarizeState =
        new SummarizeState(
            "determineToUpdateSketches",
            tenant,
            auditSignal,
            DynamicServerRecord.newEventFactory(auditSignalDesc),
            state);
    // summarizeState.setDataEngine(dataEngine);
    summarizeState.setStreamDesc(auditSignalDesc);
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);

    return dataEngine
        .summarize(summarizeState)
        .thenComposeAsync(
            (response) -> {
              if (response == null || response.isEmpty()) {
                return CompletableFuture.completedFuture(false);
              }
              final Event event = response.values().iterator().next().get(0);
              final long auditEventCount = (Long) event.get("count()");
              logger.debug(
                  "context={} auditEventCount={} endTime-startTime = {}",
                  contextDesc.getName(),
                  auditEventCount,
                  endTime - startTime);

              if (auditEventCount == 0) {
                return CompletableFuture.completedFuture(false);
              }

              // Get the count of entries in the context.
              final var aggregates = List.of((Aggregate) new Count());
              final var selectRequest = new SelectContextRequest();
              selectRequest.setContext(contextDesc.getName());
              // selectRequest.setMetrics(metrics);
              final var contextOpState =
                  new ContextOpState(
                      "Maintain_" + contextDesc.getName(), ExecutorManager.getSidelineExecutor());
              contextOpState.setContextDesc(contextDesc);
              return dataEngine
                  .getSketchesExtractor()
                  .summarizeContext(
                      selectRequest, aggregates, System.currentTimeMillis(), contextOpState)
                  .thenApplyAsync(
                      (response2) -> {
                        if (response2.size() > 0) {
                          final Event event2 = response2.get(0);
                          final long contextEntryCount = (Long) event2.get("count()");
                          logger.debug(
                              "context={} contextEntryCount = {}",
                              contextDesc.getName(),
                              contextEntryCount);

                          // If the time since last update is more than a certain period, update the
                          // derivatives. The amount of time is proportional to the amount of work,
                          // which is proportional to the number of entries * number of attributes.
                          final long work =
                              contextEntryCount * contextDesc.getAllBiosAttributes().size();
                          long contextSketchUpdateWorkTimeFactor =
                              SharedProperties.getCached(
                                  "prop.contextSketchUpdateWorkTimeFactor", 100L);
                          long contextSketchUpdateWorkTimeFactorLow =
                              SharedProperties.getCached(
                                  "prop.contextSketchUpdateWorkTimeFactorLow", 2L);
                          if ((endTime - startTime) > work * contextSketchUpdateWorkTimeFactor) {
                            logger.info(
                                "context={} updateSketchesAndFeatures = true (endTime-startTime = {},"
                                    + " work = {})",
                                contextDesc.getName(),
                                endTime - startTime,
                                work);
                            return true;
                          }

                          // If the number of changes is above a certain threshold, update the
                          // derivatives
                          // with a lower requirement on time elapsed between updates.
                          double contextSketchUpdateLowThresholdRatio =
                              SharedProperties.getCached(
                                  "prop.contextSketchUpdateLowThresholdRatio", 0.1);
                          if (auditEventCount
                                  > contextEntryCount * contextSketchUpdateLowThresholdRatio
                              && (endTime - startTime)
                                  > work * contextSketchUpdateWorkTimeFactorLow) {
                            logger.info(
                                "context={} updateSketchesAndFeatures = true (auditEventCount = {}, "
                                    + "contextEntryCount = {} endTime-startTime = {} work = {})",
                                contextDesc.getName(),
                                auditEventCount,
                                contextEntryCount,
                                endTime - startTime,
                                work);
                            return true;
                          }
                        }
                        return false;
                      },
                      state.getExecutor());
            },
            state.getExecutor())
        .toCompletableFuture();
  }
}
