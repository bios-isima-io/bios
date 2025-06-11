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
import static io.isima.bios.admin.v1.AdminConstants.ROOT_FEATURE;
import static java.lang.Boolean.TRUE;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.impl.ContextIndexHelper;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextEntryRetriever;
import io.isima.bios.data.impl.storage.ContextIterationState;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Sum;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates features and indexes of a context by iterating all rows.
 *
 * <p>The class compiles the given specifiers first, then schedules the execution on demand by using
 * method {@link #scheduleCalculation(TaskSlots)}. The execution is done asynchronously.
 */
public class ContextDerivativeCalculator {
  private static final Logger logger = LoggerFactory.getLogger(ContextDerivativeCalculator.class);
  public static final String PROP_BATCH_SIZE = "prop.contextFeatureBatchSize";

  private final DataEngineImpl dataEngineImpl;
  private final DataEngineMaintenance maintenance;
  private final StreamDesc contextDesc;
  private final String contextName;
  private final List<DigestSpecifier> featureSpecifiers;
  private final List<DigestSpecifier> indexingSpecifiers;
  private final ContextCassStream contextCassStream;
  private final List<StreamDesc> featureSubStreams;
  private final List<ExtractView.GroupCalculator> featureCalculators;
  private final List<StreamDesc> indexSubStreams;
  private final List<Event> batchEvents;
  private final List<Function<List<Event>, List<Event>>> indexFunctions;
  private final List<AtomicLong> numIndexes;
  private final String tenantName;
  private final long rollupTime;
  private final PostProcessScheduler scheduler;
  private final SketchStore sketchStore;
  private final SketchGroup sketchGroup;

  public ContextDerivativeCalculator(
      DataEngineImpl dataEngineImpl,
      PostProcessScheduler scheduler,
      SketchStore sketchStore,
      StreamDesc contextDesc,
      List<DigestSpecifier> featureSpecifiers,
      List<DigestSpecifier> indexingSpecifiers,
      List<DataSketchSpecifier> sketchSpecifiers,
      boolean updateSketchesAndFeatures)
      throws ApplicationException {
    this.dataEngineImpl = dataEngineImpl;
    this.maintenance = dataEngineImpl.getMaintenance();
    this.scheduler = scheduler;
    this.sketchStore = sketchStore;
    this.contextDesc = contextDesc;
    this.contextName = contextDesc.getName();
    this.featureSpecifiers = new ArrayList<>();
    this.indexingSpecifiers = new ArrayList<>();
    this.contextCassStream = (ContextCassStream) dataEngineImpl.getCassStream(contextDesc);
    if (this.contextCassStream == null) {
      throw new ApplicationException(
          String.format(
              "ContextCassStream for context %s not found; tenant=%s",
              contextName, contextDesc.getParent().getName()));
    }
    // TODO(Naoki): create classes FeatureInfo and IndexingInfo and bundle following
    featureSubStreams = new ArrayList<>();
    featureCalculators = new ArrayList<>();
    indexSubStreams = new ArrayList<>();
    batchEvents = new ArrayList<>();
    indexFunctions = new ArrayList<>();
    numIndexes = new ArrayList<>();
    this.tenantName = contextDesc.getParent().getName();
    this.rollupTime = System.currentTimeMillis();
    compileIndexingSpecifiers(indexingSpecifiers);
    compileFeatureSpecifiers(featureSpecifiers, updateSketchesAndFeatures);
    sketchGroup = compileSketchSpecifiers(sketchSpecifiers, updateSketchesAndFeatures);
  }

  private void compileIndexingSpecifiers(List<DigestSpecifier> indexingSpecifiers) {
    AdminInternal admin = BiosModules.getAdminInternal();
    for (var spec : indexingSpecifiers) {
      final var subStreamName = spec.getName();
      final var subStreamVersion = spec.getVersion();
      final var indexStream = admin.getStreamOrNull(tenantName, subStreamName);
      if (indexStream != null) {
        indexSubStreams.add(indexStream);
      } else {
        logger.error(
            "Index substream not found; tenant={}, substream={}, version={}",
            tenantName,
            subStreamName,
            subStreamVersion);
        continue;
      }
      final FeatureDesc feature = indexStream.getFeatures().get(0);
      indexFunctions.add(
          ContextIndexHelper.createRetroactiveContextIndexFunction(contextDesc, feature));
      numIndexes.add(new AtomicLong(0));
      this.indexingSpecifiers.add(spec);
    }
  }

  private void compileFeatureSpecifiers(
      List<DigestSpecifier> featureSpecifiers, boolean updateSketchesAndFeatures) {
    final boolean requested =
        featureSpecifiers.stream().anyMatch((spec) -> spec.getRequested() == TRUE);
    if (!requested && !updateSketchesAndFeatures) {
      return;
    }
    AdminInternal admin = BiosModules.getAdminInternal();
    for (var spec : featureSpecifiers) {
      // register the substream
      final var subStreamName = spec.getName();
      final var subStreamVersion = spec.getVersion();
      final var featureStream = admin.getStreamOrNull(tenantName, subStreamName);
      if (featureStream != null && featureStream.getSchemaVersion().equals(subStreamVersion)) {
        featureSubStreams.add(featureStream);
      } else {
        logger.error(
            "Feature substream not found; tenant={}, substream={}, version={}",
            tenantName,
            subStreamName,
            subStreamVersion);
        continue;
      }

      // register the calculator
      final var feature = featureStream.getViews().get(0);
      final var group = new Group(feature.getGroupBy());
      final var aggregates = new ArrayList<Aggregate>();
      aggregates.add(new Count().as(ROLLUP_STREAM_COUNT_ATTRIBUTE));
      for (String attribute : feature.getAttributes()) {
        aggregates.add(new Sum(attribute).as(attribute + ROLLUP_STREAM_SUM_SUFFIX));
        aggregates.add(new Min(attribute).as(attribute + ROLLUP_STREAM_MIN_SUFFIX));
        aggregates.add(new Max(attribute).as(attribute + ROLLUP_STREAM_MAX_SUFFIX));
      }
      featureCalculators.add(
          new ExtractView.GroupCalculator(
              group, List.of(), aggregates, contextCassStream, EventJson::new));
      this.featureSpecifiers.add(spec);
    }
  }

  private SketchGroup compileSketchSpecifiers(
      List<DataSketchSpecifier> sketchSpecifiers, boolean updateSketchesAndFeatures) {
    if (sketchSpecifiers == null || sketchSpecifiers.isEmpty()) {
      return null;
    }
    final boolean requested =
        sketchSpecifiers.stream().anyMatch((spec) -> spec.getRequested() == TRUE);
    if (!requested && !updateSketchesAndFeatures) {
      return null;
    }
    final var specs =
        sketchSpecifiers.stream()
            .map(
                (spec) -> {
                  final var newSpec =
                      new DataSketchSpecifier(
                          spec.getDataSketchDuration(),
                          spec.getAttributeProxy(),
                          spec.getDataSketchType(),
                          0,
                          rollupTime + 1,
                          rollupTime,
                          rollupTime);
                  newSpec.setAttributeName(spec.getAttributeName());
                  newSpec.setAttributeType(spec.getAttributeType());
                  newSpec.setRequested(TRUE);
                  return newSpec;
                })
            .collect(Collectors.toList());
    return new SketchGroup(contextDesc, specs);
  }

  /**
   * Schedule a feature and index calculation task if necessary.
   *
   * <p>The feature and index calculation task pends for an available task slot.
   *
   * <p>These steps are done asynchronously, so the method returns immediately. The returned
   * completion stage is fulfilled when all the stages are completed.
   *
   * @param taskSlots The task slots to be utilized.
   * @return Completion stage for the task completion.
   */
  public CompletionStage<Void> scheduleCalculation(TaskSlots taskSlots) {
    if (featureCalculators.isEmpty() && indexFunctions.isEmpty() && sketchGroup == null) {
      return CompletableFuture.completedStage(null);
    }
    final var digestionExecutor = BiosModules.getDigestor().getExecutor();
    return taskSlots.runAsyncTask(
        () -> execute(digestionExecutor),
        digestionExecutor,
        String.format("Context maintenance %s.%s", tenantName, contextName));
  }

  /** Lock the context, execute feature calculation, and unlock. */
  public CompletionStage<Void> execute(Executor digestionExecutor) {
    if (featureCalculators.isEmpty() && indexFunctions.isEmpty() && sketchGroup == null) {
      return CompletableFuture.completedStage(null);
    }

    final var state =
        new ContextIterationState("Calculate Context Features", digestionExecutor)
            .setTimestamp(rollupTime)
            .setStartTime(System.currentTimeMillis())
            .enableEntryCounter();
    // TODO(Naoki): Fetch the feature status again here after acquiring the lock
    //  to see if another node has done with this context already.
    logger.info("Context deriving started; tenant={}, context={}", tenantName, contextName);
    return calculationCore(state)
        .whenCompleteAsync(
            (none, t) -> {
              if (t != null) {
                final var cause = t instanceof CompletionException ? t.getCause() : t;
                logger.error(
                    "Context driving failed; tenant={}, stream={}, version={}, schemaVersion={}",
                    contextDesc.getParent().getName(),
                    contextName,
                    contextDesc.getVersion(),
                    contextDesc.getSchemaVersion(),
                    cause);
                logger.error("Call trace; {}", state.getCallTraceString());
              }
            },
            digestionExecutor);
  }

  private CompletionStage<Void> calculationCore(ContextIterationState state) {

    return BiosModules.getSharedProperties()
        .getPropertyAsync(PROP_BATCH_SIZE, state)
        .thenApply(
            (prop) -> {
              if (prop != null) {
                try {
                  return Integer.valueOf(prop);
                } catch (NumberFormatException e) {
                  logger.error(
                      "Invalid value syntax in shared property; property={}, value={}, error={}",
                      PROP_BATCH_SIZE,
                      prop,
                      e.getMessage());
                }
              }
              // return default value
              return 32768;
            })
        .thenComposeAsync(
            (batchSize) ->
                ExecutionHelper.supply(
                    () -> {
                      final var contextKeyRetriever =
                          new ContextEntryRetriever(
                              contextCassStream, state, batchSize, ContextEntryRetriever.Type.ALL);

                      final var contexts =
                          featureCalculators.stream()
                              .map((calculator) -> calculator.start())
                              .collect(Collectors.toList());

                      state.setEntryConsumer(
                          (primaryKey, entry, timestamp) -> {
                            if (maintenance.isShutdown()
                                || maintenance.isStreamDeleted(contextDesc)) {
                              contextKeyRetriever.cancel();
                              return;
                            }
                            logger.debug(
                                "pkey: {}, ts: {}, entry: {}", primaryKey, timestamp, entry);
                            for (int i = 0; i < featureCalculators.size(); ++i) {
                              featureCalculators.get(i).consumeRecord(entry, contexts.get(i));
                            }
                            if (!indexFunctions.isEmpty()) {
                              batchEvents.add(entry);
                            }
                            if (sketchGroup != null) {
                              sketchGroup.updateSketches("", (range) -> List.of(entry));
                            }
                          });

                      state.setBatchPostProcess(
                          (elapsed) -> {
                            return batchPostProcess(elapsed, 0, state);
                          });

                      return contextCassStream
                          .iterateRows(contextKeyRetriever, state)
                          .thenComposeAsync(
                              (none) -> finalizeIndexCalculation(0, state), state.getExecutor())
                          .thenComposeAsync(
                              (none) ->
                                  finalizeFeatureCalculation(
                                      featureCalculators, contexts, 0, state),
                              state.getExecutor())
                          .thenComposeAsync(
                              (none) -> finalizeSketchCalculation(state), state.getExecutor())
                          .thenComposeAsync((none) -> finalizeAll(state), state.getExecutor());
                    }),
            state.getExecutor());
  }

  private CompletionStage<Void> batchPostProcess(Long elapsed, int index, ExecutionState state) {
    if (index >= indexFunctions.size()) {
      // no more to process. clear the batch events and return immediately
      batchEvents.clear();
      return CompletableFuture.completedStage(null);
    }

    final var indexes = indexFunctions.get(index).apply(batchEvents);
    numIndexes.get(index).addAndGet(batchEvents.size());
    if (!indexes.isEmpty()) {
      return dataEngineImpl
          .populateContextIndexes(
              contextDesc, indexSubStreams.get(index), indexes, List.of(), state)
          .thenComposeAsync((none) -> batchPostProcess(elapsed, index + 1, state));
    }
    return batchPostProcess(elapsed, index + 1, state);
  }

  private CompletionStage<Void> finalizeIndexCalculation(int index, ContextIterationState state) {
    if (index == indexFunctions.size()) {
      return CompletableFuture.completedStage(null);
    }
    final var subStream = indexSubStreams.get(index);
    final var origSpecifier = indexingSpecifiers.get(index);
    final long doneSince =
        origSpecifier.getDoneSince() >= 0 ? origSpecifier.getDoneSince() : rollupTime;
    state.addHistory(
        String.format(
            "(%s.%d (version=%d)",
            subStream.getName(), subStream.getSchemaVersion(), subStream.getVersion()));
    final var specifier =
        new DigestSpecifier(
            subStream.getName(),
            subStream.getSchemaVersion(),
            -1,
            -1,
            doneSince,
            rollupTime,
            null,
            false);
    return scheduler
        .recordCompletion(contextDesc, specifier, state)
        .thenComposeAsync(
            (none) -> {
              logger.info(
                  "Context indexed; items={}, doneUntil={}, tenant={}, stream={}, version={}",
                  numIndexes.get(index).get(),
                  StringUtils.tsToIso8601(rollupTime),
                  tenantName,
                  subStream.getName(),
                  subStream.getSchemaVersion());
              state.addHistory(")");
              return finalizeIndexCalculation(index + 1, state);
            },
            state.getExecutor());
  }

  private CompletionStage<Void> finalizeFeatureCalculation(
      List<ExtractView.GroupCalculator> calculators,
      List<ExtractView.GroupCalculator.Context> contexts,
      int index,
      ExecutionState state) {
    if (index == calculators.size()) {
      return CompletableFuture.completedStage(null);
    }
    final var subStream = featureSubStreams.get(index);
    final var origSpecifier = featureSpecifiers.get(index);
    final long doneSince =
        origSpecifier.getDoneSince() >= 0 ? origSpecifier.getDoneSince() : rollupTime;
    state.addHistory(
        String.format(
            "(%s.%d (version=%d)",
            subStream.getName(), subStream.getSchemaVersion(), subStream.getVersion()));
    final var records = calculators.get(index).finish(contexts.get(index));
    logger.debug("feature records: {}", records);
    records.forEach(
        (record) -> {
          record.setEventId(UUIDs.startOf(rollupTime));
          record.setIngestTimestamp(new Date(doneSince));
        });
    return dataEngineImpl
        .insertBulk(subStream, records, state)
        .thenComposeAsync(
            (results) -> {
              final var specifier =
                  new DigestSpecifier(
                      subStream.getName(),
                      subStream.getSchemaVersion(),
                      -1,
                      -1,
                      doneSince,
                      rollupTime,
                      null,
                      false);
              return scheduler.recordCompletion(contextDesc, specifier, state);
            },
            state.getExecutor())
        .thenComposeAsync(
            (none) -> {
              if (origSpecifier.getStartTime() >= 0
                  && origSpecifier.getStartTime() != origSpecifier.getDoneSince()) {
                return dataEngineImpl.clearTimeIndex(
                    subStream, origSpecifier.getStartTime(), state);
              }
              return CompletableFuture.completedStage(null);
            },
            state.getExecutor())
        .thenComposeAsync(
            (none) -> {
              logger.info(
                  "Context summarized; items={}, doneUntil={}, tenant={}, stream={}, version={}",
                  records.size(),
                  StringUtils.tsToIso8601(rollupTime),
                  tenantName,
                  subStream.getName(),
                  subStream.getSchemaVersion());
              state.addHistory(")");
              return finalizeFeatureCalculation(calculators, contexts, index + 1, state);
            },
            state.getExecutor());
  }

  private CompletionStage<Void> finalizeSketchCalculation(ContextIterationState state) {
    if (sketchGroup == null) {
      return CompletableFuture.completedStage(null);
    }
    state.addHistory("finalizeSketch(");
    return sketchGroup
        .writeSketches(sketchStore, scheduler, contextDesc, state)
        .thenRunAsync(
            () -> {
              logger.info(
                  "Context sketches updated;"
                      + " items={}, endTime={}, tenant={}, context={}, elapsedMillis={}",
                  state.getEntryCounter().get(),
                  StringUtils.tsToIso8601(rollupTime),
                  tenantName,
                  contextName,
                  System.currentTimeMillis() - state.getStartTime());
              state.addHistory(")");
            },
            state.getExecutor());
  }

  private CompletionStage<Void> finalizeAll(ContextIterationState state) {
    state.addHistory("finalizeAll(");
    final var specifier =
        new DigestSpecifier(
            ROOT_FEATURE,
            contextDesc.getSchemaVersion(),
            -1,
            -1,
            rollupTime,
            rollupTime,
            null,
            false);
    return scheduler
        .recordCompletion(contextDesc, specifier, state)
        .thenRun(
            () -> {
              logger.info(
                  "Context derived; tenant={}, context={}, elapsedMillis={}",
                  tenantName,
                  contextName,
                  System.currentTimeMillis() - state.getStartTime());
              state.addHistory(")");
              state.markDone();
            });
  }
}
