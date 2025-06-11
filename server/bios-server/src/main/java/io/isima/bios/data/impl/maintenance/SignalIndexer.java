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

import com.ibm.asyncutil.iteration.AsyncTrampoline;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.StringUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalIndexer {
  private static final Logger logger = LoggerFactory.getLogger(SignalIndexer.class);

  private final DataEngineMaintenance dataEngineMaintenance;
  private final DataEngine dataEngine;
  private final PostProcessScheduler postProcessScheduler;
  private final SharedConfig sharedConfig;

  public SignalIndexer(
      DataEngineMaintenance dataEngineMaintenance,
      DataEngine dataEngine,
      PostProcessScheduler postProcessScheduler,
      SharedConfig sharedConfig) {
    this.dataEngineMaintenance = dataEngineMaintenance;
    this.dataEngine = dataEngine;
    this.postProcessScheduler = postProcessScheduler;
    this.sharedConfig = sharedConfig;
  }

  /**
   * Method to digest signal table events into ingest view.
   *
   * @param events Source signal events
   * @param signalStream StreamConfig to digest
   * @param indexingSpecifiers Digest specifiers for indexing
   * @throws Throwable The method re-throws whatever exception happened during the procedure
   */
  public CompletableFuture<Void> index(
      StreamDesc signalStream,
      List<DigestSpecifier> indexingSpecifiers,
      DigestState state,
      List<Event> srcEvents,
      ExecutionState maintenanceState) {

    if (indexingSpecifiers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return DataMaintenanceUtils.isRollupDebugLogEnabled(signalStream, maintenanceState)
        .thenComposeAsync(
            (debugLogEnabled) -> {
              return AsyncTrampoline.asyncWhile(
                  (i) -> i < indexingSpecifiers.size(),
                  (i) -> {
                    final var specifier = indexingSpecifiers.get(i);
                    final StreamDesc viewStream =
                        BiosModules.getAdminInternal()
                            .getStreamOrNull(
                                signalStream.getParent().getName(),
                                specifier.getName(),
                                signalStream.getVersion());
                    if (viewStream == null) {
                      // already removed, skipping
                      return CompletableFuture.completedFuture(i + 1);
                    }
                    final ViewDesc viewDesc = viewStream.getViews().get(0);
                    final var events = DataMaintenanceUtils.getEventsInRange(srcEvents, specifier);
                    return populateIngestView(
                            signalStream, viewStream, viewDesc, events, maintenanceState)
                        .thenComposeAsync(
                            (none) ->
                                postProcessScheduler.recordCompletion(
                                    signalStream, specifier, maintenanceState),
                            state.getExecutor())
                        .thenApplyAsync(
                            (none) -> {
                              logger.debug(
                                  "indexed; items={}, range=[{} + {}], coverage=[{} + {}] {}%,"
                                      + " view={}.{}({})",
                                  events.size(),
                                  StringUtils.tsToIso8601(specifier.getStartTime()),
                                  StringUtils.shortReadableDuration(
                                      specifier.getEndTime() - specifier.getStartTime()),
                                  StringUtils.tsToIso8601(specifier.getDoneSince()),
                                  StringUtils.shortReadableDuration(
                                      specifier.getDoneUntil() - specifier.getDoneSince()),
                                  specifier.getDoneCoverage(),
                                  viewDesc.getName(),
                                  specifier.getVersion(),
                                  StringUtils.tsToIso8601(specifier.getVersion()));
                              return i + 1;
                            },
                            state.getExecutor());
                  },
                  0);
            },
            state.getExecutor())
        .thenRun(() -> state.setIndexEnd(System.currentTimeMillis()));
  }

  private CompletableFuture<Void> populateIngestView(
      StreamDesc signalDesc,
      StreamDesc ingestViewConfig,
      ViewDesc viewDesc,
      List<Event> extractedEvents,
      ExecutionState parentState) {
    if (extractedEvents.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    final StreamDesc indexConfig;
    try {
      indexConfig = dataEngineMaintenance.getIndexConfig(signalDesc, viewDesc);
    } catch (NoSuchStreamException | NoSuchTenantException e) {
      return CompletableFuture.failedFuture(e);
    }
    if (indexConfig == null) {
      // already removed, do nothing
      return CompletableFuture.completedFuture(null);
    }

    final long ingestToViewStart = System.nanoTime();

    final var state = new GenericExecutionState("populateIngestView", parentState);

    boolean doWriteIndexes = viewDesc.getIndexTableEnabled() == Boolean.TRUE;
    final CompletableFuture<Void> writeIndexesFuture;
    if (doWriteIndexes) {
      writeIndexesFuture =
          dataEngine
              .insertBulk(ingestViewConfig, extractedEvents, state)
              .thenAcceptAsync(
                  (results) -> {
                    final var errors =
                        results.stream()
                            .filter((result) -> result.getError() != null)
                            .map((result) -> result.getError())
                            .collect(Collectors.toList());
                    if (!errors.isEmpty()) {
                      throw new CompletionException(
                          new ApplicationException(
                              String.format("Writing indexes failed: %s", errors)));
                    }
                    logger.debug(
                        "Time {} elapsed to ingest items={}, to view={}",
                        StringUtils.shortReadableDurationNanos(
                            System.nanoTime() - ingestToViewStart),
                        extractedEvents.size(),
                        ingestViewConfig.getName());
                  },
                  state.getExecutor())
              .toCompletableFuture();
    } else {
      writeIndexesFuture = CompletableFuture.completedFuture(null);
    }

    return writeIndexesFuture.thenComposeAsync(
        (none) -> {
          SignalCassStream cassStream = (SignalCassStream) dataEngine.getCassStream(signalDesc);
          // set of list of (time_index) followed by (list of indexes)
          final Map<Long, Set<List<Object>>> indexes =
              generateIndexes(extractedEvents, viewDesc, cassStream);

          if (doWriteIndexes) {
            return dataEngine.populateIndexes(signalDesc, indexConfig, indexes, state);
          }
          return CompletableFuture.completedFuture(null);
        },
        state.getExecutor());
  }

  private Map<Long, Set<List<Object>>> generateIndexes(
      List<Event> extractedEvents, ViewDesc viewDesc, SignalCassStream cassStream) {
    final Map<Long, Set<List<Object>>> totalIndexes = new HashMap<>();
    for (Event event : extractedEvents) {
      Long timeIndex = cassStream.makeTimeIndex(event.getIngestTimestamp().getTime());
      Set<List<Object>> indexes = totalIndexes.computeIfAbsent(timeIndex, k -> new HashSet<>());
      Object[] keys = new Object[viewDesc.getGroupBy().size() + 1];
      keys[0] = timeIndex;
      for (int i = 0; i < viewDesc.getGroupBy().size(); ++i) {
        keys[i + 1] = event.get(viewDesc.getGroupBy().get(i));
      }
      indexes.add(List.of(keys));
    }
    return totalIndexes;
  }
}
