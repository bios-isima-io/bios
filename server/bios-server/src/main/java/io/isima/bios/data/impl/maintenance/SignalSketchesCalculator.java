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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.Range;
import io.isima.bios.utils.StringUtils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalSketchesCalculator {
  private static final Logger logger = LoggerFactory.getLogger(SignalSketchesCalculator.class);

  private final DataEngineMaintenance dataEngineMaintenance;
  private final PostProcessScheduler postProcessScheduler;
  private final SketchStore sketchStore;

  public SignalSketchesCalculator(
      DataEngineMaintenance dataEngineMaintenance,
      PostProcessScheduler postProcessScheduler,
      SketchStore sketchStore) {
    this.dataEngineMaintenance = dataEngineMaintenance;
    this.postProcessScheduler = postProcessScheduler;
    this.sketchStore = sketchStore;
  }

  public CompletableFuture<Boolean> execute(
      List<Event> extractedEvents,
      StreamDesc streamDesc,
      List<DataSketchSpecifier> sketchSpecifiers,
      List<DigestSpecifier> indexingSpecifiers,
      DigestState state) {

    if ((sketchSpecifiers == null) || sketchSpecifiers.isEmpty()) {
      return CompletableFuture.completedFuture(true);
    }

    state.addHistory(")(digestSketches");

    final SketchGroup sketchGroup = new SketchGroup(streamDesc, sketchSpecifiers);

    final long indexStartTime =
        indexingSpecifiers.isEmpty() ? 0 : indexingSpecifiers.get(0).getStartTime();
    final long indexEndTime =
        indexingSpecifiers.isEmpty() ? 0 : indexingSpecifiers.get(0).getEndTime();
    final var indexRange = new Range(indexStartTime, indexEndTime);
    final var tenantName = streamDesc.getParent().getName();
    final var signalName = streamDesc.getName();
    logger.debug(
        "DigestSketches: have events for tenant={}, signal={}, index range=[{} + {}]",
        tenantName,
        signalName,
        StringUtils.tsToIso8601(indexStartTime),
        StringUtils.shortReadableDuration(indexEndTime - indexStartTime));

    // Check the latest signal config before we go. Cancel the task if current signal is stale
    final var latestSignal = BiosModules.getAdminInternal().getStreamOrNull(tenantName, signalName);
    if (latestSignal == null
        || (!latestSignal.getSchemaVersion().equals(streamDesc.getSchemaVersion())
            && indexStartTime >= latestSignal.getSchemaVersion())) {
      logger.warn(
          "The signal is stale, canceling to calculate data sketches;"
              + " tenant={}, signal={}, version={}, schemaVersion={}",
          tenantName,
          signalName,
          streamDesc.getVersion(),
          streamDesc.getSchemaVersion());
      return CompletableFuture.completedFuture(false);
    }

    return sketchGroup
        .updateSketchesAsync(
            String.format("tenant=%s, signal=%s, ", tenantName, signalName),
            (range, updateState) -> {
              // If we don't have all the events necessary, get them now.
              if (indexRange.includes(range)) {
                logger.debug("DigestSketches: time range needed is covered by index range");
                return CompletableFuture.completedFuture(extractedEvents);
              }
              state.addHistory("(updateSketch");
              logger.debug(
                  "DigestSketches: time range needed is different from index: [{} + {}]",
                  StringUtils.tsToIso8601(range.getBegin()),
                  StringUtils.shortReadableDuration(range.getEnd() - range.getBegin()));
              state.addHistory("(fetchEvents");
              return CompletableFuture.completedFuture(
                  DataMaintenanceUtils.getEventsInRange(
                      extractedEvents, range.getBegin(), range.getEnd()));
            },
            state)
        .thenComposeAsync(
            (none) -> {
              return sketchGroup
                  .writeSketches(sketchStore, postProcessScheduler, state)
                  .thenApply(
                      (x) -> {
                        state.addHistory(")");
                        return true;
                      });
            },
            state.getExecutor());
  }
}
