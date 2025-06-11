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
import com.ibm.asyncutil.util.Combinators;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.impl.TenantId;
import io.isima.bios.data.impl.sketch.DataSketch;
import io.isima.bios.data.impl.sketch.SketchStore;
import io.isima.bios.data.impl.sketch.SketchSummary;
import io.isima.bios.data.impl.sketch.SketchSummaryKey;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Event;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.StreamType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages in-memory data structures for a group of data sketches, that all belong to one stream.
 * Typically, all the sketches in one SketchGroup object correspond to one time range, but this is
 * not necessarily the case.
 */
@ToString
@Slf4j
public class SketchGroup {

  static class RangeMap extends HashMap<Range, AttributeMap> {
    private static final long serialVersionUID = 5199664819655887678L;
  }

  static class AttributeMap extends HashMap<Short, SketchMap> {
    private static final long serialVersionUID = -5173825352969126940L;
  }

  static class SketchMap extends HashMap<DataSketchType, SketchMapValue> {
    private static final long serialVersionUID = 6882037161295427218L;
  }

  @AllArgsConstructor
  @Getter
  static class SketchMapValue {
    private final DataSketchSpecifier specifier;
    private final DataSketch sketch;
  }

  private final StreamDesc streamDesc;
  private final int streamNameProxy;
  private final TenantId tenantId;
  private final RangeMap rangeMap;

  /**
   * Create in-memory data structures for sketches. Organize them by: 1. Time range, so that an
   * event that does not apply to a range can quickly skip it. 2. Attribute, so that we can process
   * all sketches related to an attribute while it is in L1/L2 cache. 3. Sketch type, so that we do
   * not have any duplicates (i.e. even if an attribute is part of multiple features, we only create
   * a sketch for it once).
   */
  public SketchGroup(StreamDesc streamDesc, List<DataSketchSpecifier> sketchSpecifiers) {

    this.streamDesc = streamDesc;
    streamNameProxy = streamDesc.getStreamNameProxy();
    tenantId = streamDesc.getParent().getId();
    rangeMap = new RangeMap();

    for (var sketchSpec : sketchSpecifiers) {
      final var range = new Range(sketchSpec.getStartTime(), sketchSpec.getEndTime());
      AttributeMap attributeMap = rangeMap.get(range);
      if (attributeMap == null) {
        attributeMap = new AttributeMap();
        rangeMap.put(range, attributeMap);
      }
      final var attributeProxy = sketchSpec.getAttributeProxy();
      SketchMap sketchMap = attributeMap.get(attributeProxy);
      if (sketchMap == null) {
        sketchMap = new SketchMap();
        attributeMap.put(attributeProxy, sketchMap);
      }
      final var sketchType = sketchSpec.getDataSketchType();
      var existingSketchEntry = sketchMap.get(sketchType);
      if (existingSketchEntry == null) {
        final var sketch = DataSketch.createSketch(sketchType, sketchSpec.getAttributeType());
        sketchMap.put(sketchType, new SketchMapValue(sketchSpec, sketch));
      } else {
        final var existingSpec = existingSketchEntry.getSpecifier();
        assert ((existingSpec.getDoneSince() == sketchSpec.getDoneSince())
            && (existingSpec.getDoneUntil() == sketchSpec.getDoneUntil())
            && (existingSpec.getAttributeName().equals(sketchSpec.getAttributeName()))
            && (existingSpec.getAttributeType() == sketchSpec.getAttributeType()));
      }
    }
  }

  /**
   * Loop through every time range, get the events applicable to that range, and get them processed
   * by all sketches in that time range.
   *
   * @param eventsRetriever A function that takes a time range and returns events corresponding to
   *     that time range. It is OK to include events outside the requested range, but all events in
   *     the requested range must be returned.
   */
  public CompletableFuture<Void> updateSketchesAsync(
      String logContext,
      BiFunction<Range, ExecutionState, CompletableFuture<List<Event>>> eventsRetriever,
      ExecutionState parentState) {
    final var state = new GenericExecutionState("updateSketches", parentState);
    return AsyncTrampoline.asyncWhile(
            (iterator) -> iterator.hasNext(),
            (iterator) -> {
              final var rangeMapEntry = iterator.next();
              final Range range = rangeMapEntry.getKey();
              return eventsRetriever
                  .apply(range, state)
                  .thenApplyAsync(
                      (events) -> {
                        for (final var event : events) {
                          if (!range.includes(event.getIngestTimestamp().getTime())) {
                            continue;
                          }
                          for (var attributeMapEntry : rangeMapEntry.getValue().entrySet()) {
                            for (var sketchMapEntry : attributeMapEntry.getValue().entrySet()) {
                              final DataSketch sketch = sketchMapEntry.getValue().getSketch();
                              final DataSketchSpecifier specifier =
                                  sketchMapEntry.getValue().getSpecifier();
                              final Object attributeValue = event.get(specifier.getAttributeName());
                              try {
                                sketch.update(attributeValue);
                              } catch (IllegalArgumentException e) {
                                logger.error(
                                    "Invalid value; {}attribute={}, value={}",
                                    logContext,
                                    attributeValue,
                                    specifier.getAttributeName());
                                logger.info("Problem event: {}", event);
                                sketch.update(0.0);
                              }
                            }
                          }
                        }
                        return iterator;
                      },
                      state.getExecutor());
            },
            rangeMap.entrySet().iterator())
        .thenRun(
            () -> {
              state.markDone();
            })
        .toCompletableFuture();
  }

  /**
   * Loop through every time range, get the events applicable to that range, and get them processed
   * by all sketches in that time range.
   *
   * @param eventsRetriever A function that takes a time range and returns events corresponding to
   *     that time range. It is OK to include events outside the requested range, but all events in
   *     the requested range must be returned.
   */
  @Deprecated
  public void updateSketches(String logContext, Function<Range, List<Event>> eventsRetriever) {
    for (var rangeMapEntry : rangeMap.entrySet()) {
      final Range range = rangeMapEntry.getKey();
      final var events = eventsRetriever.apply(range);
      for (final var event : events) {
        if (!range.includes(event.getIngestTimestamp().getTime())) {
          continue;
        }
        for (var attributeMapEntry : rangeMapEntry.getValue().entrySet()) {
          for (var sketchMapEntry : attributeMapEntry.getValue().entrySet()) {
            final DataSketch sketch = sketchMapEntry.getValue().getSketch();
            final DataSketchSpecifier specifier = sketchMapEntry.getValue().getSpecifier();
            final Object attributeValue = event.get(specifier.getAttributeName());
            try {
              sketch.update(attributeValue);
            } catch (IllegalArgumentException e) {
              logger.error(
                  "Invalid value; {}attribute={}, value={}",
                  logContext,
                  attributeValue,
                  specifier.getAttributeName());
              logger.info("Problem event: {}", event);
              sketch.update(0.0);
            }
          }
        }
      }
    }
  }

  /**
   * Write data sketches to the database, including blobs, summary information, and completion
   * records.
   */
  public CompletableFuture<Void> writeSketches(
      SketchStore sketchStore,
      PostProcessScheduler postProcessScheduler,
      ExecutionState parentState) {
    final var state = new GenericExecutionState("WriteSketches", parentState);
    return AsyncTrampoline.asyncWhile(
            (rangeIterator) -> rangeIterator.hasNext(),
            (rangeIterator) -> {
              final var rangeMapEntry = rangeIterator.next();
              final Range range = rangeMapEntry.getKey();
              final long timeIndex = sketchStore.getSummaryTimeIndex(range.getEnd());
              return AsyncTrampoline.asyncWhile(
                      (iterator) -> iterator.hasNext(),
                      (iterator) -> {
                        final var attributeMapEntry = iterator.next();
                        return handleAttributeMapEntry(
                                attributeMapEntry,
                                range,
                                timeIndex,
                                sketchStore,
                                postProcessScheduler,
                                state)
                            .thenApply((none) -> iterator);
                      },
                      rangeMapEntry.getValue().entrySet().iterator())
                  .thenApply((none) -> rangeIterator);
            },
            rangeMap.entrySet().iterator())
        .thenRun(
            () -> {
              state.markDone();
            })
        .toCompletableFuture();
  }

  private CompletionStage<Void> handleAttributeMapEntry(
      Map.Entry<Short, SketchMap> attributeMapEntry,
      Range range,
      long timeIndex,
      SketchStore sketchStore,
      PostProcessScheduler postProcessScheduler,
      ExecutionState state) {
    final short attributeProxy = attributeMapEntry.getKey();
    final DataSketchDuration duration;
    if (streamDesc.getType() == StreamType.CONTEXT) {
      duration = DataSketchDuration.ALL_TIME;
    } else {
      try {
        duration = DataSketchDuration.fromMillis(range.getWidth());
      } catch (IllegalArgumentException e) {
        logger.error(
            "Invalid feature interval was found. Check configuration;"
                + " tenant={}, {}={}, interval={}",
            streamDesc.getParent().getName(),
            streamDesc.getType() == StreamType.SIGNAL ? "signal" : "context",
            streamDesc.getName(),
            range.getWidth());
        // skip this entry
        return CompletableFuture.completedFuture(null);
      }
    }
    final SketchSummaryKey sketchSummaryKey =
        new SketchSummaryKey(
            tenantId, timeIndex, streamNameProxy, duration, attributeProxy, range.getEnd());
    final SketchSummary sketchSummary = new SketchSummary();
    // Write sketch blobs and collect summary information from each sketch.
    return AsyncTrampoline.asyncWhile(
            (iterator) -> iterator.hasNext(),
            (iterator) -> {
              final var sketchMapEntry = iterator.next();
              final DataSketch sketch = sketchMapEntry.getValue().getSketch();
              final DataSketchType sketchType = sketchMapEntry.getKey();
              sketch.populateSummary(sketchSummary);
              if (sketchType.hasBlob()) {
                return sketchStore
                    .insertBlobAsync(
                        sketchSummaryKey,
                        sketch.getSketchType(),
                        sketch.getCount(),
                        sketch.getHeader(),
                        sketch.getData(),
                        state)
                    .thenApply((none) -> iterator);
              }
              return CompletableFuture.completedFuture(iterator);
            },
            attributeMapEntry.getValue().entrySet().iterator())
        .thenComposeAsync(
            (none) -> sketchStore.insertSummaryAsync(sketchSummaryKey, sketchSummary, state),
            state.getExecutor())
        .thenComposeAsync(
            (none) ->
                AsyncTrampoline.asyncWhile(
                    (iterator) -> iterator.hasNext(),
                    (iterator) -> {
                      final var sketchMapEntry = iterator.next();
                      final DataSketchSpecifier specifier = sketchMapEntry.getSpecifier();
                      return postProcessScheduler
                          .recordCompletion(streamDesc, specifier, state)
                          .thenApply((x) -> iterator);
                    },
                    attributeMapEntry.getValue().values().iterator()),
            state.getExecutor())
        .thenRun(
            () -> {
              // TODO: mark done
            });
  }

  /**
   * Write data sketches to the database, including blobs, summary information, and completion
   * records.
   */
  public CompletableFuture<Void> writeSketches(
      SketchStore sketchStore,
      PostProcessScheduler postProcessScheduler,
      StreamDesc streamDesc,
      ExecutionState state) {
    return Combinators.allOf(
            rangeMap.entrySet().stream()
                .map(
                    (rangeMapEntry) -> {
                      // for (var rangeMapEntry : rangeMap.entrySet()) {
                      final Range range = rangeMapEntry.getKey();
                      final long timeIndex = sketchStore.getSummaryTimeIndex(range.getEnd());
                      return Combinators.allOf(
                          rangeMapEntry.getValue().entrySet().stream()
                              .map(
                                  (attributeMapEntry) -> {
                                    final short attributeProxy = attributeMapEntry.getKey();
                                    final var sketchMap = attributeMapEntry.getValue();
                                    return writeSketchesCore(
                                        sketchStore,
                                        postProcessScheduler,
                                        attributeProxy,
                                        range,
                                        timeIndex,
                                        sketchMap,
                                        streamDesc,
                                        state);
                                  })
                              .collect(Collectors.toList()));
                    })
                .collect(Collectors.toList()))
        .toCompletableFuture();
  }

  private CompletableFuture<Void> writeSketchesCore(
      SketchStore sketchStore,
      PostProcessScheduler postProcessScheduler,
      short attributeProxy,
      Range range,
      long timeIndex,
      SketchMap sketchMap,
      StreamDesc streamDesc,
      ExecutionState state) {
    final DataSketchDuration duration;
    if (streamDesc.getType() == StreamType.CONTEXT) {
      duration = DataSketchDuration.ALL_TIME;
    } else {
      try {
        duration = DataSketchDuration.fromMillis(range.getWidth());
      } catch (IllegalArgumentException e) {
        logger.error(
            "Invalid feature interval was found. Check configuration;"
                + " tenant={}, {}={}, interval={}",
            streamDesc.getParent().getName(),
            streamDesc.getType() == StreamType.SIGNAL ? "signal" : "context",
            streamDesc.getName(),
            range.getWidth());
        // skip this entry
        return CompletableFuture.completedFuture(null);
      }
    }
    final SketchSummaryKey sketchSummaryKey =
        new SketchSummaryKey(
            tenantId, timeIndex, streamNameProxy, duration, attributeProxy, range.getEnd());
    final SketchSummary sketchSummary = new SketchSummary();
    // Write sketch blobs and collect summary information from each sketch.
    return makeSketchBlobs(sketchStore, sketchMap, sketchSummary, sketchSummaryKey, state)
        .thenComposeAsync(
            (none) -> {
              // Write collected summary information for a given range and attributeProxy.
              return sketchStore.insertSummaryAsync(sketchSummaryKey, sketchSummary, state);
            },
            state.getExecutor())
        .thenComposeAsync(
            (none) -> {
              // Write completion records.
              return recordCompletion(postProcessScheduler, sketchMap, state);
            },
            state.getExecutor());
  }

  private CompletableFuture<Void> makeSketchBlobs(
      SketchStore sketchStore,
      SketchMap sketchMap,
      SketchSummary sketchSummary,
      SketchSummaryKey sketchSummaryKey,
      ExecutionState state) {
    state.addHistory("(makeSketchBlobs");
    return AsyncTrampoline.asyncWhile(
            (iterator) -> iterator.hasNext(),
            (iterator) -> {
              final var sketchMapEntry = iterator.next();
              final DataSketch sketch = sketchMapEntry.getValue().getSketch();
              final DataSketchType sketchType = sketchMapEntry.getKey();
              sketch.populateSummary(sketchSummary);
              if (sketchType.hasBlob()) {
                return sketchStore
                    .insertBlobAsync(
                        sketchSummaryKey,
                        sketch.getSketchType(),
                        sketch.getCount(),
                        sketch.getHeader(),
                        sketch.getData(),
                        state)
                    .thenApply((none) -> iterator);
              }
              return CompletableFuture.completedFuture(iterator);
            },
            sketchMap.entrySet().iterator())
        .thenRun(() -> state.addHistory(")"))
        .toCompletableFuture();
  }

  private CompletableFuture<Void> recordCompletion(
      PostProcessScheduler postProcessScheduler, SketchMap sketchMap, ExecutionState state) {
    state.addHistory("(recordCompletion");
    return Combinators.allOf(
            sketchMap.values().stream()
                .map(
                    (sketchMapEntry) -> {
                      final DataSketchSpecifier specifier = sketchMapEntry.getSpecifier();
                      return postProcessScheduler.recordCompletion(streamDesc, specifier, state);
                    })
                .collect(Collectors.toList()))
        .thenRun(() -> state.addHistory(""))
        .toCompletableFuture();
  }
}
