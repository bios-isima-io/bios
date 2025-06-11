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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.common.CompiledAggregate;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.data.impl.sketch.DataSketch;
import io.isima.bios.data.impl.sketch.SingleAttributeSketches;
import io.isima.bios.data.impl.sketch.SketchSummary;
import io.isima.bios.data.impl.sketch.SketchSummaryColumn;
import io.isima.bios.data.impl.storage.AsyncQueryStage;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.InternalAttributeType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;

/**
 * Asynchronous execution stage for sketch queries.
 *
 * <p>Sketch summary table has information about one attribute per row, so we need to stitch
 * together many rows to get the summary for one time window.
 */
abstract class SketchSummarizeStage implements AsyncQueryStage {
  private final ExecutionState state;
  private final List<CompiledAggregate> compiledAggregates;
  protected final Consumer<Throwable> errorHandler;
  private final Set<SketchSummaryColumn> summaryColumns;
  private final Set<DataSketchType> sketchBlobs;
  private final Statement statement;
  private final AtomicInteger totalCount;
  private final AtomicBoolean canceled;
  @Getter protected final int index;

  // for logging
  private final String tenantName;
  private final String streamName;
  private final Object input;
  private final QueryLogger.Item queryLoggerItem;

  private final int maxDataPoints;

  private SketchSummarizeStage(
      ExecutionState state,
      Object input,
      QueryLogger.Item queryLoggerItem,
      List<CompiledAggregate> compiledAggregates,
      Consumer<Throwable> errorHandler,
      Set<SketchSummaryColumn> summaryColumns,
      Set<DataSketchType> sketchBlobs,
      Statement statement,
      AtomicInteger totalCount,
      AtomicBoolean canceled,
      int index) {

    this.state = state;
    this.tenantName = state.getTenantName();
    this.streamName = state.getStreamName();
    this.input = input;
    this.queryLoggerItem = queryLoggerItem;
    this.compiledAggregates = compiledAggregates;
    this.errorHandler = errorHandler;
    this.summaryColumns = summaryColumns;
    this.sketchBlobs = sketchBlobs;
    this.statement = statement;
    this.totalCount = totalCount;
    this.canceled = canceled;
    this.index = index;
    this.maxDataPoints = TfosConfig.selectMaxNumDataPoints();
  }

  @Override
  public Statement getStatement() {
    return statement;
  }

  @Override
  public void beforeQuery() {
    if (queryLoggerItem != null) {
      queryLoggerItem.startDb(index);
    }
  }

  @Override
  public void handleResult(ResultSet result) {
    if (queryLoggerItem != null) {
      queryLoggerItem.endDb(index, 0);
    }
    try {
      int count = 0;
      while (!result.isExhausted() && !canceled.get()) {
        if ((++count % 1000) == 0) { // check data points count every 1000-points
          checkLimits(1000);
        }
        final Row row = result.one();
        acceptRow(row);
      }
      checkLimits(count % 1000);
      final int finalCount = count;
      if (queryLoggerItem != null) {
        queryLoggerItem.addDbRowsRead(finalCount);
      }
    } catch (Throwable t) {
      canceled.set(true);
      errorHandler.accept(t);
    }
  }

  @Override
  public void handleError(Throwable error) {
    canceled.set(true);
    errorHandler.accept(DataUtils.handleExtractError(error, statement));
  }

  /**
   * Method to Accept a row returned by Cassandra.
   *
   * <p>The behavior is different with the type of stream (signal or context).
   */
  protected abstract void acceptRow(Row row);

  /**
   * Incorporates a row into a SingleDurationResults instance.
   *
   * <p>The method first checks the query type of the stage, then parses the row according to the
   * type.
   */
  protected void incorporateRow(
      Row row, long timestamp, SketchesExtractor.SingleDurationResults singleDurationResults) {
    // Find or create a holder for the attribute this row belongs to.
    final short attributeProxy = row.getShort("attribute_proxy");
    final var singleAttributeSketches =
        singleDurationResults.computeIfAbsent(attributeProxy, (k) -> new SingleAttributeSketches());

    // Add all the values obtained from this row.
    // Only one of summaryColumns and sketchBlobs should be non-null, depending on which type
    // of query this stage object corresponds to.
    if (summaryColumns != null) {
      assert (sketchBlobs == null);
      final var sketchSummary = new SketchSummary().incorporateRow(row, summaryColumns);
      singleAttributeSketches.getSummaryMap().put(timestamp, sketchSummary);
    } else {
      assert (sketchBlobs != null);
      // Find or create a holder for this data sketch type.
      final byte sketchTypeProxy = row.getByte("sketch_type");
      final var sketchType = DataSketchType.fromProxy(sketchTypeProxy);
      final var singleTypeSketches =
          singleAttributeSketches
              .getSketchTypesMap()
              .computeIfAbsent(sketchType, k -> new SingleAttributeSketches.SingleTypeSketches());
      // Add the contents of this row to the holder.
      final var count = row.getLong("count");
      final var headerBuffer = row.getBytes("sketch_header");
      final byte[] header = new byte[headerBuffer.remaining()];
      headerBuffer.get(header);
      final var dataBuffer = row.getBytes("sketch_data");
      final byte[] data = new byte[dataBuffer.remaining()];
      dataBuffer.get(data);
      final var endTime = row.getLong("end_time");
      final var sketchContents = new SingleAttributeSketches.RawSketchContents(count, header, data);
      singleTypeSketches.getRawSketchMap().put(timestamp, sketchContents);
    }
  }

  /**
   * Collects data accumulated in SingleDurationResults objects into a list of events.
   *
   * <p>The method iterates pre-compiled aggregates, find the corresponding results object using the
   * given result finder, evaluates the results, then convert the outputs into an attributes or
   * events.
   *
   * @param eventSupplier Supplier that generates an event where the results are collected
   * @param resultFinder Finds the SingleDurationResults corresponding to a compiled aggregate
   * @return Collected result as a list of events. If results are not available, null is returned.
   */
  protected List<Event> collectResult(
      Supplier<Event> eventSupplier,
      Function<CompiledAggregate, SketchesExtractor.SingleDurationResults> resultFinder)
      throws InvalidRequestException {

    final var event = eventSupplier.get();
    // Currently sketches do not implement group by, so there will only be one event
    // per output window.
    final List<Event> eventList = new ArrayList<>();

    Long latestTimestamp = null;
    boolean complexFunctionPresent = false;
    // Loop over every aggregate that needs to be returned, and calculate it.
    for (final var aggregate : compiledAggregates) {
      // Get the summary results necessary to calculate this aggregate.
      // Walk the levels one-by-one and check for existence, because it is possible that
      // some of the sketches (e.g. for some attributes) may be lagging and their query
      // would have returned 0 results. However, outputWindow may contain a bucket for that
      // group because one of the sketches (which was ahead of the others) may have returned
      // results. Even if one of the sketches needed for an output window is missing,
      // skip the whole event because we do not want to return partial results (which can
      // cause a lot of confusion).
      final var singleDurationSketches = resultFinder.apply(aggregate);
      if (singleDurationSketches == null) {
        return null;
      }
      final var singleAttributeSketches = singleDurationSketches.get(aggregate.getAttributeProxy());
      if (singleAttributeSketches == null) {
        return null;
      }
      final var functionResult =
          DataSketch.evaluate(
              aggregate.getFunction(),
              aggregate.getAttributeDesc(),
              singleAttributeSketches,
              state);
      if (functionResult == null) {
        return null;
      }
      if (functionResult.isSimple()) {
        // on-the-fly summarize returns the values of sum, min, and max with the same type of
        // the source attribute. The event encoder cannot handle inconsistent data types, so
        // we convert the return types
        final var function = aggregate.getFunction();
        final Object originalValue = functionResult.getSimpleValue();
        final Object value;
        switch (function) {
          case SUM:
          case MIN:
          case MAX:
            {
              final var attributeType = aggregate.getAttributeDesc().getAttributeType();
              if (attributeType == InternalAttributeType.LONG) {
                value = Long.valueOf((long) (((Double) originalValue).doubleValue() + 0.01));
              } else {
                value = originalValue;
              }
              break;
            }
          default:
            value = originalValue;
        }
        event.set(aggregate.getOutputName(), value);
      } else {
        complexFunctionPresent = true;
        if (compiledAggregates.size() > 1) {
          throw new InvalidRequestException(
              "Complex function %s must be used alone in a " + "query. Found %d functions.",
              aggregate.getOutputName(), compiledAggregates.size());
        }
        // Add an event for each of the rows in the complex result.
        for (final var row : functionResult.getComplexValue().getRows()) {
          final var rowEvent = eventSupplier.get();
          eventList.add(rowEvent);
          int i = 0;
          for (final var outputName : functionResult.getComplexValue().getOutputNames()) {
            rowEvent.set(outputName, row.get(i++));
          }
        }
      }
    }
    if (!complexFunctionPresent) {
      // Complex functions add a list of events above. If we don't have a complex function,
      // add the single event with all function results to the output eventList.
      eventList.add(event);
    }
    return eventList;
  }

  protected void checkLimits(int count) throws TfosException {
    SketchesExtractor.logger.trace("checkLimits: count {}; request: {}", count, input);
    if (count <= 0) {
      return;
    }
    if (maxDataPoints > 0 && totalCount.addAndGet(count) > maxDataPoints) {
      SketchesExtractor.logger.info(
          "Sketch select operation exceeds its size limitation; totalCount={}, count={}",
          totalCount,
          count);
      SketchesExtractor.logger.info("{}", input);
      SketchesExtractor.logger.info("{}", compiledAggregates);
      throw new TfosException(
          GenericError.QUERY_SCALE_TOO_LARGE,
          String.format(
              "Sketch select operation exceeds its size limitation. Please downsize the query;"
                  + " tenant=%s, stream=%s, totalCount=%d",
              tenantName, streamName, totalCount.get()));
    }
  }

  private static Event createEvent(EventFactory factory, Long outputWindowBegin) {
    final Event event = factory.create();
    final UUID eventId = UUIDs.startOf(outputWindowBegin);
    final Date ingestTimestamp = new Date(outputWindowBegin);
    event.setEventId(eventId).setIngestTimestamp(ingestTimestamp);
    return event;
  }

  /** Generates a sketch summaries stage for a signal. */
  public static SketchSummarizeStage forSignal(
      SummarizeState state,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler,
      Set<SketchSummaryColumn> summaryColumns,
      Set<DataSketchType> sketchBlobs,
      Statement statement,
      SketchesExtractor.QueryResults queryResults,
      AtomicInteger totalCount,
      AtomicBoolean canceled,
      int index) {

    return new SketchSummarizeStage(
        state,
        state.getInput(),
        state.getQueryLoggerItem().orElse(null),
        state.getCompiledAggregates(),
        errorHandler,
        summaryColumns,
        sketchBlobs,
        statement,
        totalCount,
        canceled,
        index) {

      @Override
      protected void acceptRow(Row row) {
        // Find or create a holder for the output time window this row belongs to.
        final SummarizeRequest request = state.getInput();
        final long endTime = row.getLong("end_time");
        final byte durationProxy = row.getByte("duration_type");
        final DataSketchDuration duration = DataSketchDuration.fromProxy(durationProxy);
        final Long outputWindowBegin =
            getOutputWindow(endTime, duration, request.getStartTime(), request.getInterval());
        final var outputWindow =
            queryResults.computeIfAbsent(
                outputWindowBegin, k -> new SketchesExtractor.OutputWindow());

        // Find or create a holder for the duration type this row belongs to.
        final var singleDurationResults =
            outputWindow.computeIfAbsent(
                duration, k -> new SketchesExtractor.SingleDurationResults());

        incorporateRow(row, endTime, singleDurationResults);
      }

      /**
       * The output window begin time should in the form (queryStartTime + n * queryInterval). It is
       * not required for queryStartTime itself to be divisible by queryInterval.
       */
      private long getOutputWindow(
          long sketchRowEndTime,
          DataSketchDuration duration,
          long queryStartTime,
          long queryInterval) {
        final long sketchRowBeginTime = sketchRowEndTime - duration.getMilliseconds();
        final long numIntervalsDistanceFromFirstOutputWindow =
            (sketchRowBeginTime - queryStartTime) / queryInterval;
        final long outputWindowBeginTime =
            queryStartTime + queryInterval * numIntervalsDistanceFromFirstOutputWindow;
        return outputWindowBeginTime;
      }

      /**
       * This method summarizes information from multiple input sketch rows and returns the final
       * results for each output time window.
       */
      @Override
      public void handleCompletion() {
        try {
          state.endDbAccess();
          var startPostDbTime = Instant.now();
          SortedMap<Long, List<Event>> finalResults = new TreeMap<>();

          // Loop over every output window for which we have results and process it.
          for (final var outputWindow : queryResults.entrySet()) {
            final Long outputWindowBegin = outputWindow.getKey();

            final var eventList =
                collectResult(
                    () -> createEvent(state.getEventFactory(), outputWindowBegin),
                    (aggregate) -> outputWindow.getValue().get(aggregate.getDuration()));

            if (eventList != null) {
              finalResults.put(outputWindowBegin, eventList);
            }
          }
          final var numRecords = finalResults.values().stream().mapToLong(List::size).sum();
          var timeTakenMicros = Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
          state
              .getQueryLoggerItem()
              .ifPresent(
                  (item) -> item.addPostDb(timeTakenMicros, finalResults.size(), numRecords));
          acceptor.accept(finalResults);
        } catch (Throwable t) {
          errorHandler.accept(t);
        }
      }
    };
  }

  /** Generates a sketch summaries stage for a context. */
  public static SketchSummarizeStage forContext(
      ContextOpState state,
      SelectContextRequest request,
      List<CompiledAggregate> compiledAggregates,
      Set<SketchSummaryColumn> summaryColumns,
      Set<DataSketchType> sketchBlobs,
      Statement statement,
      AtomicInteger totalCount,
      AtomicBoolean canceled,
      int index,
      long queryTime,
      AtomicLong latestTimestamp,
      SketchesExtractor.SingleDurationResults results,
      CompletableFuture<List<Event>> future) {

    return new SketchSummarizeStage(
        state,
        request,
        null,
        compiledAggregates,
        future::completeExceptionally,
        summaryColumns,
        sketchBlobs,
        statement,
        totalCount,
        canceled,
        index) {

      @Override
      protected void acceptRow(Row row) {
        final long endTime = row.getLong("end_time");
        var current = latestTimestamp.get();
        while (current < endTime) {
          if (latestTimestamp.compareAndSet(current, endTime)) {
            break;
          }
          current = latestTimestamp.get();
        }
        incorporateRow(row, queryTime, results);
      }

      @Override
      public void handleCompletion() {
        try {
          var events = collectResult(() -> new EventJson(), (aggregate) -> results);
          if (events == null) {
            events = List.of();
          }
          for (var event : events) {
            event.setIngestTimestamp(new Date(latestTimestamp.get()));
          }
          future.complete(events);
        } catch (Throwable t) {
          errorHandler.accept(t);
        }
      }
    };
  }
}
