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

import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.impl.storage.AsyncQueryExecutor;
import io.isima.bios.data.impl.storage.AsyncQueryStage;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.IndexCassStream;
import io.isima.bios.data.impl.storage.QueryInfo;
import io.isima.bios.data.impl.storage.StorageDataUtils;
import io.isima.bios.data.impl.storage.ViewCassStream;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.StringUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Asynchronous execution stage for index-based queries.
 *
 * <p>This stage is meant to handle extractions from an index table. The {@link #handleResult}
 * method receives available dimensions in the time indexes in the range, then builds query
 * statements for the corresponding view table. {@link #handleCompletion} triggers queries of the
 * next stage to the query view table.
 */
class IndexSummarizeStage implements AsyncQueryStage {
  private final SummarizeState state;
  private final IndexCassStream cassIndex;
  private final Statement statement;
  private final Collection<CassAttributeDesc> attributes;
  private final ViewCassStream cassView;
  private final AsyncQueryExecutor nextExecutor;
  private final Queue<SummarizeViewQueryInfo> viewQueries;
  private final Consumer<Map<Long, List<Event>>> acceptor;
  private final Consumer<Throwable> errorHandler;
  private final Map<Long, Map<List<Object>, Event>> totalViewEntries;
  private final int index;

  /**
   * The constructor.
   *
   * @param state Summarize execution state
   * @param queryInfo query info for this stage
   * @param cassView CassStream for the corresponding view table
   * @param nextExecutor Async query executor used for the next step queries
   * @param viewQueries Queue to accumulate the queries for the view table
   * @param acceptor
   * @param errorHandler
   */
  public IndexSummarizeStage(
      SummarizeState state,
      QueryInfo queryInfo,
      ViewCassStream cassView,
      AsyncQueryExecutor nextExecutor,
      Queue<SummarizeViewQueryInfo> viewQueries,
      Map<Long, Map<List<Object>, Event>> totalViewEntries,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler,
      int index) {
    if (queryInfo.getCassStream().getStreamDesc().getType() != StreamType.INDEX) {
      throw new IllegalArgumentException("Stream type must be INDEX");
    }
    this.state = state;
    this.cassIndex = (IndexCassStream) queryInfo.getCassStream();
    this.statement = queryInfo.getStatement();
    this.attributes = queryInfo.getAttributes();
    this.cassView = cassView;
    this.nextExecutor = nextExecutor;
    this.viewQueries = viewQueries;
    this.totalViewEntries = totalViewEntries;
    this.acceptor = acceptor;
    this.errorHandler = errorHandler;
    this.index = index;
  }

  @Override
  public Statement getStatement() {
    return statement;
  }

  @Override
  public void beforeQuery() {
    state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
  }

  /**
   * Index query result handler.
   *
   * <p>A row in the result consists of time index and dimensions. The method builds a partition key
   * for the view table using the values, and builds a query statement.
   */
  @Override
  public void handleResult(ResultSet result) {
    state.addHistory("makeViewQueryStatements");
    state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, 0));

    List<String> partitionKeyColumns = new ArrayList<>();
    cassIndex
        .getAttributeTable()
        .entrySet()
        .forEach(entry -> partitionKeyColumns.add(entry.getValue().getColumn()));

    final long interval = state.getInput().getInterval().longValue();
    final long reqStart = (((state.getInput().getStartTime() - 1) / interval) + 1) * interval;
    final long reqEnd = state.getInput().getEndTime();
    final List<Aggregate> aggregates = state.getInput().getAggregates();

    int numRows = 0;
    while (!result.isExhausted()) {
      ++numRows;
      Row row = result.one();
      // build partition keys for ingest view table
      List<Object> partitionKeys = new ArrayList<>();
      partitionKeys.add(row.getObject(COL_TIME_INDEX));
      partitionKeyColumns.forEach(name -> partitionKeys.add(row.getObject(name)));
      for (long start = reqStart; start < reqEnd; start += interval) {
        final long end = start + interval;
        final Statement statement =
            cassView.makeGroupViewExtractStatement(
                partitionKeyColumns,
                partitionKeys,
                Collections.emptyList(),
                aggregates,
                start,
                end);
        if (statement != null) {
          viewQueries.offer(new SummarizeViewQueryInfo(statement, start + interval));
        }
      }
    }

    SignalSummarizer.logger.debug(
        "From index={} rows={} fetched, range[{} - {}]",
        cassIndex.getStreamName(),
        numRows,
        StringUtils.tsToIso8601(reqStart),
        StringUtils.tsToIso8601(reqEnd));
    final int finalNumRows = numRows;
    state.getQueryLoggerItem().ifPresent((item) -> item.addDbRowsRead(finalNumRows));
  }

  @Override
  public void handleError(Throwable error) {
    errorHandler.accept(DataUtils.handleExtractError(error, statement));
  }

  /**
   * This is invoked when all index query executions are completed.
   *
   * <p>All necessary queries are accumulated to the {@link #viewQueries} when this method is
   * invoked. The method, then, starts the queries of the next stage.
   */
  @Override
  public void handleCompletion() {
    if (viewQueries.isEmpty()) {
      var startPostDbTime = Instant.now();
      final var finalResults = convertTotalViewEntriesToResult(totalViewEntries);
      final var numRecords = finalResults.values().stream().mapToLong(List::size).sum();
      var timeTakenMicros = Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
      state
          .getQueryLoggerItem()
          .ifPresent((item) -> item.addPostDb(timeTakenMicros, finalResults.size(), numRecords));
      acceptor.accept(finalResults);
      return;
    }
    final List<String> dimensions = state.getInput().getGroup();
    final List<Aggregate> aggregates = state.getInput().getAggregates();
    // Don't start from 0 to avoid conflict with previously used numbers.
    int queryIteration = 100000000;
    for (var viewQuery : viewQueries) {
      final int index = queryIteration++;
      nextExecutor.addStage(
          new AsyncQueryStage() {
            @Override
            public Statement getStatement() {
              return viewQuery.getStatement();
            }

            @Override
            public void beforeQuery() {
              state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
            }

            /**
             * Result handler for the view table.
             *
             * <p>The results come as aggregated values for the specified dimension. We pick up the
             * timestamp from the query info as the key for the values in the result map. That
             * timestamp denotes the beginning of the summarize interval.
             *
             * @param result Query result
             */
            @Override
            public void handleResult(ResultSet result) {
              state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, 0));
              long numDbRows = 0;
              while (!result.isExhausted()) {
                final Row row = result.one();
                numDbRows++;
                final Long timestamp = viewQuery.getTimestamp();
                Map<List<Object>, Event> viewEntries = totalViewEntries.get(timestamp);
                cassView.handleGroupViewQueryRow(
                    viewEntries,
                    dimensions,
                    row,
                    Collections.emptyList(),
                    aggregates,
                    state,
                    false);
              }
              final long finalNumDbRows = numDbRows;
              state.getQueryLoggerItem().ifPresent((item) -> item.addDbRowsRead(finalNumDbRows));
            }

            @Override
            public void handleError(Throwable t) {
              errorHandler.accept(t);
            }

            /**
             * This method completes the summarize operation.
             *
             * <p>The method transforms the accumulated result to the output object and push it to
             * the acceptor.
             */
            @Override
            public void handleCompletion() {
              var startPostDbTime = Instant.now();
              state.endDbAccess();
              final var finalResults = convertTotalViewEntriesToResult(totalViewEntries);
              final var numRecords = finalResults.values().stream().mapToLong(List::size).sum();
              var timeTakenMicros =
                  Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
              state
                  .getQueryLoggerItem()
                  .ifPresent(
                      (item) -> item.addPostDb(timeTakenMicros, finalResults.size(), numRecords));
              acceptor.accept(finalResults);
            }
          });
    }
    state.startDbAccess();
    nextExecutor.execute();
  }

  /**
   * The method used for converting accumulated totalViewEntries to the output data structure.
   *
   * @param totalViewEntries The accumulated view view entries
   * @return Summarize output
   */
  private Map<Long, List<Event>> convertTotalViewEntriesToResult(
      Map<Long, Map<List<Object>, Event>> totalViewEntries) {
    final Map<Long, List<Event>> result = new TreeMap<>();
    for (Map.Entry<Long, Map<List<Object>, Event>> entry : totalViewEntries.entrySet()) {
      final Long timestamp = entry.getKey() - state.getInput().getInterval();
      final Date timestampAsDate = new Date(timestamp);
      Map<List<Object>, Event> viewEntries = entry.getValue();
      final List<Event> summary =
          viewEntries.values().stream()
              .map(
                  event -> {
                    event.setIngestTimestamp(timestampAsDate);
                    return event;
                  })
              .collect(Collectors.toList());
      StorageDataUtils.convertToOutputData(
          summary, attributes, cassView.getTenantName(), cassView.getStreamName());
      result.put(timestamp, summary);
    }
    return result;
  }
}
