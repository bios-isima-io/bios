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
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.storage.AsyncQueryStage;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.QueryInfo;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Event;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * Generic implementation of AsyncQueryStage for extracting events from a signal stream.
 *
 * <p>The class provides framework to request and handle a query against a signal table. A class
 * instance converts query response to an {@link Event} and adds it to the given event list.
 *
 * <p>The class assumes multiple stages to complete the query and tracks number of remaining stages
 * to handle. When the last stage finishes to handle the result, abstract method {@link
 * #finishExecution} is invoked. The method is meant to process collected events in eventsList and
 * returns the response.
 *
 * @param <S> ExecutionState type for the class.
 * @param <R> Operation response type.
 */
abstract class SignalQueryStage<S extends QueryExecutionState, R> implements AsyncQueryStage {
  protected final Statement statement;
  protected final S state;
  protected final CassStream cassStream;
  protected final SignalCassStream cassStreamForDecoding;
  protected final Collection<CassAttributeDesc> attributes;
  protected final boolean noEventIdColumn;

  protected final List<Event>[] eventsList;
  protected final List<Event> events;

  protected final int maxDataPoints;
  protected final AtomicInteger totalDataPoints;
  protected final AtomicBoolean canceled;

  protected final Consumer<R> acceptor;
  protected final Consumer<Throwable> errorHandler;

  @Getter protected final int index;

  public SignalQueryStage(
      S state,
      QueryInfo queryInfo,
      CassStream cassStream,
      boolean noEventIdColumn,
      List<Event>[] eventsList,
      AtomicInteger totalDataPoints,
      AtomicBoolean canceled,
      Consumer<R> acceptor,
      Consumer<Throwable> errorHandler,
      int index) {
    if (state == null || cassStream == null || acceptor == null || errorHandler == null) {
      throw new IllegalArgumentException(
          "Any of SignalQueryStage constructor parameters may not be null");
    }
    this.state = state;
    this.statement = queryInfo.getStatement();
    this.cassStream = cassStream;
    this.cassStreamForDecoding = queryInfo.getCassStream();
    this.attributes = queryInfo.getAttributes();
    this.noEventIdColumn = noEventIdColumn;

    this.eventsList = eventsList;

    this.maxDataPoints = state.isDataLengthLimited() ? TfosConfig.selectMaxNumDataPoints() : 0;
    this.totalDataPoints = totalDataPoints;
    this.canceled = canceled;

    this.acceptor = acceptor;
    this.errorHandler = errorHandler;

    this.events = new ArrayList<>();
    this.eventsList[index] = events;

    this.index = index;
  }

  @Override
  public Statement getStatement() {
    return statement;
  }

  @Override
  public void handleResult(ResultSet result) {
    try {
      int count = 0;
      while (!result.isExhausted() && !canceled.get()) {
        if (++count == 1000) { // check data points count every 1000-points
          checkLimits(count);
          count = 0;
        }
        final Row row = result.one();
        acceptRow(row);
      }
      resultReceived();
      checkLimits(count);
    } catch (Throwable t) {
      canceled.set(true);
      errorHandler.accept(t);
    }
  }

  protected void acceptRow(Row row) throws ApplicationException {
    final Event event =
        cassStreamForDecoding.buildEvent(
            row, attributes, noEventIdColumn, state.getEventFactory(), null, null);
    if (event != null) {
      events.add(event);
    }
  }

  protected void checkLimits(int count) throws TfosException {
    if (count <= 0) {
      return;
    }
    if (maxDataPoints > 0 && totalDataPoints.addAndGet(count) > maxDataPoints) {
      throw new TfosException(
          GenericError.QUERY_SCALE_TOO_LARGE,
          "Select operation exceeds its size limitation. Please downsize the query."
              + " stream="
              + cassStream.getStreamName()
              + ", totalDataPoints="
              + totalDataPoints
              + ", count="
              + count
              + ".");
    }
  }

  protected void resultReceived() {}

  protected abstract R finishExecution(List<Event> totalEvents)
      throws TfosException, ApplicationException;

  protected boolean isForRollup() {
    return false;
  }

  /** Meter external raw reads. */
  protected void meterRawReads(long recordsReturned) {
    state.addRecordsRead(recordsReturned);
  }

  @Override
  public void handleError(Throwable error) {
    // we should record read records even on error
    meterRawReads(0);
    canceled.set(true);
    errorHandler.accept(DataUtils.handleExtractError(error, statement));
  }

  @Override
  public void handleCompletion() {
    try {
      state.endDbAccess();
      // No more stages are left. Finish the execution.
      final List<Event> totalEvents = new ArrayList<>();
      for (var events : eventsList) {
        totalEvents.addAll(events);
      }
      acceptor.accept(finishExecution(totalEvents));
    } catch (Throwable t) {
      errorHandler.accept(t);
    }
  }
}
