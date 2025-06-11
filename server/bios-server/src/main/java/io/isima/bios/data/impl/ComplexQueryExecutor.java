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

import static io.isima.bios.admin.v1.impl.AdminImpl.DEFAULT_SKETCHES_INTERVAL_DEFAULT;
import static io.isima.bios.admin.v1.impl.AdminImpl.DEFAULT_SKETCHES_INTERVAL_KEY;

import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.ComplexQueryRequest;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.SummarizeRequest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Abstract class to be inherited by classes that implement complex queries. */
public abstract class ComplexQueryExecutor {
  // static final Logger logger = LoggerFactory.getLogger(ComplexQueryExecutor.class);

  protected final DataEngine dataEngine;
  protected final ComplexQueryState state;
  protected final Consumer<ComplexQuerySingleResponse[]> acceptor;
  protected final Consumer<Throwable> errorHandler;
  protected final int totalResponses;

  protected final AtomicInteger responsesCompleted = new AtomicInteger(0);
  protected ComplexQuerySingleResponse[] responsesArray;
  long w1;
  long w2;
  long w3;

  public ComplexQueryExecutor(
      DataEngine dataEngine,
      ComplexQueryState state,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler,
      int totalResponses) {
    this.dataEngine = dataEngine;
    this.state = state;
    this.acceptor = acceptor;
    this.errorHandler = errorHandler;
    this.totalResponses = totalResponses;
    responsesArray = new ComplexQuerySingleResponse[totalResponses];
  }

  /**
   * Asynchronously executes the complex query request; acceptor and/or errorhandler are called when
   * completed.
   */
  public void executeAsync() {
    // There are 3 windows sizes involved here:
    // w1: the full period of the original query for this synopsis, e.g. 1 day.
    // w2: the window size of the original query for this synopsis, e.g. 1 hour.
    // w3: smallest granularity at which we have data sketches available, e.g. 5 minutes.
    BiosModules.getSharedProperties()
        .getPropertyCachedLongAsync(
            DEFAULT_SKETCHES_INTERVAL_KEY,
            DEFAULT_SKETCHES_INTERVAL_DEFAULT,
            state,
            (sketchInterval) -> {
              w1 = state.getInput().getEndTime() - state.getInput().getStartTime();
              w2 = state.getInput().getInterval();
              w3 = Math.min(sketchInterval, w2);
              executeAsyncCore();
            },
            errorHandler::accept);
  }

  public abstract void executeAsyncCore();

  protected SummarizeRequest createSummarizeForOriginalPeriod(long interval) {
    final ComplexQueryRequest request = state.getInput();
    final var newRequest = new SummarizeRequest();
    newRequest.setStartTime(request.getStartTime());
    newRequest.setEndTime(request.getEndTime());
    newRequest.setInterval(interval);
    newRequest.setSnappedStartTime(request.getSnappedStartTime());
    populateSummarizeCommonParts(newRequest);
    return newRequest;
  }

  protected SummarizeRequest createSummarizeForPreviousPeriod(long interval) {
    final ComplexQueryRequest request = state.getInput();
    final var newRequest = new SummarizeRequest();
    final long duration = request.getEndTime() - request.getStartTime();
    newRequest.setStartTime(request.getStartTime() - duration);
    newRequest.setEndTime(request.getStartTime());
    newRequest.setInterval(interval);
    newRequest.setSnappedStartTime(request.getSnappedStartTime() - duration);
    populateSummarizeCommonParts(newRequest);
    return newRequest;
  }

  protected void populateSummarizeCommonParts(SummarizeRequest newRequest) {
    final ComplexQueryRequest request = state.getInput();
    newRequest.setHorizon(newRequest.getInterval());
    newRequest.setTimezone(request.getTimezone());
  }

  protected SummarizeState createSummarizeState(
      String executionName, SummarizeRequest summarizeRequest, ExecutionState parent) {
    final var summarizeState =
        new SummarizeState(
            executionName,
            state.getTenantName(),
            state.getStreamName(),
            state.generateEventFactory(),
            parent);
    // summarizeState.setDataEngine(dataEngine);
    summarizeState.setStreamDesc(state.getStreamDesc());
    summarizeState.setInput(summarizeRequest);
    summarizeState.setValidated(true);
    return summarizeState;
  }

  /** Executes a summarize request asynchronously. */
  protected CompletableFuture<Void> runAsync(
      String executionName,
      int resultNum,
      boolean isWindowedResponse,
      SummarizeRequest summarizeRequest,
      QueryLogger.Item tempQueryLoggerItem,
      ExecutionState parent) {
    final var summarizeState = createSummarizeState(executionName, summarizeRequest, parent);
    summarizeState.setQueryLoggerItem(tempQueryLoggerItem);

    return dataEngine
        .summarize(summarizeState)
        .thenAccept(
            (resultMap) -> {
              responsesArray[resultNum] =
                  new ComplexQuerySingleResponse(resultMap, isWindowedResponse);
              summarizeState.addHistory("}");
              summarizeState.markDone();
              incrementAndCheckCompletion();
              state.addHistory(")})");
            })
        .toCompletableFuture()
        .exceptionally(
            (t) -> {
              errorHandler.accept(t);
              return null;
            });
  }

  protected void incrementAndCheckCompletion() {
    int completed = responsesCompleted.incrementAndGet();
    if (completed == totalResponses) {
      acceptor.accept(responsesArray);
    }
  }
}
