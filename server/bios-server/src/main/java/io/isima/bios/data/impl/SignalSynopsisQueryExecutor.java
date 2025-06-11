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

import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.common.EventFactory;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.QueryLogger;
import io.isima.bios.models.ComplexQuerySingleResponse;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SignalSynopsisQueryExecutor extends ComplexQueryExecutor {
  // static final Logger logger = LoggerFactory.getLogger(SignalSynopsis.class);

  // Counts of events for the requested time windows.
  private static final int RESPONSE_COUNTS = 0;
  // Count of events for one large cyclical time window before the given windows.
  private static final int RESPONSE_PREV_COUNT = 1;
  // List of attributes, including enriched (additional) attributes.
  private static final int RESPONSE_ATTRIBUTES = 2;
  private static final int TOTAL_RESPONSES = 3;

  private static final String ATTRIBUTE_NAME = "attributeName";
  private static final String ATTRIBUTE_TYPE = "attributeType";
  private static final String ATTRIBUTE_ORIGIN = "attributeOrigin";
  private static final String ATTRIBUTE_ORIGIN_ORIGINAL = "Original";
  private static final String ATTRIBUTE_ORIGIN_ENRICHED = "Enriched";

  public SignalSynopsisQueryExecutor(
      DataEngine dataEngine,
      ComplexQueryState state,
      Consumer<ComplexQuerySingleResponse[]> acceptor,
      Consumer<Throwable> errorHandler) {
    super(dataEngine, state, acceptor, errorHandler, TOTAL_RESPONSES);
  }

  @Override
  public void executeAsyncCore() {

    state.addHistory("(signalSynopsis{");
    var tempQueryLoggerItems = QueryLogger.newTempItems(3);

    // Add counts for the small original windows.
    final var countRequest = createSummarizeForOriginalPeriod(state.getInput().getInterval());
    countRequest.setAggregates(List.of(new Aggregate(MetricFunction.COUNT, null)));
    final var countFuture =
        runAsync("counts", RESPONSE_COUNTS, true, countRequest, tempQueryLoggerItems.get(0), state);

    // Add counts for the full previous (cyclical) window.
    final var prevCountRequest = createSummarizeForPreviousPeriod(w1);
    prevCountRequest.setAggregates(List.of(new Aggregate(MetricFunction.COUNT, null)));
    final var prevCountFuture =
        runAsync(
            "prevCount",
            RESPONSE_PREV_COUNT,
            false,
            prevCountRequest,
            tempQueryLoggerItems.get(1),
            state);

    // Add the list of all attributes of this signal, including additional (enriched) attributes.
    final var attributesResponse = new HashMap<Long, List<Event>>();
    final List<Event> eventList = new ArrayList<>();
    attributesResponse.put(0L, eventList);
    responsesArray[RESPONSE_ATTRIBUTES] = new ComplexQuerySingleResponse(attributesResponse, false);
    final var attributeEventFactory = state.generateEventFactory();

    addAttributes(
        eventList,
        attributeEventFactory,
        state.getStreamDesc().getAttributes(),
        ATTRIBUTE_ORIGIN_ORIGINAL);
    addAttributes(
        eventList,
        attributeEventFactory,
        state.getStreamDesc().getAdditionalAttributes(),
        ATTRIBUTE_ORIGIN_ENRICHED);
    tempQueryLoggerItems.get(2).addPostDb(0, 1, eventList.size());
    // The operation never completes here since previous two runAsync run after this method, but
    // we need to increase completion count here.
    incrementAndCheckCompletion();

    // Update the query logging metrics.
    CompletableFuture.allOf(countFuture, prevCountFuture)
        .thenRunAsync(
            () -> {
              state
                  .getQueryLoggerItem()
                  .ifPresent((item) -> item.incorporateTempItems(tempQueryLoggerItems));
            },
            state.getExecutor())
        .exceptionally(
            (t) -> {
              errorHandler.accept(t);
              return null;
            });
  }

  private static void addAttributes(
      List<Event> eventList,
      EventFactory eventFactory,
      List<AttributeDesc> attributes,
      String attributeOrigin) {
    if (attributes != null) {
      for (final var attribute : attributes) {
        final var event = eventFactory.create();
        event.set(ATTRIBUTE_NAME, attribute.getName());
        event.set(ATTRIBUTE_TYPE, attribute.getAttributeType().getBiosAttributeType().stringify());
        event.set(ATTRIBUTE_ORIGIN, attributeOrigin);
        eventList.add(event);
      }
    }
  }
}
