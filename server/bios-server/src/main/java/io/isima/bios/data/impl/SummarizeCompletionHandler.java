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

import io.isima.bios.models.Event;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Class to handle multiple summarize executions.
 *
 * <p>When you run multiple summarize executors, create their acceptors using method {@link
 * #makeExecutorAcceptor()} for each. These acceptors put their results to this class concurrently,
 * the last acceptor collects the results to make a single replying result.
 */
public class SummarizeCompletionHandler {
  private final Consumer<Map<Long, List<Event>>> masterAcceptor;
  private final AtomicInteger remaining;
  private Map<Long, List<Event>>[] results = null;

  /**
   * The constructor of this class.
   *
   * @param masterAcceptor Acceptor used for replying to the operation request
   */
  public SummarizeCompletionHandler(Consumer<Map<Long, List<Event>>> masterAcceptor) {
    this.masterAcceptor = masterAcceptor;
    remaining = new AtomicInteger(0);
  }

  /**
   * Makes an acceptor for a query executor.
   *
   * @return New acceptor
   */
  public Consumer<Map<Long, List<Event>>> makeExecutorAcceptor() {
    return new ExecutorAcceptor();
  }

  /**
   * Prepares for running query executors.
   *
   * <p>You must call this method before running the executors.
   */
  public void prepare() {
    results = new Map[remaining.get()];
  }

  class ExecutorAcceptor implements Consumer<Map<Long, List<Event>>> {
    private final int index;

    public ExecutorAcceptor() {
      index = remaining.get();
      remaining.incrementAndGet();
    }

    @Override
    public void accept(Map<Long, List<Event>> result) {
      results[index] = result;
      if (remaining.decrementAndGet() == 0) {
        masterAcceptor.accept(collectResults());
      }
    }

    /**
     * Method to collect all results when all corresponding executors complete.
     *
     * <p>NOTE: The order of results matters in this method. When multiple results have non-empty
     * events with the same timestamp, the latter wins. This behavior is necessary to override the
     * results by on-the-fly summarize query at the end edge of the results. So in case of
     * on-the-fly summarize, the end-edge query has to be placed at the highest index.
     */
    private Map<Long, List<Event>> collectResults() {
      final var masterResult = new TreeMap<Long, List<Event>>();
      for (var result : results) {
        result.forEach(
            (timestamp, events) -> {
              // override the results only when the events are non-empty
              if (events.isEmpty()) {
                masterResult.putIfAbsent(timestamp, events);
              } else {
                masterResult.put(timestamp, events);
              }
            });
      }
      return masterResult;
    }
  }
}
