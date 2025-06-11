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
package io.isima.bios.execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to execute asynchronous tasks concurrently. */
public final class ConcurrentExecutionController {
  private static final Logger logger = LoggerFactory.getLogger(ConcurrentExecutionController.class);

  public static CompletableFuture<Void> executeAsync(
      Collection<? extends AsyncExecutionStage<?>> stages, int maxConcurrency) {
    return new ConcurrentExecutionController(
            new ConcurrentLinkedQueue<AsyncExecutionStage<?>>(stages), maxConcurrency)
        .execute();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * ConcurrentExecutionController builder.
   *
   * <p>Note: This class is mutable. Builder objects returned by moeifier methods are the same with
   * original one, the old states would be lost after calling them.
   */
  public static class Builder {
    final ConcurrentLinkedQueue<AsyncExecutionStage<?>> stages;
    int concurrency = 128;

    Builder() {
      stages = new ConcurrentLinkedQueue<>();
    }

    public Builder addStage(AsyncExecutionStage<?> stage) {
      stages.add(stage);
      return this;
    }

    public Builder concurrency(int value) {
      concurrency = value;
      return this;
    }

    public ConcurrentExecutionController build() {
      return new ConcurrentExecutionController(stages, concurrency);
    }
  }

  private final ConcurrentLinkedQueue<AsyncExecutionStage<?>> processStages;
  private final int maxConcurrency;
  private final AtomicInteger concurrency = new AtomicInteger();
  private volatile boolean canceled;
  private final CompletableFuture<Void> completionFuture;

  /**
   * Constructor of the {@link ConcurrentExecutionController}.
   *
   * @param stages Execution stages
   * @param maxConcurrency Maximum number of allowed concurrency
   */
  private ConcurrentExecutionController(
      ConcurrentLinkedQueue<AsyncExecutionStage<?>> stages, int maxConcurrency) {
    this.processStages = stages;
    this.maxConcurrency = maxConcurrency;
    this.canceled = false;
    this.completionFuture = new CompletableFuture<>();
  }

  /**
   * Starts the execution.
   *
   * <p>This method must be called after queuing all query stages.
   */
  public CompletableFuture<Void> execute() {
    if (processStages.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    int numStages = Math.min(processStages.size(), maxConcurrency);
    concurrency.set(numStages);
    final var stages = new ArrayList<AsyncExecutionStage<?>>(numStages);
    for (int i = 0; i < numStages; i++) {
      final var stage = processStages.poll();
      stage.setController(this);
      stages.add(stage);
    }
    for (int i = 0; i < numStages; i++) {
      executeStage(stages.get(i));
    }
    return completionFuture;
  }

  /**
   * Queue an additional stage.
   *
   * <p>This method is meant to be called from one of execution chain completion handler in order to
   * extend the chain.
   *
   * @param stage Additional stage to queue
   */
  public void enqueueStage(AsyncExecutionStage<?> stage) {
    this.processStages.add(stage);
  }

  private void executeStage(AsyncExecutionStage<?> stage) {
    if (stage == null) {
      return;
    }
    final var state = stage.getState();
    stage
        .runAsync()
        .thenRunAsync(handleResult(state), state.getExecutor())
        .exceptionally(handleError(state));
  }

  private Runnable handleResult(ExecutionState stage) {
    return () -> {
      if (canceled) {
        return;
      }
      final var next = processStages.poll();
      if (next != null) {
        next.setController(this);
        executeStage(next);
      } else if (concurrency.decrementAndGet() == 0) {
        completionFuture.complete(null);
      }
    };
  }

  private Function<Throwable, Void> handleError(ExecutionState stage) {
    return (t) -> {
      logger.debug("Error during concurrent multi-async operations", t);
      stage.markError();
      completionFuture.completeExceptionally(
          (t instanceof CompletionException) ? t : new CompletionException(t));
      return null;
    };
  }
}
