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
package io.isima.bios.data.impl.storage;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.isima.bios.framework.BiosModules;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to help executing asynchronous queries.
 *
 * <p>The class executes queued queries asynchronously with concurrency up to specified parallelism.
 *
 * <p>The stage's {@link AsyncQueryStage#handleResult(ResultSet result)} method is invoked on
 * successful query for each stage. Then next enqueued stage is executed up to allowed concurrency
 * if any.
 *
 * <p>If a stage failed to execute, the stage's {@link AsyncQueryStage#handleError(Throwable t)} is
 * invoked. No more stage would be executed in the case. Also handling of all ongoing queries are
 * canceled, assuming the first error handler concludes the execution.
 *
 * <p>Use method {@link #addStage(AsyncQueryStage)} to enqueue query stages. The method {@link
 * #execute()} must be invoked in order to start the execution.
 */
public class AsyncQueryExecutor {
  private final Queue<AsyncQueryStage> processStages;
  private final Session session;
  private final Executor executor;
  private final int maxConcurrency;
  private final AtomicInteger concurrency = new AtomicInteger();
  private volatile boolean started;
  private volatile boolean canceled;

  /**
   * Constructor of the {@link AsyncQueryExecutor}.
   *
   * @param session Cassandra session
   * @param maxConcurrency Maximum number of allowed concurrency
   */
  public AsyncQueryExecutor(Session session, int maxConcurrency, Executor executor) {
    processStages = new ConcurrentLinkedQueue<>();
    this.session = session;
    this.executor = executor;
    this.maxConcurrency = maxConcurrency;
    started = false;
    canceled = false;
  }

  /**
   * Method to add a query stage.
   *
   * @param stage The stage
   */
  public void addStage(AsyncQueryStage stage) {
    processStages.offer(stage);
  }

  /** Returns number of remaining queued stages. */
  public int getNumRemainingStages() {
    return processStages.size();
  }

  /**
   * Starts the execution.
   *
   * <p>This method must be called after queuing all query stages.
   */
  public void execute() {
    if (started) {
      throw new IllegalStateException("The instance may not be reused");
    }
    started = true;
    int numStages = Math.min(processStages.size(), maxConcurrency);
    concurrency.set(numStages);
    AsyncQueryStage[] stages = new AsyncQueryStage[numStages];
    for (int i = 0; i < numStages; i++) {
      stages[i] = processStages.poll();
    }
    for (int i = 0; i < numStages; i++) {
      executeStage(stages[i]);
    }
  }

  private void executeStage(AsyncQueryStage stage) {
    if (stage == null) {
      return;
    }
    final Statement statement = stage.getStatement();
    if (statement == null) {
      throw new NullPointerException(
          String.format(
              "%s.getStatement may not return null. Check the implementation of class %s",
              AsyncQueryStage.class.getSimpleName(), stage.getClass().getSimpleName()));
    }
    stage.beforeQuery();

    // TODO(Naoki): Use paging in handling results to save amount of memory
    ListenableFuture<ResultSet> future =
        Futures.transformAsync(session.executeAsync(statement), iterate(executor), executor);

    Futures.addCallback(
        future,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ResultSet result) {
            if (canceled) {
              return;
            }
            stage.handleResult(result);

            // try executing available stages
            final AsyncQueryStage nextStage = processStages.poll();
            if (nextStage != null) {
              executeStage(nextStage);
            } else {
              int current = concurrency.get();
              while (!concurrency.compareAndSet(current, current - 1)) {
                current = concurrency.get();
              }
              if (current == 1) {
                stage.handleCompletion();
              }
            }
          }

          @Override
          public void onFailure(Throwable t) {
            if (canceled) {
              // we won't handle second errors.
              return;
            }
            canceled = true;
            stage.handleError(t);
          }
        },
        executor);
  }

  private AsyncFunction<ResultSet, ResultSet> iterate(Executor executor) {
    return new AsyncFunction<>() {
      @Override
      public ListenableFuture<ResultSet> apply(ResultSet rows) {

        boolean wasLastPage = rows.getExecutionInfo().getPagingState() == null;
        if (wasLastPage) {
          return Futures.immediateFuture(rows);
        }
        ListenableFuture<ResultSet> future = rows.fetchMoreResults();
        return Futures.transformAsync(future, iterate(executor), executor);
      }
    };
  }

  public void retry(AsyncQueryStage stage, long backoffMillis) {
    if (backoffMillis < 0) {
      throw new IllegalArgumentException("backoffMillis value may not be negative");
    }
    BiosModules.getDigestor()
        .schedule(() -> executeStage(stage), backoffMillis, TimeUnit.MILLISECONDS);
  }
}
