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
package io.isima.bios.execution.digestor;

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModule;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Digestor module.
 *
 * <p>This module takes care of server internal task executions. The class has two responsibilities:
 *
 * <ul>
 *   <li>To invoke regular tasks such as data reduction, metrics collection, and maintenance
 *   <li>To provide executors for non time critical tasks such as post storage processes and user
 *       management
 * </ul>
 *
 * <p>In order to create a regular task, make a class that implements {@link Digestion} and register
 * it to this class via the method {@link #register}.
 *
 * <p>To get an executor for an external operation, use the method {@link #getExecutor()}. The
 * returned executor is guaranteed to keep using the same thread.
 */
public class Digestor implements BiosModule {
  private static final Logger logger = LoggerFactory.getLogger(Digestor.class);

  private static final long SHUTDOWN_TIMEOUT_MS = 20000;

  private final EventExecutorGroup executorGroup;
  private final List<DigestionTask> tasks;
  private final List<ScheduledFuture<?>> taskFutures;

  private final AtomicBoolean isActive;

  /**
   * The constructor.
   *
   * @param numThreads Number of threads that the instance manages
   * @param threadFactory Thread factory used for creating threads
   */
  public Digestor(int numThreads, ThreadFactory threadFactory) {
    executorGroup = new DefaultEventExecutorGroup(numThreads, threadFactory);
    tasks = new ArrayList<>();
    taskFutures = new ArrayList<>();
    isActive = new AtomicBoolean(true);
  }

  /**
   * Register a digestion.
   *
   * @param digestion Digestion to register
   */
  public void register(Digestion<?> digestion) {
    final var task = new DigestionTask(digestion);
    taskFutures.add(
        executorGroup.scheduleAtFixedRate(
            task, digestion.getInitialDelay(), digestion.getInterval(), digestion.getTimeUnit()));
    tasks.add(task);
  }

  /**
   * Provide an EventExecutorGroup instance.
   *
   * <p>The provided executor is guaranteed to use the same thread for each execution. The method is
   * meant to be used by a task invoker who wants to run a task in a Digestor thread.
   *
   * @return
   */
  public Executor getExecutor() {
    return executorGroup.next();
  }

  public ExecutionState newExecutionState(String executionName) {
    return new GenericExecutionState(executionName, getExecutor());
  }

  public <T> CompletionStage<T> requestOneTimeTask(
      String taskName, Function<ExecutionState, CompletionStage<T>> task) {
    final var state = newExecutionState(taskName);
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync((none) -> task.apply(state), state.getExecutor());
  }

  public <S extends ExecutionState, T> CompletionStage<T> requestOneTimeTask(
      String taskName,
      Function<Executor, S> stateSupplier,
      Function<ExecutionState, CompletionStage<T>> task) {
    final var state = stateSupplier.apply(getExecutor());
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync((none) -> task.apply(state), state.getExecutor());
  }

  public void schedule(Runnable task, long backoffMillis, TimeUnit timeUnit) {
    executorGroup.schedule(task, backoffMillis, timeUnit);
  }

  @Override
  public void shutdown() {
    // shutdown must happen only once
    if (!isActive.compareAndSet(true, false)) {
      return;
    }
    taskFutures.forEach((fut) -> fut.cancel(false));
    CompletableFuture<?>[] remainingTasks = new CompletableFuture<?>[tasks.size()];

    // We don't interrupt running tasks, but notify the tasks so that they can stop gracefully.
    for (int i = 0; i < tasks.size(); ++i) {
      remainingTasks[i] = tasks.get(i).shutdown();
    }
    try {
      CompletableFuture.allOf(remainingTasks).get(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while shutting down", e);
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      // we ignore this
    }
    try {
      executorGroup.shutdownGracefully(0, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS).sync();
    } catch (InterruptedException e) {
      logger.error("An exception has happened during Digestor executor shutdown", e);
      Thread.currentThread().interrupt();
    }
  }

  private class DigestionTask implements Runnable {
    private final Digestion<?> digestion;
    private final AtomicReference<CompletableFuture<Void>> currentExecution;

    public DigestionTask(Digestion<?> digestion) {
      this.digestion = digestion;
      currentExecution = new AtomicReference<>();
    }

    /**
     * Executes the digestion task.
     *
     * <p>The method is called by a
     */
    @Override
    public void run() {
      if (currentExecution.get() != null) { // meaning that the previous execution is still ongoing
        logger.warn(
            "Previous digestion is still ongoing, skipping this cycle;"
                + " digestionName={}, interval={} ms",
            digestion.getName(),
            digestion.getTimeUnit().toMillis(digestion.getInterval()));
        return;
      }

      final var state = digestion.createState(executorGroup.next());
      currentExecution.set(
          digestion
              .executeAsyncGeneric(state)
              .exceptionally(
                  (t) -> {
                    for (var st = state; st != null; st = st.getParent()) {
                      st.markError();
                    }
                    logger.error(
                        "Digestion aborted; {}",
                        state.getCallTraceString(),
                        digestion.getName(),
                        t);
                    return null;
                  }));

      currentExecution.get().thenRunAsync(() -> currentExecution.set(null), state.getExecutor());
    }

    /**
     * Notifies the task that the digestor is being shut down.
     *
     * @return Future for the task completion; The task may or may not have been canceled.
     */
    public CompletableFuture<Void> shutdown() {
      digestion.shutdown();
      final var exec = currentExecution.get();
      return exec != null ? exec : CompletableFuture.completedFuture(null);
    }
  }
}
