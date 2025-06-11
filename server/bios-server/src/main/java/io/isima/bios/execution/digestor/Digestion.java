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
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Abstract class that executes a type of digestions.
 *
 * <p>An instance of this class is registered to {@link Digestor} where the digestion task is
 * executed periodically at a fixed rate determined by class parameters {@link #initialDelay},
 * {@link #interval}, and {@link #timeUnit}.
 *
 * <p>The abstract method {@link #executeAsync()} does actual task.
 */
@Getter
@RequiredArgsConstructor
@ToString
public abstract class Digestion<StateT extends ExecutionState> {

  private final String name;
  private final long initialDelay;
  private final long interval;
  private final TimeUnit timeUnit;
  private final Class<StateT> stateClass;

  protected volatile boolean isShutdown = false;

  /** Notifies running tasks that the digestor is being shut down. */
  public void shutdown() {
    isShutdown = true;
  }

  /**
   * Generic method to invoke a digestion task.
   *
   * <p>This method is meant to be called only the by Digestor who does not know the state type of
   * this class. The request comes with a generic state object but it's guaranteed to be of type
   * StateT.
   *
   * @param state Execution state
   * @return Future for the completion
   */
  CompletableFuture<Void> executeAsyncGeneric(ExecutionState state) {
    return executeAsync(stateClass.cast(state));
  }

  /**
   * Create a state object.
   *
   * <p>This method is called by Digestor when it invokes a task.
   *
   * @param executorGroup Executor group used for the task
   * @return Created state object
   */
  public abstract StateT createState(EventExecutorGroup executorGroup);

  /**
   * The method to execute the digestion task asynchronously.
   *
   * <p>The task is invoked by {@link Digestor} periodically with the digestion's interval. There
   * are several rules for implementing this method:
   *
   * <ul>
   *   <li>Split the execution into multiple stages if the entire execution takes long time.
   *   <li>Always use the executor provided by the state object to run the asynchronous stages.
   *   <li>Never fork the task into multiple (extra) threads; Run multiple async execution stages if
   *       concurrent subtasks are necessary. If you need to delay executing a stage, schedule it
   *       using the given scheduled executor service.
   *   <li>The method must not block the executor thread; Never use mutex locks on the thread, I/O
   *       (DB, files, etc) methods must be asynchronous.
   * </ul>
   *
   * @param state The execution state for the task
   * @return Future for the completion
   */
  public abstract CompletableFuture<Void> executeAsync(StateT state);
}
