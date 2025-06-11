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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DigestorThreadAssignmentTest {
  private static final int NUM_THREADS = 4;

  private static Digestor digestor;

  @BeforeClass
  public static void setUp() throws Exception {
    digestor =
        new Digestor(
            NUM_THREADS, ExecutorManager.makeThreadFactory("test-digestor", Thread.MIN_PRIORITY));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    digestor.shutdown();
  }

  @Test
  public void test() throws Exception {
    final Queue<Integer> threadCounts = new ConcurrentLinkedQueue<>();
    final Set<String> totalThreadNames = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < 100; ++i) {
      final var digestion = new RepeatStages(threadCounts, totalThreadNames);
      digestor.register(digestion);
    }
    while (threadCounts.size() < 100) {
      Thread.sleep(100);
    }
    Thread.sleep(100);

    digestor.shutdown();

    while (!threadCounts.isEmpty()) {
      assertThat(threadCounts.remove(), is(1));
    }
    assertThat(totalThreadNames.size(), is(NUM_THREADS));
  }

  /**
   * Digestion to test thread assignment.
   *
   * <p>The task executes multiple asynchronous stages in an execution, which should stay in a
   * single thread. The class has a set of thread names where stages accumulates its thread names.
   * The size of the set must be one after the execution.
   */
  private static class RepeatStages extends Digestion<GenericExecutionState> {

    // Shared by multiple instances, each instance pushes threads count for each end of executions.
    private final Queue<Integer> threadCounts;

    // Shared by multiple instances, each instance puts used thread names at the end of each
    // execution to verify all Digestor threads were used.
    private final Set<String> totalThreadNames;

    private final Set<String> threadNames;

    public RepeatStages(Queue<Integer> threadCounts, Set<String> totalThreadNames) {
      super("RepeatStages", 0, 1, TimeUnit.MILLISECONDS, GenericExecutionState.class);
      this.threadCounts = threadCounts;
      this.totalThreadNames = totalThreadNames;
      threadNames = ConcurrentHashMap.newKeySet();
    }

    private void runStage() {
      threadNames.add(Thread.currentThread().getName());
    }

    @Override
    public GenericExecutionState createState(EventExecutorGroup executorGroup) {
      return new GenericExecutionState("RepeatStages", executorGroup);
    }

    @Override
    public CompletableFuture<Void> executeAsync(GenericExecutionState state) {
      final var executor = state.getExecutor();
      return CompletableFuture.runAsync(this::runStage, executor)
          .thenRunAsync(this::runStage, executor)
          .thenRunAsync(this::runStage, executor)
          .thenRunAsync(this::runStage, executor)
          .thenRunAsync(this::runStage, executor)
          .thenRunAsync(this::runStage, executor)
          .thenRunAsync(
              () -> {
                threadCounts.add(threadNames.size());
                totalThreadNames.addAll(threadNames);
                threadNames.clear();
              },
              executor);
    }
  }
}
