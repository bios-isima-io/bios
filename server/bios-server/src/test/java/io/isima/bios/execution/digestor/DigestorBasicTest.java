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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DigestorBasicTest {

  private Digestor digestor;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {
    digestor =
        new Digestor(2, ExecutorManager.makeThreadFactory("test-digestor", Thread.MIN_PRIORITY));
  }

  @After
  public void tearDown() throws Exception {
    digestor.shutdown();
  }

  @Test
  public void testFundamental() throws Exception {
    final var digestion = new DelayAndIncrement(1, -1, 0, 100, TimeUnit.MILLISECONDS);
    digestor.register(digestion);
    Thread.sleep(1000);
    digestor.shutdown();

    assertThat(digestion.isShutdown, is(true));
    assertThat(digestion.getConcurrentExecutionCheckValue(), is(0));
    assertThat(digestion.getCount(), lessThan(15));
    assertThat(digestion.getCount(), greaterThan(5));
  }

  @Test
  public void testSlowDigestion() throws Exception {
    // task delay is longer than interval
    final var digestion = new DelayAndIncrement(200, -1, 0, 100, TimeUnit.MILLISECONDS);
    digestor.register(digestion);
    Thread.sleep(2000);
    digestor.shutdown();

    assertThat(digestion.isShutdown, is(true));
    assertThat(digestion.getConcurrentExecutionCheckValue(), is(0));
    assertThat(digestion.getCount(), lessThan(15));
    assertThat(digestion.getCount(), greaterThan(5));
  }

  @Test
  public void testErrors() throws Exception {
    // task throws in every 5 times
    final var digestion = new DelayAndIncrement(1, 5, 0, 100, TimeUnit.MILLISECONDS);
    digestor.register(digestion);
    Thread.sleep(2000);
    digestor.shutdown();

    // Verify that the digestion does not stop by the errors
    assertThat(digestion.isShutdown, is(true));
    assertThat(digestion.getConcurrentExecutionCheckValue(), is(0));
    assertThat(digestion.getCount(), lessThan(25));
    assertThat(digestion.getCount(), greaterThan(5));
  }

  private static class DelayAndIncrement extends Digestion<GenericExecutionState> {

    private final long taskDelayMillis;
    private final int errorInterval;
    private volatile int count;
    private final AtomicInteger concurrentExecutionCheck;

    public DelayAndIncrement(
        long taskDelayMillis,
        int errorInterval,
        long initialDelay,
        long interval,
        TimeUnit timeUnit) {
      super("DelayAndIncrement", initialDelay, interval, timeUnit, GenericExecutionState.class);
      this.taskDelayMillis = taskDelayMillis;
      this.errorInterval = errorInterval;
      count = 0;
      concurrentExecutionCheck = new AtomicInteger(0);
    }

    public int getCount() {
      return count;
    }

    public int getConcurrentExecutionCheckValue() {
      return concurrentExecutionCheck.get();
    }

    @Override
    public GenericExecutionState createState(EventExecutorGroup executorGroup) {
      return new GenericExecutionState("DelayAndIncrement", executorGroup);
    }

    @Override
    public CompletableFuture<Void> executeAsync(GenericExecutionState state) {
      return CompletableFuture.runAsync(
              () -> {
                state.addHistory("(checkExecOverlap");
                // verify that the digestor synchronizes executions
                concurrentExecutionCheck.compareAndSet(0, 1);
                state.addHistory(")(delay");

                state.addHistory(")(increment");
                try {
                  ++count;
                  if (errorInterval > 0 && count % errorInterval == 0) {
                    throw new RuntimeException("TEST EXCEPTION THROWN INTENTIONALLY");
                  }
                } finally {
                  concurrentExecutionCheck.decrementAndGet();
                }
              },
              CompletableFuture.delayedExecutor(
                  taskDelayMillis, TimeUnit.MILLISECONDS, state.getExecutor()))
          .thenRunAsync(() -> state.addHistory(")"), state.getExecutor());
    }
  }
}
