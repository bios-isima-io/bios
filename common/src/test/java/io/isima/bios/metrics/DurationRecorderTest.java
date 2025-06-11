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
package io.isima.bios.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.Test;

public class DurationRecorderTest {
  private static final MetricsOperation TEST_OP = new TestMetricOperation("op", "subOp");
  private static final String TENANT = "tenant1";
  private static final String SIGNAL = "signal1";
  private static final String TYPE = "signal";

  @Test
  public void testConsume() {
    DurationRecorder recorder = createTestRecorder();
    for (int i = 0; i < 100; i++) {
      recorder.consume(i + 1);
    }
    recorder.checkpoint();
    Stats stats = recorder.getStandByStat();
    assertThat(stats.getCount(), is(100));
    assertThat(stats.getSum(), is(100L * 101L / 2L));
    assertThat(stats.getMax(), is(100L));
    assertThat(stats.getMin(), is(1L));
  }

  @Test
  public void testConsumeIn() {
    DurationRecorder recorder = createTestRecorder();
    for (int i = 0; i < 100; i++) {
      recorder.consumeIn(i + 1, 100);
    }
    recorder.checkpoint();
    Stats stats = recorder.getStandByStat();
    assertThat(stats.getCount(), is(100));
    assertThat(stats.getDataBytesIn(), is(100L * 100L));
    assertThat(stats.getDataBytesOut(), is(0L));
  }

  @Test
  public void testConsumeMix() {
    DurationRecorder recorder = createTestRecorder();
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        recorder.consumeIn(i + 1, 100);
      } else {
        recorder.consumeOut(i + 1, 100);
      }
    }
    recorder.checkpoint();
    Stats stats = recorder.getStandByStat();
    assertThat(stats.getCount(), is(100));
    assertThat(stats.getDataBytesIn(), is(50L * 100L));
    assertThat(stats.getDataBytesOut(), is(50L * 100L));
  }

  @Test
  public void testConsumeError() {
    DurationRecorder recorder = createTestRecorder();
    for (int i = 0; i < 100; i++) {
      recorder.consumeError(i + 1, 100);
    }
    recorder.checkpoint();
    Stats stats = recorder.getStandByStat();
    assertThat(stats.getCount(), is(0));
    assertThat(stats.getOverallCount(), is(100));
    assertThat(stats.getErrorCount(), is(100));
    assertThat(stats.getDataBytesIn(), is(100L * 100L));
  }

  @Test
  public void testScheduling() throws ExecutionException, InterruptedException {
    DurationRecorder recorder = createTestRecorder();
    CompletableFuture f1 = CompletableFuture.runAsync(() -> consumeInLoop(10000, recorder));
    CompletableFuture f2 = CompletableFuture.runAsync(() -> consumeInLoop(5000, recorder));
    CompletableFuture f3 = CompletableFuture.runAsync(() -> consumeInLoop(156, recorder));
    int countSoFar = 0;
    long sumSoFar = 0;
    while (countSoFar < 10000) {
      Thread.yield();
      recorder.checkpoint();
      Stats stats = recorder.getStandByStat();
      countSoFar += stats.getCount();
      sumSoFar += stats.getSum();
      assertThat(countSoFar, Matchers.lessThanOrEqualTo(15156));
    }
    assertThat(countSoFar, Matchers.greaterThan(0));
    assertThat(sumSoFar, Matchers.greaterThan(0L));
    f1.get();
    f2.get();
    f3.get();

    recorder.checkpoint();
    Stats stats = recorder.getStandByStat();
    countSoFar += stats.getCount();
    sumSoFar += stats.getSum();
    assertThat(countSoFar, is(15156));
    final long expectedSum = (10001L * 5000L) + (2500L * 5001L) + (78L * 157L);
    assertThat(sumSoFar, is(expectedSum));
  }

  private void consumeInLoop(int loopCount, DurationRecorder recorder) {
    for (int i = 0; i < loopCount; i++) {
      if (i % 4 == 0) {
        // create a synthetic delay
        Thread.yield();
        Thread.yield();
        recorder.consumeOut(i + 1, 10);
      } else {
        Thread.yield();
        recorder.consumeOut(i + 1, 10);
      }
    }
  }

  private DurationRecorder createTestRecorder() {
    return new DurationRecorder(
        TEST_OP, TENANT, SIGNAL, TYPE, TimeUnit.MICROSECONDS, TimeUnit.MICROSECONDS);
  }

  private static final class TestMetricOperation implements MetricsOperation {
    private final String operationName;
    private final String subOperationName;

    private TestMetricOperation(String operationName, String subOperationName) {
      this.operationName = operationName;
      this.subOperationName = subOperationName;
    }

    @Override
    public String getOperationName() {
      return operationName;
    }

    @Override
    public String getSubOperationName() {
      return subOperationName;
    }

    @Override
    public boolean isInputOperation() {
      return false;
    }
  }
}
