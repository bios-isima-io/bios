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
package io.isima.bios.recorder;

import static org.hamcrest.MatcherAssert.assertThat;

import io.isima.bios.stats.ClockProvider;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

public class RecorderConcurrencyTest extends RecorderTestImplBase {
  @Override
  @Before
  public void resetClock() {
    // use wall clock and not test clock for this test
    ClockProvider.resetClock();
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    final CompletableFuture f1 = CompletableFuture.runAsync(() -> manyRequests(10000, recorder));
    final CompletableFuture f2 = CompletableFuture.runAsync(() -> manyRequests(15000, recorder));
    final CompletableFuture f3 = CompletableFuture.runAsync(() -> manyRequests(1156, recorder));
    int countSoFar = 0;
    while (countSoFar < 10000) {
      Thread.yield();
      OperationMetricGroupRecorder standby = recorder.checkpoint();
      if (standby == null) {
        continue;
      }
      countSoFar += standby.getOperationMetrics().getCount();
      MatcherAssert.assertThat(countSoFar, Matchers.lessThanOrEqualTo(26156));
    }
    f1.get();
    f2.get();
    f3.get();

    OperationMetricGroupRecorder standby = recorder.checkpoint();
    if (standby != null) {
      countSoFar += standby.getOperationMetrics().getCount();
    }
    assertThat(countSoFar, CoreMatchers.is(26156));
  }

  private void manyRequests(int loopCount, OperationMetricGroupRecorder recorder) {
    final Random rand = new Random();
    for (int i = 0; i < loopCount; i++) {
      recordRandomDelayAllLatency(recorder, rand);
    }
  }
}
