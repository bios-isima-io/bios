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
package io.isima.bios.stats;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Test;

public class CounterTest {
  @Test
  public void testCounter() {
    Counter counter = Counter.create();
    counter.increment();
    counter.increment();
    counter.add(8);
    assertThat(counter.getCount(), is(10L));
  }

  @Test
  public void testCounterCheckpoint() {
    Counter counter = Counter.create();
    counter.increment();
    counter.increment();
    counter.add(8);

    Counter standby = counter.checkPoint();
    assertThat(counter.getCount(), is(0L));
    assertThat(standby.getCount(), is(10L));
  }

  @Test
  public void testConcurrentCounterUpdates() throws ExecutionException, InterruptedException {
    final Counter counter = Counter.create();
    final CompletableFuture f1 = CompletableFuture.runAsync(() -> manyRequests(20000, counter));
    final CompletableFuture f2 = CompletableFuture.runAsync(() -> manyRequests(15000, counter));
    final CompletableFuture f3 = CompletableFuture.runAsync(() -> manyRequests(15000, counter));
    int countSoFar = 0;
    while (countSoFar < 15000) {
      Thread.yield();
      Counter standby = counter.checkPoint();
      if (standby == null) {
        continue;
      }
      countSoFar += standby.getCount();
      assertThat(countSoFar, Matchers.lessThanOrEqualTo(50000));
    }
    f1.get();
    f2.get();
    f3.get();

    Counter standby = counter.checkPoint();
    if (standby != null) {
      countSoFar += standby.getCount();
    }
    assertThat(countSoFar, CoreMatchers.is(50000));
  }

  private void manyRequests(int loopCount, Counter counter) {
    for (int i = 0; i < loopCount; i++) {
      counter.increment();
      if (i % 1000 == 0) {
        Thread.yield();
      }
    }
  }
}
