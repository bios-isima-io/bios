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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.junit.Test;

/**
 * Tests the throttling effect of the {@link OperationsMetricsCollector}. The test verifies that no
 * counts are lost despite the presence of simulated errors and high load.
 */
public class MetricsThrottlingTest extends LoadGeneratorBase {
  // Increase this count for local stress testing, but check in with a lower count to reduce
  // build times
  private static final int TEST_REQUEST_COUNT = 50;

  private volatile boolean shouldSimulateError = false;
  private volatile int sleepMillis = 2;

  @Test
  public void testNormalThrottling() throws ExecutionException, InterruptedException {
    sleepMillis = 2;
    simulateLoad();
    shutDownLoad();
    assertCompletionCounts(TEST_REQUEST_COUNT * 2);
  }

  @Test
  public void testThrottlingInPresenceOfErrors() throws ExecutionException, InterruptedException {
    sleepMillis = 50;
    shouldSimulateError = true;
    simulateLoad();
    shouldSimulateError = false;
    shutDownLoad();
    assertCompletionCounts(TEST_REQUEST_COUNT * 2);
  }

  @Override
  protected Supplier<Boolean> simulateError() {
    if (shouldSimulateError) {
      // error 10% if time
      return () -> rand.nextInt(100) > 95;
    } else {
      return () -> false;
    }
  }

  @Override
  protected Runnable delayLoop() {
    return () -> {
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    };
  }

  private void simulateLoad() throws ExecutionException, InterruptedException {
    loadRegistryForTenant(node1Registry, TEST_TENANT1, 2, 2);
    loadRegistryForTenant(node2Registry, TEST_TENANT1, 2, 2);

    loadRegistryForTenant(node1Registry, TEST_TENANT2, 2, 2);
    loadRegistryForTenant(node2Registry, TEST_TENANT2, 2, 2);

    List<CompletableFuture> futures = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final int x = i;
      futures.add(CompletableFuture.runAsync(() -> loadSignal(node1Registry, x), testPool));
      futures.add(CompletableFuture.runAsync(() -> loadSignal(node2Registry, x), testPool));
    }
    for (int i = 0; i < 2; i++) {
      final int x = i;
      futures.add(CompletableFuture.runAsync(() -> loadContext(node1Registry, x), testPool));
      futures.add(CompletableFuture.runAsync(() -> loadContext(node2Registry, x), testPool));
    }

    for (CompletableFuture future : futures) {
      future.get();
    }
  }

  private void loadSignal(OperationsMeasurementRegistry registry, int signalIdx) {
    for (int i = 0; i < TEST_REQUEST_COUNT / 2; i++) {
      simulateSignalLoad(registry, TEST_TENANT1, signalIdx, 2);
      simulateSignalLoad(registry, TEST_TENANT2, signalIdx, 2);
    }
  }

  private void loadContext(OperationsMeasurementRegistry registry, int contextIdx) {
    for (int i = 0; i < TEST_REQUEST_COUNT; i++) {
      simulateContextLoad(registry, TEST_TENANT1, contextIdx, 1);
      simulateContextLoad(registry, TEST_TENANT2, contextIdx, 1);
    }
  }
}
