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

import static io.isima.bios.recorder.OperationsMetricsCollector.Entry;
import static org.hamcrest.CoreMatchers.is;

import io.isima.bios.models.AppType;
import io.isima.bios.stats.ClockProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Supplier;
import org.hamcrest.junit.MatcherAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class LoadGeneratorBase extends RecorderTestBase {
  private static final String TEST_TENANT_BASE = "tenant";
  static final String TEST_TENANT1 = TEST_TENANT_BASE + 0;
  static final String TEST_TENANT2 = TEST_TENANT_BASE + 1;

  private static final String TEST_SIGNAL_BASE = "signal";
  private static final String TEST_CONTEXT_BASE = "context";
  private static final String END_MARKER_TENANT = "theEnd";

  private volatile boolean shutdown = false;
  private CompletableFuture<Void> checkpointOne;
  private CompletableFuture<Void> checkpointTwo;

  final Random rand = new Random(System.currentTimeMillis());
  OperationsMeasurementRegistry node1Registry;
  OperationsMeasurementRegistry node2Registry;
  private final Map<OperationsMetricsDimensions, Long> completionTracker =
      new ConcurrentHashMap<>();

  static ExecutorService testPool;

  @BeforeClass
  public static void startPool() {
    testPool = Executors.newFixedThreadPool(20);
  }

  @AfterClass
  public static void stopPool() {
    testPool.shutdownNow();
  }

  @Before
  public void resetClockAndLoad() {
    // use wall clock and not test clock for this test
    ClockProvider.resetClock();
    final var baseBuilder =
        OperationsMeasurementRegistry.newRegistryBuilder("_system")
            .maxSuccessBandwidth(1000)
            .maxErrorBandwidth(2)
            .numDestinations(2);

    node1Registry = baseBuilder.nodeNameSupplier(() -> "testNode0").build();
    node2Registry = baseBuilder.nodeNameSupplier(() -> "testNode1").build();
    node1Registry.addTenant(TEST_TENANT1);
    node1Registry.addTenant(TEST_TENANT2);
    checkpointOne =
        CompletableFuture.runAsync(
            () -> doPeriodicCheckPoint(node1Registry.getOperationsMetricsCollector()), testPool);
    checkpointTwo =
        CompletableFuture.runAsync(
            () -> doPeriodicCheckPoint(node2Registry.getOperationsMetricsCollector()), testPool);
  }

  void shutDownLoad() throws ExecutionException, InterruptedException {
    this.shutdown = true;
    checkpointOne.get();
    checkpointTwo.get();
    node1Registry.removeTenant(TEST_TENANT1);
    node1Registry.removeTenant(TEST_TENANT2);
    node1Registry.resetRegistry();
    node2Registry.resetRegistry();
  }

  private void doPeriodicCheckPoint(OperationsMetricsCollector operationsMetricsCollector) {
    final BlockingDeque<Entry> completionQueue = new LinkedBlockingDeque<>();
    final var completionFuture =
        testPool.submit(
            () -> completionHandler(operationsMetricsCollector, completionQueue, () -> {}));
    // make sure everything is flushed out
    int loopAfterShutdown = 3;
    while (loopAfterShutdown >= 0) {
      delayLoop().run();
      operationsMetricsCollector.checkpoint();
      Entry entry;
      do {
        entry = operationsMetricsCollector.getNextEntry();
        if (entry != null) {
          completionQueue.add(entry);
        }
      } while (entry != null);
      if (shutdown) {
        // ensure completion before exiting..1 * 3 sec should be more than enough
        operationsMetricsCollector.waitForCompletion(1000);
        loopAfterShutdown--;
      }
      if (loopAfterShutdown <= 0) {
        completionQueue.add(END_MARKER);
      }
    }
    try {
      completionFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private void completionHandler(
      OperationsMetricsCollector manager,
      BlockingDeque<Entry> completionQueue,
      Runnable delaySimulator) {
    while (true) {
      final Entry e;
      try {
        e = completionQueue.take();
        if (e.getKey().getTenantName().equalsIgnoreCase(END_MARKER_TENANT)) {
          break;
        }
        delaySimulator.run();
        if (simulateError().get()) {
          manager.error(e.getKey(), true);
        } else {
          manager.completed(e.getKey());
          completionTracker.compute(
              e.getKey(),
              (k, v) -> {
                Long nextValue =
                    (Long) e.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS);
                if (v == null) {
                  return nextValue;
                } else {
                  return v + nextValue;
                }
              });
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  void assertCompletionCounts(long expectedCount) {
    for (var tracker : completionTracker.entrySet()) {
      MatcherAssert.assertThat(tracker.getValue(), is(expectedCount));
    }
  }

  void loadRegistryForTenant(
      OperationsMeasurementRegistry registry, String tenant, int signalCount, int contextCount) {
    /*
    for (int i = 0; i < signalCount; i++) {
      registry.registerSignal(tenant, TEST_SIGNAL_BASE + i);
    }
    for (int i = 0; i < contextCount; i++) {
      registry.registerContext(tenant, TEST_CONTEXT_BASE + i);
    }
     */
  }

  void simulateSignalLoad(
      OperationsMeasurementRegistry registry, String tenant, int signalIdx, int requestCount) {
    for (RequestType type : SignalRequestType.values()) {
      final var recorder =
          registry.getRecorder(
              tenant, TEST_SIGNAL_BASE + signalIdx, "loadGenerator", AppType.INTERNAL, type);
      for (int i = 0; i < requestCount; i++) {
        recordRandomDelayAllLatency(recorder, rand);
      }
    }
  }

  void simulateContextLoad(
      OperationsMeasurementRegistry registry, String tenant, int contextIdx, int requestCount) {
    for (RequestType type : ContextRequestType.values()) {
      for (var apiVersion : apiVersions) {
        final var recorder =
            registry.getRecorder(
                tenant, TEST_CONTEXT_BASE + contextIdx, "loadGenerator", AppType.INTERNAL, type);
        for (int i = 0; i < requestCount; i++) {
          recordRandomDelayAllLatency(recorder, rand);
        }
      }
    }
  }

  protected Supplier<Boolean> simulateError() {
    return () -> false;
  }

  protected Runnable delayLoop() {
    return () -> {
      try {
        Thread.sleep(2);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    };
  }

  private static final Entry END_MARKER =
      new Entry() {
        @Override
        public OperationsMetricsDimensions getKey() {
          return new OperationsMetricsDimensions(
              END_MARKER_TENANT, "", "loadGenerator", AppType.INTERNAL, SignalRequestType.INSERT);
        }

        @Override
        public Map<String, Object> getEvent() {
          return Collections.emptyMap();
        }
      };
}
