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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import org.junit.Test;

public class OperationsMetricsCollectorTest extends RecorderTestImplBase {
  private OperationsMetricsCollector manager;

  @Test
  public void testCheckPointNone() {
    manager = new OperationsMetricsCollector(consumer -> {}, 10, 2, null, 1);
    manager.checkpoint();
    assertNull(manager.getNextEntry());
  }

  @Test
  public void testCheckPointOne() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    TestInternalRegister register = new TestInternalRegister(Collections.singletonList(recorder));
    manager = new OperationsMetricsCollector(register, 10, 4, null, 1);
    for (int i = 0; i < 10; i++) {
      recordRequestDelay(recorder, 10, 100, 100 + i, false);
    }
    manager.checkpoint();
    final var entry = manager.getNextEntry();
    assertNotNull(entry);
    manager.completed(entry.getKey());
    assertSuccessCounts(entry.getEvent(), 10, 10, 100, 100, true);
    assertNull(manager.getNextEntry());
  }

  @Test
  public void testCheckPointIfNotConsumed() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    TestInternalRegister register = new TestInternalRegister(Collections.singletonList(recorder));
    manager = new OperationsMetricsCollector(register, 100, 50, null, 1);
    for (int i = 0; i < 10; i++) {
      recordRequestDelay(recorder, 10, 100, 100 + i, false);
    }
    manager.checkpoint();
    // checkpoint again without consuming
    for (int i = 0; i < 2; i++) {
      recordRequestDelay(recorder, 2, 100, 100 + i, false);
    }

    manager.checkpoint();
    final var entry = manager.getNextEntry();
    assertNotNull(entry);
    manager.completed(entry.getKey());
    assertSuccessCounts(entry.getEvent(), 10, 10, 100, 100, true);
    assertNull(manager.getNextEntry());

    manager.checkpoint();
    final var entry1 = manager.getNextEntry();
    assertNotNull(entry1);
    manager.completed(entry1.getKey());
    assertSuccessCounts(entry1.getEvent(), 2, 2, 100, 100, true);
    assertNull(manager.getNextEntry());
  }

  @Test
  public void testPriorityOrderForManyCountsAndSignals() {
    List<OperationMetricGroupRecorder> testRecorders = createSignalTestRecorder("isima", "s1");
    testRecorders.addAll(createSignalTestRecorder("other", "s2"));
    testRecorders.addAll(createSignalTestRecorder("isima", "s3"));

    TestInternalRegister register = new TestInternalRegister(testRecorders);
    manager = new OperationsMetricsCollector(register, 1000, 5, null, 1);

    int startCount = 10;
    for (final var rec : testRecorders) {
      for (int i = 0; i < startCount; i++) {
        recordRequestDelay(rec, 20, 20, 500 + i, false);
      }
      startCount++;
    }
    manager.checkpoint();
    OperationsMetricsCollector.Entry entry;
    int resultCount = testRecorders.size() - 1;
    do {
      entry = manager.getNextEntry();
      if (entry == null) {
        break;
      }
      manager.completed(entry.getKey());
      assertSuccessCounts(entry.getEvent(), 10 + resultCount, 20, 20, 500, false);
      resultCount--;
    } while (resultCount >= 0);
    assertNull(manager.getNextEntry());
    assertThat(resultCount, is(-1));
  }

  @Test
  public void testPriorityForSameCount() {
    List<OperationMetricGroupRecorder> testRecorders = createControlTestRecorder("isima", "");
    TestInternalRegister register = new TestInternalRegister(testRecorders);
    manager = new OperationsMetricsCollector(register, 100, 20, null, 1);

    for (final var rec : testRecorders) {
      for (int i = 0; i < 5; i++) {
        recordRequestDelayAllLatency(rec, 20, 28, 1000 + i);
      }
    }
    manager.checkpoint();
    OperationsMetricsCollector.Entry entry;
    int resultCount = testRecorders.size() - 1;
    int lastPriority = Integer.MAX_VALUE;
    do {
      entry = manager.getNextEntry();
      if (entry == null) {
        break;
      }
      manager.completed(entry.getKey());
      assertThat(entry.getKey().getRequestType().priority(), lessThanOrEqualTo(lastPriority));
      lastPriority = entry.getKey().getRequestType().priority();
      assertSuccessCounts(entry.getEvent(), 5, 20, 28, 1000, false);
      resultCount--;
    } while (resultCount >= 0);

    assertNull(manager.getNextEntry());
    assertThat(resultCount, is(-1));
  }

  private static final class TestInternalRegister implements InternalRegister {
    private final List<OperationMetricGroupRecorder> recorders;

    TestInternalRegister(List<OperationMetricGroupRecorder> recorders) {
      this.recorders = recorders;
    }

    @Override
    public void runForAllRecorders(
        BiConsumer<OperationsMetricsDimensions, OperationMetricGroupRecorder> consumer) {
      recorders.forEach(
          (r) -> {
            final var composedKey =
                new OperationsMetricsDimensions(
                    r.getTenant(),
                    r.getStream(),
                    r.getAppName(),
                    r.getAppType(),
                    r.getRequestType());
            consumer.accept(composedKey, r);
          });
    }
  }
}
