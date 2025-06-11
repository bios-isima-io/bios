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

import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;
import static io.isima.bios.recorder.ContextRequestType.UPSERT;
import static io.isima.bios.recorder.ControlRequestType.CREATE_TENANT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import io.isima.bios.models.AppType;
import io.isima.bios.recorder.OperationsMeasurementRegistry;
import org.junit.Ignore;
import org.junit.Test;

public class OperationMetricsTracerTest {
  @Test
  @Ignore
  public void testControlRequestType() {
    final var registry = createTestRegistry();
    OperationMetricsTracer trackerUnderTest = new OperationMetricsTracer(10L);
    trackerUnderTest.attachRecorder(
        registry.getRecorder("t1", "", "app", AppType.BATCH, CREATE_TENANT));
    trackerUnderTest.startEncoding(true);
    trackerUnderTest.stop(101L);
    final var rec = registry.getRecorder("t1", "", "app", AppType.BATCH, CREATE_TENANT);
    assertNotNull(trackerUnderTest.getLocalRecorder(""));
    assertThat(rec.getOperationMetrics().getBytesWritten(), is(10L));
    assertThat(rec.getOperationMetrics().getBytesRead(), is(101L));
  }

  @Test
  @Ignore
  public void testSingleRecorder() {
    final var registry = createTestRegistry();
    OperationMetricsTracer trackerUnderTest = new OperationMetricsTracer(100L);
    trackerUnderTest.attachRecorder(
        registry.getRecorder("t1", "c1", "tracerTest", AppType.INTERNAL, UPSERT));
    final var local = trackerUnderTest.getLocalRecorder("c1");
    for (int i = 0; i < 10; i++) {
      final var timer = local.getStorageAccessMetrics().createTimer();
      local.getNumReads().add(10L);
      Thread.yield();
      Thread.yield();
      timer.commit();
    }
    trackerUnderTest.startEncoding(true);
    trackerUnderTest.stop(10);
    final var rec = registry.getRecorder("t1", "c1", "tracerTest", AppType.INTERNAL, UPSERT);
    assertThat(rec.getStorageAccessMetrics().getCount(), is(10L));
    assertThat(rec.getOperationMetrics().getBytesWritten(), is(100L));
    assertThat(rec.getNumReads().getCount(), is(100L));
  }

  @Test
  @Ignore
  public void testContextChaining() {
    final var registry = createTestRegistry();
    OperationMetricsTracer trackerUnderTest = new OperationMetricsTracer(100L);
    trackerUnderTest.attachRecorder(
        registry.getRecorder("t1", "c1", "tracerTest", AppType.INTERNAL, UPSERT));
    final var local1 = trackerUnderTest.getLocalRecorder("C1");
    for (int i = 0; i < 10; i++) {
      final var timer1 = local1.getStorageAccessMetrics().createTimer();
      local1.getNumReads().add(100);
      trackerUnderTest.getLocalRecorder("C2", registry::getRecorder).getNumReads().add(10);
      trackerUnderTest.getLocalRecorder("C3", registry::getRecorder).getNumReads().add(5);
      Thread.yield();
      Thread.yield();
      timer1.commit();
    }
    trackerUnderTest.startEncoding(true);
    trackerUnderTest.stop(2000);
    final var rec = registry.getRecorder("t1", "c1", "tracerTest", AppType.INTERNAL, UPSERT);
    assertThat(rec.getStorageAccessMetrics().getCount(), is(10L));
    assertThat(rec.getOperationMetrics().getCount(), is(1L));
    assertThat(rec.getNumReads().getCount(), is(1000L));
    final var rec1 = registry.getRecorder("t1", "C2", "tracerTest", AppType.INTERNAL, UPSERT);
    assertThat(rec1.getNumReads().getCount(), is(100L));
    final var rec2 = registry.getRecorder("t1", "C3", "tracerTest", AppType.INTERNAL, UPSERT);
    assertThat(rec2.getNumReads().getCount(), is(50L));
  }

  OperationsMeasurementRegistry createTestRegistry() {
    return OperationsMeasurementRegistry.newRegistryBuilder(TENANT_SYSTEM)
        .maxSuccessBandwidth(10)
        .maxErrorBandwidth(2)
        .build();
  }
}
