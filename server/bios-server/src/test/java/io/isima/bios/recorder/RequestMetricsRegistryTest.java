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

import static io.isima.bios.recorder.SignalRequestType.SELECT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import io.isima.bios.models.AppType;
import org.junit.Ignore;
import org.junit.Test;

public class RequestMetricsRegistryTest extends RecorderTestBase {
  @Test
  public void testNullSystemTenant() {
    try {
      OperationsMeasurementRegistry.newRegistryBuilder(null).build();
      fail("Expected to fail");
    } catch (NullPointerException npe) {
      assertNotNull(npe.getMessage());
    }
  }

  @Test
  public void testLowSuccessBandwidth() {
    try {
      OperationsMeasurementRegistry.newRegistryBuilder("sys").maxSuccessBandwidth(1).build();
      fail("Expected to fail");
    } catch (IllegalArgumentException npe) {
      assertNotNull(npe.getMessage());
    }
  }

  @Test
  public void testInvalidErrorBandwidth() {
    try {
      OperationsMeasurementRegistry.newRegistryBuilder("sys").maxErrorBandwidth(-1).build();
      fail("Expected to fail");
    } catch (IllegalArgumentException npe) {
      assertNotNull(npe.getMessage());
    }
  }

  @Test
  public void testControlPlaneDynamicRegistry() {
    final var registry = createTestRegistry(() -> "testNode");
    registry.addTenant("tenant1");
    OperationMetricGroupRecorder recorder =
        registry.getRecorder(
            "tenant1", "", "registryTest", AppType.INTERNAL, ControlRequestType.GET_SIGNALS);
    assertThat(recorder.getTenant(), is("tenant1"));

    recordRequestDelayAllLatency(recorder, 100, 200, 100);
    registry.getOperationsMetricsCollector().checkpoint();
    final var entry = registry.getOperationsMetricsCollector().getNextEntry();
    assertThat(entry.getKey().getStreamName(), is(""));
    assertThat(entry.getKey().getTenantName(), is("tenant1"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_BIOS_NODE), is("testNode"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS), is(1L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_LATENCY_SUM), is(100L));
    registry.removeTenant("tenant1");
  }

  @Test
  @Ignore
  public void testControlPlaneReturnsDummyRecorderIfNotWhitelisted() {
    final var registry = createTestRegistry(() -> "testNode");
    OperationMetricGroupRecorder recorder =
        registry.getRecorder(
            "tenant1", "", "registryTest", AppType.INTERNAL, ControlRequestType.GET_SIGNALS);
    assertThat(recorder.getTenant(), is("sys"));

    recordRequestDelayAllLatency(recorder, 100, 200, 100);
    registry.getOperationsMetricsCollector().checkpoint();
    final var entry = registry.getOperationsMetricsCollector().getNextEntry();
    assertThat(entry.getKey().getStreamName(), is(""));
    assertThat(entry.getKey().getTenantName(), is("sys"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_BIOS_NODE), is("testNode"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS), is(1L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_LATENCY_SUM), is(100L));
  }

  @Test
  public void testRecordCounters() {
    final var registry = createTestRegistry(() -> "testNode");
    final var recorder =
        registry.getRecorder("tenant1", "signal1", "registryTest", AppType.INTERNAL, SELECT);
    assertThat(recorder.getTenant(), is("tenant1"));

    recordRequestDelayAllLatency(recorder, 100, 200, 100);
    recorder.getNumReads().add(5000L);
    recorder.getNumReads().increment();
    recorder.getNumWrites().add(8L);
    recorder.getNumWrites().increment();
    recorder.getNumWrites().increment();
    registry.getOperationsMetricsCollector().checkpoint();
    final var entry = registry.getOperationsMetricsCollector().getNextEntry();
    assertThat(entry.getKey().getStreamName(), is("signal1"));
    assertThat(entry.getKey().getTenantName(), is("tenant1"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_BIOS_NODE), is("testNode"));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS), is(1L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_LATENCY_SUM), is(100L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_NUM_READS), is(5001L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_NUM_WRITES), is(10L));
  }
}
