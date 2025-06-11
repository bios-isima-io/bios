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
import static org.hamcrest.junit.MatcherAssert.assertThat;

import com.google.common.base.Ticker;
import io.isima.bios.models.AppType;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/** Test that the registered control plane recorders eventually expires. */
public class RecorderRegistryExpiryTest extends RecorderTestImplBase {

  @Test
  public void testControlPlaneDynamicRegistryExpires() {
    TestTicker ticker = new TestTicker();
    OperationsMeasurementRegistry registry = createTestRegistryWithTestTicker(ticker);
    registry.addTenant("tenant1");
    OperationMetricGroupRecorder recorder =
        registry.getRecorder(
            "tenant1",
            "",
            "recorderRegistryExpiryTest",
            AppType.INTERNAL,
            ControlRequestType.GET_SIGNALS);

    recordRequestDelayAllLatency(recorder, 100, 200, 100);
    recordRequestDelayAllLatency(recorder, 100, 200, 100);

    // do it again
    OperationMetricGroupRecorder recorder1 =
        registry.getRecorder(
            "tenant1",
            "",
            "recorderRegistryExpiryTest",
            AppType.INTERNAL,
            ControlRequestType.GET_SIGNALS);

    recordRequestDelayAllLatency(recorder1, 100, 200, 100);

    ticker.advanceTime(Duration.ofMinutes(11));
    // this will be a new entry
    OperationMetricGroupRecorder recorder2 =
        registry.getRecorder(
            "tenant1",
            "",
            "recorderRegistryExpiryTest",
            AppType.INTERNAL,
            ControlRequestType.GET_SIGNALS);

    recordRequestDelayAllLatency(recorder2, 100, 200, 100);

    // ensure all are checkpointed and nothing is lost despite expiry
    final var entry = registry.getOperationsMetricsCollector().getNextEntry();
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS), is(3L));
    assertThat(entry.getEvent().get(RecorderConstants.ATTR_OP_LATENCY_SUM), is(300L));
    Assert.assertNull(registry.getOperationsMetricsCollector().getNextEntry());
    registry.getOperationsMetricsCollector().completed(entry.getKey());

    registry.getOperationsMetricsCollector().checkpoint();

    final var entry1 = registry.getOperationsMetricsCollector().getNextEntry();
    assertThat(entry1.getEvent().get(RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS), is(1L));
    assertThat(entry1.getEvent().get(RecorderConstants.ATTR_OP_LATENCY_SUM), is(100L));
  }

  private static final class TestTicker extends Ticker {
    private long advance = 0;

    @Override
    public long read() {
      return System.nanoTime() + advance;
    }

    private void advanceTime(Duration elapsed) {
      this.advance = elapsed.toNanos();
    }
  }
}
