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
package io.isima.bios.sdk.metrics;

import static io.isima.bios.sdk.metrics.MetricsController.begin;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import io.isima.bios.metrics.DurationRecorder;
import io.isima.bios.metrics.MetricRecord;
import io.isima.bios.sdk.metrics.MetricsController.MetricsHandle;
import io.isima.bios.sdk.metrics.MetricsController.OperationType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Metrics unit test cases.
 *
 * <p>TODO(ramesh) Metrics enablement in {@link Metrics} relies on static initialization order,
 * which is risky in a unit test environment (as multiple unit tests use the same JVM), for now
 * these tests just logs a warning, if Metrics.enabled() returns false, to avoid future brittleness.
 */
public class MetricControllerTest {

  static {
    System.setProperty(Metrics.CLIENT_METRICS_ENABLED, "true");
  }

  @AfterClass
  public static void clear() {
    System.clearProperty(Metrics.CLIENT_METRICS_ENABLED);
  }

  @Test
  public void testConstructor() {
    MetricsController cntlUnderTest =
        new MetricsController("tenant", "sig", "signal", Collections.emptySet());
    assertNotNull(cntlUnderTest);
  }

  @Test
  public void testSingleSignalSelect() {
    if (!Metrics.isEnabled()) {
      System.out.println("WARNING: Static initialization order broken. Skipping Metric tests");
      return;
    }
    MetricsController cntlUnderTest =
        new MetricsController("tenant", "sig", "signal", Set.of(OperationType.SELECT));
    long currentTime = System.currentTimeMillis() - 100 * 100;
    for (int i = 0; i < 100; i++) {
      final long startTime = currentTime + i * 100;
      MetricsHandle handle = begin(OperationType.SELECT, true, () -> startTime);
      cntlUnderTest.endAsync(handle, startTime + 10);

      fillTimeBuffer(handle.timeStampBuffer(), startTime + 10);
      cntlUnderTest.end(handle, startTime + 100);
      cntlUnderTest.endSync(handle, startTime + 100);
    }
    verifyDuration(cntlUnderTest);
  }

  @Test
  public void testSingleSelectAsync() {
    if (!Metrics.isEnabled()) {
      System.out.println("WARNING: Static initialization order broken. Skipping Metric tests");
      return;
    }
    MetricsController cntlUnderTest =
        new MetricsController("tenant", "sig", "signal", Set.of(OperationType.SELECT));
    long currentTime = System.currentTimeMillis() - 100 * 100;
    for (int i = 0; i < 100; i++) {
      final long startTime = currentTime + i * 100;
      MetricsHandle handle = begin(OperationType.SELECT, false, () -> startTime);
      cntlUnderTest.endAsync(handle, startTime + 10);

      fillTimeBuffer(handle.timeStampBuffer(), startTime + 10);
      cntlUnderTest.end(handle, startTime + 100);
    }
    verifyDurationAsync(cntlUnderTest);
  }

  // simulates how a multi-signal select behaves w.r.t metrics and tests it
  @Test
  public void testMultiSignalSelect() {
    if (!Metrics.isEnabled()) {
      System.out.println("WARNING: Static initialization order broken. Skipping Metric tests");
      return;
    }
    MetricsController cntlUnderTest1 =
        new MetricsController("tenant", "sig", "signal", Set.of(OperationType.SELECT));
    MetricsController cntlUnderTest2 =
        new MetricsController("tenant", "sig1", "signal", Set.of(OperationType.SELECT));
    long currentTime = System.currentTimeMillis() - 100 * 100;
    for (int i = 0; i < 100; i++) {
      final long startTime = currentTime + i * 100;
      MetricsHandle handle = begin(OperationType.SELECT, true, () -> startTime);

      fillTimeBuffer(handle.timeStampBuffer(), startTime + 10);
      cntlUnderTest1.end(handle, startTime + 100);
      cntlUnderTest2.end(handle, startTime + 100);
      cntlUnderTest1.endSync(handle, startTime + 100);
      cntlUnderTest2.endSync(handle, startTime + 100);
    }
    verifyDuration(cntlUnderTest1);
    verifyDuration(cntlUnderTest2);
  }

  private void fillTimeBuffer(ByteBuffer timeStampBuffer, long startTime) {
    timeStampBuffer.clear();
    timeStampBuffer.order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 16; i++) {
      timeStampBuffer.putLong(startTime + i * 10);
    }
  }

  private void verifyDuration(MetricsController controller) {
    DurationRecorder end = controller.getRecorder(ClientMetricsOperation.EXTRACT_SYNC);
    DurationRecorder intermediate =
        controller.getRecorder(ClientMetricsOperation.EXTRACT_REQUEST_DISPATCH);
    end.checkpoint();
    intermediate.checkpoint();
    MetricRecord endRecord = end.captureMetricRecord();
    MetricRecord intermediateRecord = intermediate.captureMetricRecord();
    assertThat(endRecord.getCount(), is(100));
    assertThat(intermediateRecord.getCount(), is(100));
  }

  private void verifyDurationAsync(MetricsController controller) {
    DurationRecorder async = controller.getRecorder(ClientMetricsOperation.EXTRACT_ASYNC);
    DurationRecorder intermediate =
        controller.getRecorder(ClientMetricsOperation.EXTRACT_REQUEST_DISPATCH);
    async.checkpoint();
    intermediate.checkpoint();
    MetricRecord asyncRecord = async.captureMetricRecord();
    MetricRecord intermediateRecord = intermediate.captureMetricRecord();
    assertThat(asyncRecord.getCount(), is(100));
    assertThat(intermediateRecord.getCount(), is(100));
  }
}
