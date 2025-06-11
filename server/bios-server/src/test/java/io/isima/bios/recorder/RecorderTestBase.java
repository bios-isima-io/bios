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

import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_BYTES_READ;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_BYTES_WRITTEN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_VALIDATION_ERRORS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS;
import static io.isima.bios.recorder.RecorderConstants.BIOS_API_VERSION_1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import io.isima.bios.stats.ClockProvider;
import io.isima.bios.stats.Moments;
import io.isima.bios.stats.Timer;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;

public class RecorderTestBase {
  protected static final RequestType TEST_OP = new TestMetricOperation("op");
  protected static final String TENANT = "tenant1";
  protected static final String SIGNAL = "signal1";
  protected static final String[] apiVersions = {BIOS_API_VERSION_1};

  protected final TestClock testClock = new TestClock();

  @Before
  public void setTestClock() {
    ClockProvider.useTestClock(testClock);
  }

  @After
  public void resetClock() {
    ClockProvider.resetClock();
  }

  protected void assertSuccessCounts(
      OperationMetrics opLatency,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoError,
      long subCount) {
    assertThat(opLatency.getNumSuccessfulOperations(), is(count));
    assertThat(opLatency.getBytesWritten(), is(count * dataIn));
    assertThat(opLatency.getBytesRead(), is(count * dataOut));
    assertThat(opLatency.getLatencySum(), is(getExpectedSum(count, minElapsed)));
    if (minElapsed > 0) {
      assertThat(opLatency.getLatencyMin(), is(minElapsed));
      assertThat(opLatency.getLatencyMax(), is(minElapsed + count - 1));
      if (count > 1) {
        assertThat(opLatency.getLatencyMin(), lessThan(opLatency.getLatencyMax()));
      }
    } else {
      assertThat(opLatency.getLatencyMin(), is(0L));
      assertThat(opLatency.getLatencyMax(), is(0L));
    }
    if (checkNoError) {
      assertErrorCounts(opLatency, 0L, 0L, 0L, 0L, false);
    }
  }

  protected void assertSuccessCounts(
      OperationMetrics opLatency,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoError) {
    assertSuccessCounts(opLatency, count, dataIn, dataOut, minElapsed, checkNoError, 1);
  }

  protected void assertSuccessCounts(
      Map<String, Object> event,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoError) {
    assertThat(event.get(ATTR_OP_SUCCESSFUL_OPERATIONS), is(count));
    assertThat(event.get(ATTR_OP_BYTES_WRITTEN), is(count * dataIn));
    assertThat(event.get(ATTR_OP_BYTES_READ), is(count * dataOut));
    assertThat(event.get(ATTR_OP_LATENCY_SUM), is(getExpectedSum(count, minElapsed)));
    if (minElapsed > 0) {
      assertThat(event.get(ATTR_OP_LATENCY_MIN), is(minElapsed));
      assertThat(event.get(ATTR_OP_LATENCY_MAX), is(minElapsed + count - 1));
    } else {
      assertThat(event.get(ATTR_OP_LATENCY_MIN), is(0L));
      assertThat(event.get(ATTR_OP_LATENCY_MAX), is(0L));
    }
    if (checkNoError) {
      assertErrorCounts(event, 0L, 0L, 0L, 0L, false);
    }
  }

  private void assertErrorCounts(
      Map<String, Object> event,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoSuccess) {
    assertThat(event.get(ATTR_OP_NUM_VALIDATION_ERRORS), is(count));
    if (checkNoSuccess) {
      assertSuccessCounts(event, 0L, 0L, 0L, 0L, false);
    }
  }

  protected void assertErrorCounts(
      OperationMetrics opLatency,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoSuccess,
      long subCount) {
    assertThat(opLatency.getNumValidationErrors(), is(count));
    assertThat(opLatency.getNumTransientErrors(), is(count * subCount));
    if (checkNoSuccess) {
      assertSuccessCounts(opLatency, 0L, 0L, 0L, 0L, false);
    }
  }

  protected void assertErrorCounts(
      OperationMetrics opLatency,
      long count,
      long dataIn,
      long dataOut,
      long minElapsed,
      boolean checkNoSuccess) {
    assertErrorCounts(opLatency, count, dataIn, dataOut, minElapsed, checkNoSuccess, 1);
  }

  private Timer[] getLatencyTimers(OperationMetricGroupRecorder recorder) {
    Timer[] timers = new Timer[8];
    timers[0] = recorder.getEncodeMetrics().createTimer();
    timers[1] = recorder.getValidateMetrics().createTimer();
    timers[2] = recorder.getPreProcessMetrics().createTimer();
    timers[3] = recorder.getDbPrepareMetrics().createTimer();
    timers[4] = recorder.getStorageAccessMetrics().createTimer();
    timers[5] = recorder.getDbErrorMetrics().createTimer();
    timers[6] = recorder.getPostProcessMetrics().createTimer();
    timers[7] = recorder.getDecodeMetrics().createTimer();
    return timers;
  }

  protected Timer[] getUnattachedTimers(int count) {
    Timer[] timers = new Timer[count];
    for (int i = 0; i < count; i++) {
      timers[i] = Timer.create();
    }
    return timers;
  }

  protected void attachTimers(OperationMetricGroupRecorder recorder, Timer[] timers) {
    recorder.getEncodeMetrics().attachTimer(timers[0]);
    recorder.getValidateMetrics().attachTimer(timers[1]);
    recorder.getPreProcessMetrics().attachTimer(timers[2]);
    recorder.getDbPrepareMetrics().attachTimer(timers[3]);
    recorder.getStorageAccessMetrics().attachTimer(timers[4]);
    recorder.getDbErrorMetrics().attachTimer(timers[5]);
    recorder.getPostProcessMetrics().attachTimer(timers[6]);
    recorder.getDecodeMetrics().attachTimer(timers[7]);
  }

  private void assertCounts(Moments moments, long count, long minElapsed) {
    assertThat(moments.getCount(), is(count));
    assertThat(moments.getMin(), is(minElapsed));
    assertThat(moments.getMax(), is(minElapsed + count - 1));
    if (count > 1) {
      assertThat(moments.getMax(), greaterThan(moments.getMin()));
    }
    assertThat(moments.getSum(), is(getExpectedSum(count, minElapsed)));
  }

  protected void assertAllCounts(
      OperationMetricGroupRecorder recorder, long count, long minLatency) {
    assertCounts(recorder.getDecodeMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getValidateMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getPreProcessMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getDbPrepareMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getStorageAccessMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getDbErrorMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getPostProcessMetrics().checkPoint(), count, minLatency);
    assertCounts(recorder.getEncodeMetrics().checkPoint(), count, minLatency);
  }

  protected void stopAllTimers(Timer[] timers) {
    for (var timer : timers) {
      timer.commit();
    }
  }

  protected void recordRequestDelay(
      OperationMetricGroupRecorder recorder,
      long bytesIn,
      long bytesOut,
      long delayMicros,
      boolean error) {
    testClock.controlClock(1, delayMicros * 1000);
    Timer timer = new Timer();
    Thread.yield();
    final var elapsed = timer.commit();
    final var operationMetrics = recorder.getOperationMetrics();
    if (error) {
      operationMetrics.update(0, 0, 1, elapsed, 1, 1, bytesIn, bytesOut);
    } else {
      operationMetrics.update(1, 0, 0, elapsed, 1, 1, bytesIn, bytesOut);
    }
  }

  protected void recordRequestDelayAllLatency(
      OperationMetricGroupRecorder recorder, long dataIn, long dataOut, long delayMicros) {
    testClock.controlClock(9, delayMicros * 1000);
    final var timer = new Timer();
    final Timer[] timers = getLatencyTimers(recorder);
    Thread.yield();
    stopAllTimers(timers);
    Thread.yield();
    Thread.yield();
    recorder.getOperationMetrics().update(1, 0, 0, timer.commit(), 1, 1, dataIn, dataOut);
  }

  protected void recordRandomDelayAllLatency(OperationMetricGroupRecorder recorder, Random random) {
    final var timer = new Timer();
    final Timer[] timers = getLatencyTimers(recorder);
    Thread.yield();
    Thread.yield();
    long j = 0;
    for (int i = 0; i < (1000 + random.nextInt(10000)); i++) {
      j += i * 10;
    }
    stopAllTimers(timers);
    Thread.yield();
    Thread.yield();
    recorder.getOperationMetrics().update(1, 0, 0, timer.commit(), 1, 1, j, j + 1000);
  }

  OperationsMeasurementRegistry createTestRegistry() {
    return OperationsMeasurementRegistry.newRegistryBuilder("sys")
        .maxSuccessBandwidth(10)
        .maxErrorBandwidth(2)
        .build();
  }

  OperationsMeasurementRegistry createTestRegistry(Supplier<String> nodeNameSupplier) {
    return OperationsMeasurementRegistry.newRegistryBuilder("sys")
        .maxSuccessBandwidth(10)
        .maxErrorBandwidth(2)
        .nodeNameSupplier(nodeNameSupplier)
        .build();
  }

  private long getExpectedSum(long count, long minElapsed) {
    // expected is (n/2) * (2a + (n-1)d); sum of n terms of an AP
    final double factor = ((double) count) / 2.0d;
    return (long) (factor * ((double) ((2 * minElapsed) + count - 1)));
  }

  private static final class TestMetricOperation implements RequestType {
    private final String operationName;

    private TestMetricOperation(String operationName) {
      this.operationName = operationName;
    }

    @Override
    public String getRequestName() {
      return operationName;
    }

    @Override
    public int getRequestNumber() {
      return 0;
    }

    @Override
    public boolean isControl() {
      return false;
    }

    @Override
    public int priority() {
      return 0;
    }

    @Override
    public boolean shouldRateLimit() {
      return false;
    }
  }
}
