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

import io.isima.bios.stats.Counter;
import io.isima.bios.stats.Timer;
import org.junit.Ignore;
import org.junit.Test;

public class OperationMetricGroupRecorderTest extends RecorderTestImplBase {

  @Test
  public void testSuccessRequestLatency() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 100; i++) {
      recordRequestDelay(recorder, 10, 100, 100 + i, false);
    }
    OperationMetrics opLatency = recorder.getOperationMetrics().checkPoint();
    assertSuccessCounts(opLatency, 100, 10, 100, 100, true);
  }

  @Test
  @Ignore
  public void testErrorRequestLatency() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 10; i++) {
      recordRequestDelay(recorder, 10, 20, 50 + i, true);
    }
    OperationMetrics opLatency = recorder.getOperationMetrics().checkPoint();
    assertErrorCounts(opLatency, 10, 10, 20, 50, true);
  }

  @Test
  @Ignore
  public void testMixErrorAndSuccessRequestLatency() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 18; i++) {
      recordRequestDelay(recorder, 100, 2000, 500 + i, false);
    }
    for (int i = 0; i < 12; i++) {
      recordRequestDelay(recorder, 98, 200, 50 + i, true);
    }
    OperationMetrics opLatency = recorder.getOperationMetrics().checkPoint();
    assertSuccessCounts(opLatency, 18, 100, 2000, 500, false);
    assertErrorCounts(opLatency, 12, 98, 200, 50, false);
  }

  @Test
  public void testAllLatency() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 2000; i++) {
      recordRequestDelayAllLatency(recorder, 100, 1000, 100 + i);
    }
    OperationMetrics opLatency = recorder.getOperationMetrics().checkPoint();
    assertSuccessCounts(opLatency, 2000, 100, 1000, 100, true);
    assertAllCounts(recorder, 2000, 100);
  }

  @Test
  public void testAttachTimer() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    testClock.controlClock(9, 50 * 1000);
    final var timer = new Timer();
    final var timers = getUnattachedTimers(8);
    Thread.yield();
    attachTimers(recorder, timers);
    Thread.yield();
    Thread.yield();
    stopAllTimers(timers);
    Thread.yield();
    recorder.getOperationMetrics().update(1, 0, 0, timer.commit(), 1, 1, 100, 100);
    assertSuccessCounts(recorder.getOperationMetrics().checkPoint(), 1, 100, 100, 50, true);
    assertAllCounts(recorder, 1, 50);
  }

  /*
  @Test
  public void testBulkAttachTimer() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    testClock.controlClock(9, 50 * 1000);
    RequestTimer timer = RequestTimer.create();
    final var timers = getUnattachedTimers(8);
    Thread.yield();
    recorder.getOperationMetrics().attachBulkRequestTimer(10, timer);
    attachTimers(recorder, timers);
    Thread.yield();
    Thread.yield();
    stopAllTimers(timers);
    Thread.yield();
    timer.stop(1, 1, 100, 100);
    assertSuccessCounts(recorder.getOperationMetrics().checkPoint(), 1, 100, 100, 50, true, 10);
    assertAllCounts(recorder, 1, 50);
  }
   */

  /*
   @Test
   public void testBulkAttachTimerError() {
     OperationMetricGroupRecorder recorder = createTestRecorder();
     testClock.controlClock(9, 10 * 1000);
     RequestTimer timer = RequestTimer.create();
     final var timers = getUnattachedTimers(8);
     Thread.yield();
     recorder.getOperationMetrics().attachBulkRequestTimer(10, timer);
     attachTimers(recorder, timers);
     Thread.yield();
     Thread.yield();
     stopAllTimers(timers);
     Thread.yield();
     timer.errorStop(100, 100);
     assertErrorCounts(recorder.getOperationMetrics().checkPoint(), 1, 100, 100, 10, true, 10);
     assertAllCounts(recorder, 1, 10);
   }
  */

  /*
  @Test
  public void testBulkAttachTimerPartialError() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    testClock.controlClock(9, 10 * 1000);
    RequestTimer timer = RequestTimer.create();
    Thread.yield();
    recorder.getOperationMetrics().attachBulkRequestTimer(10, timer);
    Thread.yield();
    timer.partialErrorStop(8, 100, 100);
    final var checkpointed = recorder.getOperationMetrics().checkPoint();
    assertThat(checkpointed.getNumTransientErrors(), is(2L));
  }
   */

  /*
  @Test
  public void testMultiRecorderAttachTimer() {
    OperationMetricGroupRecorder recorder1 = createTestRecorder();
    OperationMetricGroupRecorder recorder2 = createTestRecorder();
    testClock.controlClock(9, 50 * 1000);
    for (int i = 0; i < 10; i++) {
      RequestTimer timer = RequestTimer.create();
      final var timers = getUnattachedTimers(8);
      Thread.yield();
      recorder1.getOperationMetrics().attachRequestTimer(timer);
      recorder2.getOperationMetrics().attachRequestTimer(timer);
      attachTimers(recorder1, timers);
      attachTimers(recorder2, timers);
      Thread.yield();
      Thread.yield();
      stopAllTimers(timers);
      Thread.yield();
      if (i % 2 == 0) {
        timer.stop(100, 100);
      } else {
        timer.errorStop(100, 100);
      }
    }
    final var checkpointed1 = recorder1.getOperationMetrics().checkPoint();
    final var checkpointed2 = recorder2.getOperationMetrics().checkPoint();
    assertThat(checkpointed1.getNumSuccessfulOperations(), is(5L));
    assertThat(checkpointed1.getNumValidationErrors(), is(5L));
    assertThat(checkpointed2.getNumSuccessfulOperations(), is(5L));
    assertThat(checkpointed2.getNumValidationErrors(), is(5L));
    assertThat(checkpointed1.getLatencySum(), is(125L));
    assertThat(checkpointed2.getLatencySum(), is(125L));
  }
   */

  @Test
  public void testRecordsReadCounter() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 10; i++) {
      recorder.getNumReads().add(50);
    }
    Counter recordsRead = recorder.getNumReads().checkPoint();
    assertThat(recordsRead.getCount(), is(500L));
  }

  @Test
  public void testRecordsReturnedCounter() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    for (int i = 0; i < 10; i++) {
      recorder.getNumWrites().add(20);
    }
    Counter recordsReturned = recorder.getNumWrites().checkPoint();
    assertThat(recordsReturned.getCount(), is(200L));
  }

  /*
  @Test
  public void testMergeFrom() {
    OperationMetricGroupRecorder recorder = createTestRecorder();
    OperationMetricGroupRecorder merge = createTestRecorder();
    testClock.controlClock(9, 10 * 1000);

    RequestTimer timer = RequestTimer.create();
    final var timers = getUnattachedTimers(8);
    Thread.yield();
    recorder.getOperationMetrics().attachRequestTimer(timer);
    attachTimers(recorder, timers);
    stopAllTimers(timers);
    timer.stop(10, 10);

    testClock.controlClock(8, 5 * 1000);
    final var mergeTimers = getUnattachedTimers(8);
    attachTimers(merge, mergeTimers);
    stopAllTimers(mergeTimers);

    recorder.getRecordsRead().add(50);
    recorder.getRecordsReturned().add(2);
    merge.getRecordsRead().add(50);
    merge.getRecordsReturned().add(3);

    recorder.mergeFrom(merge);

    final var dbLatency = recorder.getDbAccessMetrics().checkPoint();
    assertThat(dbLatency.getCount(), is(2L));
    assertThat(dbLatency.getSum(), is(15L));
    assertThat(dbLatency.getMin(), is(5L));
    assertThat(dbLatency.getMax(), is(10L));
    Counter recordsRead = recorder.getRecordsRead().checkPoint();
    assertThat(recordsRead.getCount(), is(100L));
    Counter recordsReturned = recorder.getRecordsReturned().checkPoint();
    assertThat(recordsReturned.getCount(), is(5L));
  }
   */
}
