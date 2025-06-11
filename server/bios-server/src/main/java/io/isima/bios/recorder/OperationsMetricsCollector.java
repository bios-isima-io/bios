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

import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_APP_NAME;
import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_APP_TYPE;
import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_NODE;
import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_REQUEST;
import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_STREAM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_BIOS_TENANT;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_BYTES_READ;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_BYTES_WRITTEN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DBP_COUNT;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DBP_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DBP_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DBP_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DB_ERRORS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DECODE_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DECODE_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_DECODE_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_ENCODE_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_ENCODE_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_ENCODE_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_DECODES;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_ENCODES;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_POST_PROCESSES;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_PRE;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_READS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_TRANSIENT_ERRORS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_VALIDATION;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_VALIDATION_ERRORS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_NUM_WRITES;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_POST_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_POST_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_POST_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_PRE_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_PRE_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_PRE_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_STORAGE_ACCESSES;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_STORAGE_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_STORAGE_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_STORAGE_LATENCY_SUM;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_SUCCESSFUL_OPERATIONS;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_VALIDATION_LATENCY_MAX;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_VALIDATION_LATENCY_MIN;
import static io.isima.bios.recorder.RecorderConstants.ATTR_OP_VALIDATION_LATENCY_SUM;

import io.isima.bios.stats.ClockProvider;
import io.isima.bios.stats.Counter;
import io.isima.bios.stats.Moments;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request metrics collector manages regular runtime operations of metrics as follows: a. Collection
 * b. Checkpoint (Check pointing must be explicitly invoked, say periodically) c. Priority queueing
 * of events
 */
public class OperationsMetricsCollector {

  /** Key and current value for the key. */
  public interface Entry {
    OperationsMetricsDimensions getKey();

    Map<String, Object> getEvent();
  }

  private static Logger logger = LoggerFactory.getLogger(OperationsMetricsCollector.class);

  private final InternalRegister recorderRegistry;
  private final PriorityQueue<SystemOperationsMetricsEntry> eventQueue;
  private final Supplier<String> nodeNameSupplier;

  // following are protected under lock since completion and error signals can
  // come from a thread that is different from checkpoint thread
  private final Map<OperationsMetricsDimensions, OperationMetricGroupRecorder> pendingCompletionMap;
  private final Map<OperationsMetricsDimensions, OperationMetricGroupRecorder> errorMap;
  private int numDestinations;
  private boolean shutdown;

  OperationsMetricsCollector(
      InternalRegister register,
      int maxSuccessBandwidth,
      int maxErrorBandwidth,
      Supplier<String> nodeNameSupplier,
      int numDestinations) {
    this.recorderRegistry = register;
    this.eventQueue = new PriorityQueue<>();
    this.nodeNameSupplier = (nodeNameSupplier == null) ? () -> "" : nodeNameSupplier;

    this.pendingCompletionMap = new HashMap<>();
    this.errorMap = new HashMap<>();
    this.numDestinations = numDestinations;
    this.shutdown = false;
  }

  /**
   * Checkpoints currently accumulated stats for every operation.
   *
   * <p>After a call to checkpoint, metric events will be flushed to a priority queue that can then
   * be picked up one by one, by a call to {@link this#getNextEntry()}.
   */
  public synchronized void checkpoint() {
    if (!errorMap.isEmpty()) {
      for (final var err : errorMap.entrySet()) {
        offerRecorderAsIs(err.getKey(), err.getValue());
      }
      errorMap.clear();
    }
    if (!eventQueue.isEmpty() || !pendingCompletionMap.isEmpty()) {
      // do not do checkpoint and add more to the queue if there are still pending items due to
      // throttling at times when the db is slow or error prone. It will anyway be done in the
      // next cycle this will allow to limit the size of the queue
      logger.warn(
          "BiosServer Metrics: Skipping this checkpoint cycle as there are "
              + "{} operations queued and {} total operations in progress.",
          pendingCompletionMap.size());
      return;
    }
    recorderRegistry.runForAllRecorders(this::checkpointOne);
  }

  /**
   * Get the next event from queue.
   *
   * <p>Caller can keep picking up events by calling this method, until it returns null.
   *
   * @return metric for a given operation in a map of attributes form that is suitable for ingestion
   *     or network transmission.
   */
  public synchronized Entry getNextEntry() {
    final var next = eventQueue.poll();
    return next;
  }

  /** Mark the entry against the key as completely done with. */
  public synchronized void completed(OperationsMetricsDimensions key) {
    // successful completion means we can increase the bandwidth
    pendingCompletionMap.remove(key);
  }

  /**
   * The entry was not successfully committed by the caller and the caller wishes to queue it back.
   *
   * @param key Recorder key that uniquely identifies a recorder
   * @param mustRetry When the error is due to a problem and it must be re-attempted.
   */
  public synchronized void error(OperationsMetricsDimensions key, boolean mustRetry) {
    // error completion means we reduce the bandwidth
    OperationMetricGroupRecorder recorder = pendingCompletionMap.remove(key);
    if (mustRetry) {
      if (recorder != null) {
        // put into a map to try in the next cycle
        errorMap.put(key, recorder);
      }
    }
  }

  /** Signals shutdown. */
  public void shutdown() {
    shutdown = true;
  }

  /** Wait for the given millis max for any pending tasks to complete. */
  public synchronized void waitForCompletion(int millisToWait) {
    Clock clock = ClockProvider.getClock();
    Instant start = clock.instant();
    long elapsedMillis = 0;
    while (pendingCompletionMap.size() > 0 && elapsedMillis < millisToWait) {
      try {
        this.wait(millisToWait - elapsedMillis);
        elapsedMillis = Duration.between(start, clock.instant()).toMillis();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  synchronized void checkpointOnDeregister(
      OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
    checkpointOne(key, recorder);
  }

  synchronized void checkpointOnRemoval(
      OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
    final var standbyRecorder = recorder.checkpoint();
    if (standbyRecorder != null) {
      offerRecorderMultiDestinations(key, standbyRecorder);
    }
  }

  private void checkpointOne(
      OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
    final var standbyRecorder = recorder.checkpoint();
    if (standbyRecorder != null) {
      offerRecorderMultiDestinations(key, standbyRecorder);
    }
  }

  private void offerRecorderMultiDestinations(
      OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
    offerRecorderAsIs(key, recorder);

    // duplicate for other destinations, if target is not already system tenant
    if (!key.getTenantName().equals(recorderRegistry.getSystemTenant())) {
      for (int i = 1; i < numDestinations; i++) {
        final var newKey = key.newDestination(i);
        offerRecorderAsIs(newKey, recorder);
      }
    }
  }

  private void offerRecorderAsIs(
      OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
    // save it for future retries
    pendingCompletionMap.put(key, recorder);
    eventQueue.offer(new SystemOperationsMetricsEntry(key, recorder));
  }

  private final class SystemOperationsMetricsEntry
      implements Entry, Comparable<SystemOperationsMetricsEntry> {
    private final long opCount;
    private final OperationsMetricsDimensions recorderKey;
    private final OperationMetricGroupRecorder operationRecorder;

    private SystemOperationsMetricsEntry(
        OperationsMetricsDimensions key, OperationMetricGroupRecorder recorder) {
      this.operationRecorder = recorder;
      this.opCount = recorder.getOperationMetrics().getCount();
      this.recorderKey = key;
    }

    @Override
    public OperationsMetricsDimensions getKey() {
      return recorderKey;
    }

    @Override
    public Map<String, Object> getEvent() {
      Map<String, Object> eventAttributes = new HashMap<>();
      final String tenant = recorderKey.getTenantName();
      final String stream = recorderKey.getStreamName();

      eventAttributes.put(ATTR_BIOS_TENANT, tenant);
      eventAttributes.put(ATTR_BIOS_STREAM, stream);
      eventAttributes.put(ATTR_BIOS_REQUEST, recorderKey.getRequestType().getRequestName());
      eventAttributes.put(ATTR_BIOS_NODE, nodeNameSupplier.get());
      eventAttributes.put(ATTR_BIOS_APP_NAME, recorderKey.getAppName());
      eventAttributes.put(ATTR_BIOS_APP_TYPE, recorderKey.getAppType().stringify());

      final OperationMetrics metrics = operationRecorder.getOperationMetrics();
      eventAttributes.put(ATTR_OP_SUCCESSFUL_OPERATIONS, metrics.getNumSuccessfulOperations());
      eventAttributes.put(ATTR_OP_NUM_VALIDATION_ERRORS, metrics.getNumValidationErrors());
      eventAttributes.put(ATTR_OP_NUM_TRANSIENT_ERRORS, metrics.getNumTransientErrors());

      final Counter recordsWritten = operationRecorder.getNumWrites();
      eventAttributes.put(
          ATTR_OP_NUM_WRITES, recordsWritten != null ? recordsWritten.getCount() : 0);
      eventAttributes.put(ATTR_OP_BYTES_WRITTEN, metrics.getBytesWritten());

      final Counter recordsRead = operationRecorder.getNumReads();
      eventAttributes.put(ATTR_OP_NUM_READS, recordsRead != null ? recordsRead.getCount() : 0);
      eventAttributes.put(ATTR_OP_BYTES_READ, metrics.getBytesRead());

      eventAttributes.put(ATTR_OP_LATENCY_MIN, metrics.getLatencyMin());
      eventAttributes.put(ATTR_OP_LATENCY_MAX, metrics.getLatencyMax());
      eventAttributes.put(ATTR_OP_LATENCY_SUM, metrics.getLatencySum());

      final ArrayList<String> zeroFills = new ArrayList<>();
      final Moments storageAccess = operationRecorder.getStorageAccessMetrics();
      if (storageAccess == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_STORAGE_ACCESSES,
                ATTR_OP_STORAGE_LATENCY_MIN,
                ATTR_OP_STORAGE_LATENCY_MAX,
                ATTR_OP_STORAGE_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_STORAGE_ACCESSES, storageAccess.getCount());
        eventAttributes.put(ATTR_OP_STORAGE_LATENCY_MIN, storageAccess.getMin());
        eventAttributes.put(ATTR_OP_STORAGE_LATENCY_MAX, storageAccess.getMax());
        eventAttributes.put(ATTR_OP_STORAGE_LATENCY_SUM, storageAccess.getSum());
      }

      // optional attributes follow, these do not go into the signal

      final Moments decode = operationRecorder.getDecodeMetrics();
      if (decode == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_NUM_DECODES,
                ATTR_OP_NUM_DECODES,
                ATTR_OP_DECODE_LATENCY_MIN,
                ATTR_OP_DECODE_LATENCY_MAX,
                ATTR_OP_DECODE_LATENCY_SUM));
      } else {
        final var count = decode.getCount();
        eventAttributes.put(ATTR_OP_NUM_DECODES, count);
        eventAttributes.put(ATTR_OP_DECODE_LATENCY_MIN, decode.getMin());
        eventAttributes.put(ATTR_OP_DECODE_LATENCY_MAX, decode.getMax());
        eventAttributes.put(ATTR_OP_DECODE_LATENCY_SUM, decode.getSum());
      }

      // Following metrics collections are for dev/debug, values do not go to the operations signal
      final Moments validate = operationRecorder.getValidateMetrics();
      if (validate == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_NUM_VALIDATION,
                ATTR_OP_NUM_VALIDATION,
                ATTR_OP_VALIDATION_LATENCY_MIN,
                ATTR_OP_VALIDATION_LATENCY_MAX,
                ATTR_OP_VALIDATION_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_NUM_VALIDATION, validate.getCount());
        eventAttributes.put(ATTR_OP_VALIDATION_LATENCY_MIN, validate.getMin());
        eventAttributes.put(ATTR_OP_VALIDATION_LATENCY_MAX, validate.getMax());
        eventAttributes.put(ATTR_OP_VALIDATION_LATENCY_SUM, validate.getSum());
      }

      final Moments pre = operationRecorder.getPreProcessMetrics();
      if (pre == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_NUM_PRE,
                ATTR_OP_NUM_PRE,
                ATTR_OP_PRE_LATENCY_MIN,
                ATTR_OP_PRE_LATENCY_MAX,
                ATTR_OP_PRE_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_NUM_PRE, pre.getCount());
        eventAttributes.put(ATTR_OP_PRE_LATENCY_MIN, pre.getMin());
        eventAttributes.put(ATTR_OP_PRE_LATENCY_MAX, pre.getMax());
        eventAttributes.put(ATTR_OP_PRE_LATENCY_SUM, pre.getSum());
      }

      final Moments dbp = operationRecorder.getDbPrepareMetrics();
      if (dbp == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_DBP_COUNT,
                ATTR_OP_DBP_COUNT,
                ATTR_OP_DBP_LATENCY_MIN,
                ATTR_OP_DBP_LATENCY_MAX,
                ATTR_OP_DBP_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_DBP_COUNT, dbp.getCount());
        eventAttributes.put(ATTR_OP_DBP_LATENCY_MIN, dbp.getMin());
        eventAttributes.put(ATTR_OP_DBP_LATENCY_MAX, dbp.getMax());
        eventAttributes.put(ATTR_OP_DBP_LATENCY_SUM, dbp.getSum());
      }

      final Moments dbError = operationRecorder.getDbErrorMetrics();
      if (dbError == null) {
        zeroFills.addAll(Arrays.asList(ATTR_OP_DB_ERRORS));
      } else {
        eventAttributes.put(ATTR_OP_DB_ERRORS, dbError.getCount());
      }

      final Moments post = operationRecorder.getPostProcessMetrics();
      if (post == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_NUM_POST_PROCESSES,
                ATTR_OP_NUM_POST_PROCESSES,
                ATTR_OP_POST_LATENCY_MIN,
                ATTR_OP_POST_LATENCY_MAX,
                ATTR_OP_POST_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_NUM_POST_PROCESSES, post.getCount());
        eventAttributes.put(ATTR_OP_POST_LATENCY_MIN, post.getMin());
        eventAttributes.put(ATTR_OP_POST_LATENCY_MAX, post.getMax());
        eventAttributes.put(ATTR_OP_POST_LATENCY_SUM, post.getSum());
      }

      final Moments enc = operationRecorder.getEncodeMetrics();
      if (enc == null) {
        zeroFills.addAll(
            Arrays.asList(
                ATTR_OP_NUM_ENCODES,
                ATTR_OP_NUM_ENCODES,
                ATTR_OP_ENCODE_LATENCY_MIN,
                ATTR_OP_ENCODE_LATENCY_MAX,
                ATTR_OP_ENCODE_LATENCY_SUM));
      } else {
        eventAttributes.put(ATTR_OP_NUM_ENCODES, enc.getCount());
        eventAttributes.put(ATTR_OP_ENCODE_LATENCY_MIN, enc.getMin());
        eventAttributes.put(ATTR_OP_ENCODE_LATENCY_MAX, enc.getMax());
        eventAttributes.put(ATTR_OP_ENCODE_LATENCY_SUM, enc.getSum());
      }

      for (final var key : zeroFills) {
        eventAttributes.put(key, 0L);
      }
      return eventAttributes;
    }

    @Override
    public int compareTo(SystemOperationsMetricsEntry o) {
      // this is a reverse compareTo to attain maxHeap property
      if (this.opCount > o.opCount) {
        return -1;
      } else if (this.opCount < o.opCount) {
        return 1;
      } else {
        return o.recorderKey.compareTo(this.recorderKey);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SystemOperationsMetricsEntry)) {
        return false;
      }
      SystemOperationsMetricsEntry that = (SystemOperationsMetricsEntry) o;
      return opCount == that.opCount && recorderKey.equals(that.recorderKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(opCount, recorderKey);
    }
  }
}
