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

import io.isima.bios.recorder.OperationMetricGroupRecorder;
import io.isima.bios.recorder.OperationMetricsRecorder;
import io.isima.bios.stats.Counter;
import io.isima.bios.stats.Moments;
import io.isima.bios.stats.Timer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;

/**
 * Convenience class for encapsulating information about the lifecycle of an operation.
 *
 * <p>The main purpose of this class is to hold metric timers and counters to measure an operation.
 */
public class OperationMetricsTracer {
  private final Timer timer;
  private final Timer decodeTimer;
  @Getter private Optional<Timer> validateTimer;
  @Getter private Optional<Timer> preProcessTimer;

  private LocalRecorder localRecorder;
  private final AtomicReference<Map<String, LocalRecorder>> localRecorders =
      new AtomicReference<>();

  private List<OperationMetricsTracer> subTracers;

  private Optional<Timer> postProcessTimer = Optional.empty();
  private Optional<Timer> encodeTimer;
  private volatile boolean isSuccess = true;

  @Setter protected long bytesWritten;
  protected long numPartialSuccess;

  @Getter @Setter private boolean isWriteOperation = false;

  public OperationMetricsTracer(long bytesWritten) {
    timer = new Timer();
    decodeTimer = new Timer();
    validateTimer = Optional.empty();
    preProcessTimer = Optional.empty();
    encodeTimer = Optional.empty();
    this.numPartialSuccess = 0;
    this.localRecorder = null;
    this.bytesWritten = (bytesWritten < 0) ? 0 : bytesWritten;
  }

  public void doneDecoding() {
    decodeTimer.stopWithoutCommit();
  }

  public void startValidation() {
    validateTimer = Optional.of(new Timer());
  }

  public void endValidation() {
    validateTimer.ifPresent((timer) -> timer.stopWithoutCommit());
  }

  public void clearBytesWritten() {
    bytesWritten = 0;
  }

  public LocalRecorder attachRecorder(OperationMetricGroupRecorder recorder) {
    endValidation();
    this.localRecorder = new LocalRecorder(recorder);
    localRecorder.getDecodeMetrics().attachTimer(decodeTimer);
    localRecorder.getValidateMetrics().attachTimer(validateTimer.orElse(null));
    decodeTimer.commit();
    validateTimer.ifPresent(timer -> timer.commit());
    preProcessTimer = Optional.of(new Timer());
    return this.localRecorder;
  }

  public void attachRecorders(List<OperationMetricGroupRecorder> recorders) {
    endValidation();
    Map<String, LocalRecorder> tmpMap = new HashMap<>();
    for (var recorder : recorders) {
      recorder.getDecodeMetrics().attachTimer(decodeTimer);
      recorder.getValidateMetrics().attachTimer(validateTimer.orElse(null));
      tmpMap.compute(
          recorder.getStream().toLowerCase(),
          (name, existing) -> {
            final LocalRecorder newRecorder;
            if (existing != null) {
              existing.incrementNumOperations();
              existing.getDecodeMetrics().attachTimer(decodeTimer);
              existing.getValidateMetrics().attachTimer(validateTimer.orElse(null));
              return existing;
            } else {
              return new LocalRecorder(recorder);
            }
          });
    }
    localRecorders.set(Collections.unmodifiableMap(tmpMap));
    decodeTimer.commit();
    validateTimer.ifPresent((timer) -> timer.commit());
  }

  public void addSubTracer(OperationMetricsTracer subTracer) {
    if (subTracers == null) {
      subTracers = new ArrayList<>();
    }
    subTracers.add(subTracer);
  }

  public void recordPartialSuccess(int numSuccess) {
    this.numPartialSuccess = numSuccess;
  }

  public Instant getStartTime() {
    return timer.getStartTime();
  }

  public void addError(boolean isValidationError) {
    if (localRecorder != null) {
      localRecorder.addError(isValidationError);
      if (!isValidationError) {
        localRecorder.getNumReads().clear();
        localRecorder.getNumWrites().clear();
      }
    }
  }

  public void startPostProcess() {
    if (localRecorder != null) {
      postProcessTimer = Optional.of(new Timer());
      localRecorder.getPostProcessMetrics().attachTimer(postProcessTimer.get());
    }
  }

  public void endPostProcess() {
    postProcessTimer.ifPresent(
        (timer) -> {
          timer.commit();
          postProcessTimer = Optional.empty();
        });
  }

  public void startEncoding(boolean isSuccess) {
    encodeTimer = Optional.of(new Timer());
    if (localRecorder != null) {
      localRecorder.getEncodeMetrics().attachTimer(encodeTimer.get());
    }
    final var currentLocalRecorders = localRecorders.get();
    if (currentLocalRecorders != null) {
      currentLocalRecorders
          .values()
          .forEach((recorder) -> recorder.getEncodeMetrics().attachTimer(encodeTimer.get()));
    }
    if (subTracers != null) {
      subTracers.forEach((subTracer) -> subTracer.startEncoding(isSuccess));
    }
    this.isSuccess = isSuccess;
  }

  public void doneEncoding() {
    encodeTimer.ifPresent((timer) -> timer.stopWithoutCommit());
  }

  public void stop(long bytesRead) {
    long latency = timer.stopWithoutCommit();
    // if sub traces exist, this is a parent operation of sub operations.
    if (subTracers != null && subTracers.size() > 0) {
      // Since there is no good way as of v1.1 to know the read bytes for each operation,
      // we just divide total bytes, and we ignore any modulo for simplicity.
      final long dividedBytes = bytesRead / subTracers.size();
      subTracers.forEach(
          (subTracer) -> {
            subTracer.stop(dividedBytes);
          });
      return;
    }
    encodeTimer.ifPresent((timer) -> timer.commit());
    final long bytesReadToRecord = isWriteOperation ? 0 : bytesRead;
    if (localRecorder != null) {
      complete(localRecorder, latency, bytesReadToRecord);
    }
    final var currentLocalRecorders = localRecorders.get();
    if (currentLocalRecorders != null) {
      currentLocalRecorders
          .values()
          .forEach((recorder) -> complete(recorder, latency, bytesReadToRecord));
    }
  }

  private void complete(LocalRecorder recorder, long latency, long bytesRead) {
    recorder.commit();
    long numWrites = recorder.getNumWrites().getCount();
    long numReads = recorder.getNumReads().getCount();
    final long numValidationErrors = recorder.getNumValidationErrors().getCount();
    final long numTransientErrors = recorder.getNumTransientErrors().getCount();
    if (numTransientErrors > 0) {
      numWrites = numTransientErrors < numWrites ? numWrites - numTransientErrors : 0;
      numReads = numTransientErrors < numReads ? numReads - numTransientErrors : 0;
    }
    recorder
        .globalRecorder
        .getOperationMetrics()
        .update(
            isSuccess ? recorder.numOperations : 0,
            numValidationErrors,
            numTransientErrors,
            latency,
            numWrites,
            numReads,
            bytesWritten,
            bytesRead);
  }

  public OperationMetricsRecorder getLocalRecorder(String streamName) {
    if (localRecorder != null) {
      return localRecorder;
    }
    final var currentLocalRecorders = localRecorders.get();
    return currentLocalRecorders != null
        ? currentLocalRecorders.get(streamName.toLowerCase())
        : null;
  }

  public OperationMetricsRecorder getLocalRecorder(String stream, MetricsRecordFinder recFinder) {
    if (localRecorder == null) {
      return null;
    }
    if (localRecorder.getKey().equalsIgnoreCase(stream)) {
      return localRecorder;
    }
    // this is needed only in rare cases where there is context chaining
    final var existing = localRecorder.globalRecorder;
    if (localRecorders.get() == null) {
      // right now a given request is processed only one thread at a time. So no need to lock
      localRecorders.compareAndSet(null, new HashMap<>());
    }
    // add the recorder
    return localRecorders
        .get()
        .compute(
            stream.toLowerCase(),
            (k, v) -> {
              if (v == null) {
                final var globalRecorder =
                    recFinder.getRecorder(
                        existing.getTenant(),
                        stream,
                        existing.getAppName(),
                        existing.getAppType(),
                        existing.getRequestType());
                return new LocalRecorder(globalRecorder);
              } else {
                return v;
              }
            });
  }

  private static final class LocalRecorder implements OperationMetricsRecorder {
    private final Moments decodeMoments;
    private final Moments validateMoments;
    private final Moments preProcessMoments;
    private final Moments postProcessMoments;
    private final Moments storageAccessMoments;
    private final Moments encodeMoments;
    private final Counter numWrites;
    private final Counter numReads;
    private final Counter numValidationErrors;
    private final Counter numTransientErrors;
    private final OperationMetricGroupRecorder globalRecorder;
    private long numOperations = 1;

    private LocalRecorder(OperationMetricGroupRecorder globalRecorder) {
      this.decodeMoments = Moments.create();
      this.validateMoments = Moments.create();
      this.preProcessMoments = Moments.create();
      this.postProcessMoments = Moments.create();
      this.storageAccessMoments = Moments.create();
      this.encodeMoments = Moments.create();
      this.numWrites = Counter.create();
      this.numReads = Counter.create();
      this.numValidationErrors = Counter.create();
      this.numTransientErrors = Counter.create();
      this.globalRecorder = globalRecorder;
    }

    public void incrementNumOperations() {
      ++numOperations;
    }

    public String getKey() {
      return globalRecorder.getStream();
    }

    @Override
    public Moments getDecodeMetrics() {
      return decodeMoments;
    }

    @Override
    public Moments getValidateMetrics() {
      return validateMoments;
    }

    @Override
    public Moments getPreProcessMetrics() {
      return preProcessMoments;
    }

    @Override
    public Moments getPostProcessMetrics() {
      return postProcessMoments;
    }

    @Override
    public Moments getStorageAccessMetrics() {
      return storageAccessMoments;
    }

    @Override
    public Moments getEncodeMetrics() {
      return encodeMoments;
    }

    @Override
    public Counter getNumWrites() {
      return numWrites;
    }

    @Override
    public Counter getNumReads() {
      return numReads;
    }

    @Override
    public Counter getNumValidationErrors() {
      return numValidationErrors;
    }

    @Override
    public Counter getNumTransientErrors() {
      return numTransientErrors;
    }

    private void commit() {
      // commit value to the global recorder
      globalRecorder.mergeFrom(this);
    }
  }
}
