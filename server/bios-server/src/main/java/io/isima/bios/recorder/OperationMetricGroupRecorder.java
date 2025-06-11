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

import io.isima.bios.models.AppType;
import io.isima.bios.stats.Counter;
import io.isima.bios.stats.Moments;
import java.util.Objects;
import lombok.Getter;

/** Metrics recorder with {@link io.isima.bios.stats.Metric} object(s). */
public class OperationMetricGroupRecorder implements OperationMetricsRecorder {
  private final String tenantName;
  private final String streamName;
  @Getter private final String appName;
  @Getter private final AppType appType;
  private final RequestType requestType;
  private final OperationMetrics operationMetrics;
  private final Moments decodeMoments;
  private final Moments validateMoments;
  private final Moments preProcessMoments;
  private final Moments dbPrepareMoments;
  private final Moments storageAccessMoments;
  private final Moments dbErrorMoments;
  private final Moments postProcessMoments;
  private final Moments encodeMoments;
  private final Counter recordsRead;
  private final Counter recordsReturned;
  private final Counter numValidationErrors;
  private final Counter numTransientErrors;

  /**
   * Constructor when stream name is not null for stream specific recordings.
   *
   * @param tenantName name of the tenant
   * @param streamName name of the stream
   * @param requestType name of the operation
   */
  OperationMetricGroupRecorder(
      String tenantName,
      String streamName,
      String appName,
      AppType appType,
      RequestType requestType) {
    this.tenantName = tenantName;
    this.streamName = streamName;
    this.appName = appName;
    this.appType = appType;
    this.requestType = requestType;
    this.operationMetrics = OperationMetrics.create();
    this.decodeMoments = Moments.create();
    this.validateMoments = Moments.create();
    this.preProcessMoments = Moments.create();
    this.dbPrepareMoments = Moments.create();
    this.storageAccessMoments = Moments.create();
    this.dbErrorMoments = Moments.create();
    this.postProcessMoments = Moments.create();
    this.encodeMoments = Moments.create();
    this.recordsRead = Counter.create();
    this.recordsReturned = Counter.create();
    this.numValidationErrors = Counter.create();
    this.numTransientErrors = Counter.create();
  }

  private OperationMetricGroupRecorder(
      OperationMetricGroupRecorder other, OperationMetrics latency) {
    this.tenantName = other.tenantName;
    this.streamName = other.streamName;
    this.appName = other.appName;
    this.appType = other.appType;
    this.requestType = other.requestType;
    this.operationMetrics = latency;
    this.decodeMoments = other.decodeMoments.checkPoint();
    this.validateMoments = other.validateMoments.checkPoint();
    this.preProcessMoments = other.preProcessMoments.checkPoint();
    this.dbPrepareMoments = other.dbPrepareMoments.checkPoint();
    this.storageAccessMoments = other.storageAccessMoments.checkPoint();
    this.dbErrorMoments = other.dbErrorMoments.checkPoint();
    this.postProcessMoments = other.postProcessMoments.checkPoint();
    this.encodeMoments = other.encodeMoments.checkPoint();
    this.recordsRead = other.recordsRead.checkPoint();
    this.recordsReturned = other.recordsReturned.checkPoint();
    this.numValidationErrors = other.numValidationErrors.checkPoint();
    this.numTransientErrors = other.numTransientErrors.checkPoint();
  }

  public String getTenant() {
    return tenantName;
  }

  public String getStream() {
    return streamName;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  /**
   * Tracks request Latency.
   *
   * <p>Sample usage: RequestTimer timer = getRequestLatency().createTimer() timer.start(); // do
   * some operation timer.stop(bytesIn, bytesOut) // on success OR timer.errorStop(bytesIn,
   * bytesOut) // on error
   *
   * <p>Alternate usage: RequestTimer timer = RequestTimer.create() timer.start(); // do some
   * operation getRequestLatency().attachTimer(timer); timer.stop() OR timer.errorStop();
   *
   * @return request latency accumulator
   */
  public OperationMetrics getOperationMetrics() {
    return operationMetrics;
  }

  /**
   * Sub operation latency of decoding a request.
   *
   * @return Latency accumulator for decode operation
   */
  @Override
  public Moments getDecodeMetrics() {
    return decodeMoments;
  }

  /**
   * Sub operation latency of encoding a response.
   *
   * @return Latency accumulator for encode operation
   */
  @Override
  public Moments getEncodeMetrics() {
    return encodeMoments;
  }

  /**
   * Merge local to global. This will make all the timers and counters of a given request merged
   * atomically into the recorder.
   */
  public void mergeFrom(OperationMetricsRecorder perRequestRecorder) {
    decodeMoments.mergeFrom(perRequestRecorder.getDecodeMetrics());
    dbErrorMoments.mergeFrom(perRequestRecorder.getDbErrorMetrics());
    storageAccessMoments.mergeFrom(perRequestRecorder.getStorageAccessMetrics());
    dbPrepareMoments.mergeFrom(perRequestRecorder.getDbPrepareMetrics());
    preProcessMoments.mergeFrom(perRequestRecorder.getPreProcessMetrics());
    postProcessMoments.mergeFrom(perRequestRecorder.getPostProcessMetrics());
    validateMoments.mergeFrom(perRequestRecorder.getValidateMetrics());
    encodeMoments.mergeFrom(perRequestRecorder.getEncodeMetrics());
    final var recordsReadCounter = perRequestRecorder.getNumReads();
    if (recordsReadCounter != null) {
      recordsRead.add(recordsReadCounter.getCount());
    }
    final var recordsReturnedCounter = perRequestRecorder.getNumWrites();
    if (recordsReturnedCounter != null) {
      recordsReturned.add(recordsReturnedCounter.getCount());
    }
    numValidationErrors.add(perRequestRecorder.getNumValidationErrors().getCount());
    numTransientErrors.add(perRequestRecorder.getNumTransientErrors().getCount());
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
  public Moments getDbPrepareMetrics() {
    return dbPrepareMoments;
  }

  @Override
  public Moments getStorageAccessMetrics() {
    return storageAccessMoments;
  }

  @Override
  public Moments getDbErrorMetrics() {
    return dbErrorMoments;
  }

  @Override
  public Moments getPostProcessMetrics() {
    return postProcessMoments;
  }

  @Override
  public Counter getNumReads() {
    return recordsRead;
  }

  @Override
  public Counter getNumWrites() {
    return recordsReturned;
  }

  @Override
  public Counter getNumValidationErrors() {
    return numValidationErrors;
  }

  @Override
  public Counter getNumTransientErrors() {
    return numTransientErrors;
  }

  OperationsMetricsDimensions toRecorderKey() {
    return new OperationsMetricsDimensions(tenantName, streamName, appName, appType, requestType);
  }

  OperationMetricGroupRecorder checkpoint() {
    OperationMetrics latency = operationMetrics.checkPoint();
    return (latency != null) ? new OperationMetricGroupRecorder(this, latency) : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperationMetricGroupRecorder recorder = (OperationMetricGroupRecorder) o;
    return streamName.equals(recorder.streamName)
        && tenantName.equals(recorder.tenantName)
        && appName.equals(recorder.appName)
        && appType.equals(recorder.appType)
        && requestType.getRequestNumber() == recorder.getRequestType().getRequestNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, streamName, appName, appType, requestType);
  }
}
