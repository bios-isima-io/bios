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

import io.isima.bios.stats.Countable;
import io.isima.bios.stats.Metric;
import lombok.Getter;

/** Holds operation metric values. */
@Getter
public class OperationMetrics implements Countable, Metric<OperationMetrics> {
  private long numSuccessfulOperations;

  private long numValidationErrors;
  private long numTransientErrors;

  private long numWrites;
  private long bytesWritten;

  private long numReads;
  private long bytesRead;

  private long latencyMin;
  private long latencyMax;
  private long latencySum;

  /**
   * Creates a fresh latency accumulator. This latency accumulator will use a default time unit of
   * micro seconds.
   *
   * @return a latency accumulator.
   */
  public static OperationMetrics create() {
    return new OperationMetrics();
  }

  public OperationMetrics() {
    init();
  }

  private OperationMetrics(OperationMetrics other) {
    this.numSuccessfulOperations = other.numSuccessfulOperations;

    this.numValidationErrors = other.numValidationErrors;
    this.numTransientErrors = other.numTransientErrors;

    this.numWrites = other.numWrites;
    this.bytesWritten = other.bytesWritten;

    this.numReads = other.numReads;
    this.bytesRead = other.bytesRead;

    this.latencySum = other.latencySum;
    this.latencyMax = other.latencyMax;
    this.latencyMin = other.latencyMin;
  }

  private void init() {
    this.numSuccessfulOperations = 0;
    this.numValidationErrors = 0;
    this.numTransientErrors = 0;

    this.numWrites = 0;
    this.bytesWritten = 0;

    this.numReads = 0;
    this.bytesRead = 0;

    this.latencyMin = Long.MAX_VALUE;
    this.latencyMax = Long.MIN_VALUE;
    this.latencySum = 0;
  }

  public long getLatencyMin() {
    return latencyMin != Long.MAX_VALUE ? latencyMin : 0;
  }

  public long getLatencyMax() {
    return latencyMax != Long.MIN_VALUE ? latencyMax : 0;
  }

  @Override
  public long getCount() {
    return numValidationErrors + numSuccessfulOperations;
  }

  @Override
  public synchronized OperationMetrics checkPoint() {
    if (getCount() > 0 || numTransientErrors > 0) {
      // check point only if there is some activity
      OperationMetrics standby = new OperationMetrics(this);
      this.init();
      return standby;
    } else {
      return null;
    }
  }

  public synchronized void update(
      long numSuccessfulOperations,
      long numValidationErrors,
      long numTransientErrors,
      long elapsed,
      long numWrites,
      long numReads,
      long bytesWritten,
      long bytesRead) {
    if (numSuccessfulOperations > 0) {
      this.numSuccessfulOperations += numSuccessfulOperations;
      latencyMin = Math.min(elapsed, latencyMin);
      latencyMax = Math.max(elapsed, latencyMax);
      latencySum += elapsed;
    }
    if (numTransientErrors == 0) {
      this.numWrites += numWrites;
      this.bytesWritten += bytesWritten;
      this.numReads += numReads;
      this.bytesRead += bytesRead;
    }
    this.numValidationErrors += numValidationErrors;
    this.numTransientErrors += numTransientErrors;
  }
}
