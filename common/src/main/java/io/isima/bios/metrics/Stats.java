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

/**
 * Accumulates stats. It is the responsibility of layers above to build periodicity and check
 * pointing above the stats accumulator.
 *
 * <p>This is not threadsafe on its own. It is assumed that the {@code DurationRecorder} controls
 * usage of this layer and DurationRecorder is thread safe.
 */
public class Stats {
  private int count;
  private int errorCount;
  private long errorSum;
  private long min;
  private long max;
  private long sum;
  private long dataBytesIn;
  private long dataBytesOut;

  public Stats() {
    init();
  }

  public void init() {
    count = 0;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    sum = 0L;
    dataBytesIn = 0L;
    dataBytesOut = 0L;
  }

  public void push(long value) {
    push(value, 0L, 0L);
  }

  public void push(long value, long dataBytesIn, long dataBytesOut) {
    ++count;

    max = Math.max(max, value);
    min = Math.min(min, value);
    sum += value;

    this.dataBytesIn += dataBytesIn;
    this.dataBytesOut += dataBytesOut;
  }

  public void pushError(long value, long dataBytesIn) {
    ++errorCount;

    errorSum += value;

    this.dataBytesIn += dataBytesIn;
  }

  /**
   * Get the overall count for this cycle.
   *
   * <p>NOTE: All accessors should be done only after checkpoint on the standby stats to avoid any
   * thread safety issues.
   *
   * @return overall count
   */
  public int getOverallCount() {
    return this.count + this.errorCount;
  }

  public int getCount() {
    return this.count;
  }

  public int getErrorCount() {
    return this.errorCount;
  }

  public long getMin() {
    return count == 0 ? 0L : min;
  }

  public long getMax() {
    return count == 0 ? 0L : max;
  }

  public long getSum() {
    return count == 0 ? 0L : sum;
  }

  public long getErrorSum() {
    return errorCount == 0 ? 0L : errorSum;
  }

  public long getDataBytesIn() {
    return dataBytesIn;
  }

  public long getDataBytesOut() {
    return dataBytesOut;
  }

  public String report() {
    // min, max, count, sum
    return String.format("%d,%d,%d,%d", getMin(), getMax(), getCount(), getSum());
  }
}
