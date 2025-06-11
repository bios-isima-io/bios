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
package io.isima.bios.stats;

import lombok.Getter;

/**
 * Latency accumulator and metering.
 *
 * <p>TODO: In the future, latency can also support metering and quantiles.
 */
public class Moments implements Countable, Metric<Moments> {
  private long min;
  private long max;
  @Getter private long sum;
  @Getter private long count;

  public Moments() {
    init();
  }

  private Moments(Moments other) {
    this.count = other.count;
    this.max = other.max;
    this.min = other.min;
    this.sum = other.sum;
  }

  private void init() {
    this.min = Long.MAX_VALUE;
    this.max = Long.MIN_VALUE;
    this.sum = 0;
    this.count = 0;
  }

  /**
   * Creates a fresh latency accumulator. This latency accumulator will use a default time unit of
   * micro seconds.
   *
   * @return a latency accumulator.
   */
  public static Moments create() {
    return new Moments();
  }

  /**
   * Creates a new timer. When the timer stops, latency will be accumulated.
   *
   * @return a new timer.
   */
  public Timer createTimer() {
    return new Timer(this::update);
  }

  /**
   * Attach an existing timer to this Latency. Latency will be accumulated for this timer when the
   * timer commits.
   *
   * @param timer a timer that was externally created.
   */
  public void attachTimer(Timer timer) {
    if (timer == null) {
      return;
    }
    timer.attachConsumer(this::update);
  }

  /**
   * Returns min latency seen so far.
   *
   * @return min latency in units set for this object.
   */
  public long getMin() {
    return (count == 0) ? 0 : min;
  }

  /**
   * Returns max latency seen so far.
   *
   * @return max latency in units set for this object.
   */
  public long getMax() {
    return (count == 0) ? 0 : max;
  }

  /** Merge from */
  public void mergeFrom(Moments other) {
    if (other == null) {
      return;
    }
    merge(other);
  }

  /**
   * Slice into pieces. Finds the share of this in the pie.
   *
   * @param total all
   */
  public void slice(int total) {
    if (this.count <= 0) {
      return;
    }
    this.sum = this.sum / total;
  }

  @Override
  public synchronized Moments checkPoint() {
    if (count > 0) {
      // check point only if there is some activity
      Moments standby = new Moments(this);
      this.init();
      return standby;
    } else {
      return null;
    }
  }

  private synchronized void update(long elapsed) {
    this.count++;
    min = Math.min(min, elapsed);
    max = Math.max(max, elapsed);
    sum += elapsed;
  }

  private synchronized void merge(Moments other) {
    this.count += other.getCount();
    this.min = Math.min(min, other.getMin());
    this.max = Math.max(max, other.getMax());
    this.sum += other.getSum();
  }
}
