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

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import lombok.Getter;

/**
 * Timer interface for calculating simple elapsed times.
 *
 * <p>Start time is set implicitly when timer is created.
 */
public class Timer implements Closeable {
  @Getter private final Instant startTime;
  private final Clock clock;
  private final List<LongConsumer> durationConsumers;

  private volatile boolean stopped = false;
  private long elapsedMicros;

  public static Timer create() {
    return new Timer();
  }

  public Timer(LongConsumer consumer) {
    this.durationConsumers = new ArrayList<>();
    this.durationConsumers.add(consumer);
    this.clock = ClockProvider.getClock();
    this.startTime = clock.instant();
  }

  public Timer() {
    this.durationConsumers = new ArrayList<>();
    this.clock = ClockProvider.getClock();
    this.startTime = clock.instant();
  }

  public long stopWithoutCommit() {
    if (stopped) {
      // avoid stopping twice
      return elapsedMicros;
    }
    Instant endTime = clock.instant();
    elapsedMicros = Duration.between(startTime, endTime).toNanos() / 1000;
    stopped = true;
    return elapsedMicros;
  }

  /**
   * Bind the timer to the consumer. Allows for optional late binding of the timer.
   *
   * @param consumer consumer that consumes the timer data when the timer stops
   */
  public void attachConsumer(LongConsumer consumer) {
    this.durationConsumers.add(consumer);
  }

  /**
   * Flushes the timer value to the attached consumers.
   *
   * <p>If the timer is still runnning, it is stopped first.
   */
  public long commit() {
    stopWithoutCommit();
    if (durationConsumers.isEmpty()) {
      return elapsedMicros;
    }
    var elapsedTimeToCommit = elapsedMicros / durationConsumers.size();
    for (final var consumer : durationConsumers) {
      consumer.accept(elapsedTimeToCommit);
    }
    return elapsedMicros;
  }

  @Override
  public void close() {
    commit();
  }
}
