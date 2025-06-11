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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A discrete clock for testing. Uses Java 8 Clock API to plug clocks that control time for testing
 * purposes.
 */
public class TestClock extends Clock {
  private final AtomicInteger timesCalled = new AtomicInteger(0);
  private final AtomicReference<Instant> currentInstantRef = new AtomicReference<>(Instant.now());

  private volatile int times = 1;
  private volatile long skipNanos = 1000;

  public void controlClock(int times, long skipNanos) {
    this.timesCalled.set(0);
    this.currentInstantRef.set(Instant.now());
    this.times = times;
    this.skipNanos = skipNanos;
  }

  @Override
  public ZoneId getZone() {
    return systemDefaultZone().getZone();
  }

  @Override
  public Clock withZone(ZoneId zone) {
    throw new UnsupportedOperationException("Cannot change zone for this clock");
  }

  @Override
  public Instant instant() {
    final var counter = timesCalled.incrementAndGet();
    Instant currentInstant = currentInstantRef.get();
    if (counter % times == 0) {
      currentInstantRef.compareAndSet(currentInstant, currentInstant.plusNanos(skipNanos));
    }
    return currentInstant;
  }
}
