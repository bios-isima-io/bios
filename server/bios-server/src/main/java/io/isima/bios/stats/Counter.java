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

import java.util.concurrent.atomic.AtomicLong;

/** A counter metric. Can be used as a simple counter for counting activities. */
public class Counter implements Countable, Metric<Counter> {
  private final AtomicLong counter;

  public static Counter create() {
    return new Counter();
  }

  public Counter() {
    counter = new AtomicLong(0L);
  }

  private Counter(long val) {
    counter = new AtomicLong(val);
  }

  /** Increments the counter by 1. */
  public void increment() {
    counter.incrementAndGet();
  }

  /**
   * Adds n to the counter.
   *
   * @param n number to add to the counter.
   */
  public void add(long n) {
    counter.addAndGet(n);
  }

  @Override
  public long getCount() {
    return counter.get();
  }

  @Override
  public Counter checkPoint() {
    long expectedVal = counter.get();
    if (expectedVal <= 0) {
      // optimize for no activity
      return null;
    }
    while (!counter.compareAndSet(expectedVal, 0)) {
      expectedVal = counter.get();
    }
    return expectedVal <= 0 ? null : new Counter(expectedVal);
  }

  public void clear() {
    counter.set(0);
  }
}
