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
package io.isima.bios.data.impl.storage;

import java.util.concurrent.atomic.AtomicLong;

public class ContextMetricsCounter implements ContextMetricsCounterMBean {
  private final AtomicLong lookupCount;
  private final AtomicLong cacheHitCount;
  private final AtomicLong cacheItemsCount;

  public ContextMetricsCounter() {
    lookupCount = new AtomicLong();
    cacheHitCount = new AtomicLong();
    cacheItemsCount = new AtomicLong();
  }

  public void cacheLookedUp() {
    lookupCount.incrementAndGet();
  }

  public void cacheHit() {
    cacheHitCount.incrementAndGet();
  }

  /**
   * @deprecated Supported only by {@link DefaultContextCache}
   */
  @Deprecated
  public void cacheItemCreated() {
    cacheItemsCount.incrementAndGet();
  }

  /**
   * @deprecated Supported only by {@link DefaultContextCache}
   */
  @Deprecated
  public void cacheItemReleased() {
    cacheItemsCount.decrementAndGet();
  }

  @Override
  public long getLookupCount() {
    return lookupCount.get();
  }

  @Override
  public long getCacheHitCount() {
    return cacheHitCount.get();
  }

  @Override
  @Deprecated
  public long getCacheItemsCount() {
    return cacheItemsCount.get();
  }

  @Override
  public void reset() {
    lookupCount.set(0);
    cacheHitCount.set(0);
  }

  /** This is meant to be used for testing. */
  public void fullReset() {
    reset();
    cacheItemsCount.set(0);
  }
}
