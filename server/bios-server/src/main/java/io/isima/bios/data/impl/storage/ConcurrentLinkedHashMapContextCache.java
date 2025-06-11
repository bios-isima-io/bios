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

import com.google.common.base.Preconditions;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.isima.bios.models.Event;
import io.isima.bios.models.ObjectListEventValue;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache class to manage and provide in-memory context entry lookup.
 *
 * <p>All methods must be implemented to be thread safe.
 */
public class ConcurrentLinkedHashMapContextCache implements ContextCache {
  private static final Logger logger =
      LoggerFactory.getLogger(ConcurrentLinkedHashMapContextCache.class);

  protected final ConcurrentMap<List<Object>, ContextCacheItem<ObjectListEventValue>> cache;
  protected final Map<String, Integer> attributeNameToIndex;
  protected final List<String> attributeNames;

  private final int maxItems;

  /**
   * Constructor of the class.
   *
   * @param maxItems Maximum number of cache items.
   */
  public ConcurrentLinkedHashMapContextCache(int maxItems, List<String> attributeNames) {
    cache =
        new ConcurrentLinkedHashMap.Builder<List<Object>, ContextCacheItem<ObjectListEventValue>>()
            .maximumWeightedCapacity(maxItems)
            .build();
    this.maxItems = maxItems;
    attributeNameToIndex = new LinkedHashMap<>();
    for (int index = 0; index < attributeNames.size(); ++index) {
      attributeNameToIndex.put(attributeNames.get(index), index);
    }
    this.attributeNames = attributeNames;
  }

  @Override
  public Event get(List<Object> key) {
    Preconditions.checkArgument(key != null, "Lookup key may not be null");
    final var item = cache.get(key);
    final var value = item == null ? null : item.getValue();
    return value == null ? null : value.toEvent(attributeNameToIndex);
  }

  @Override
  public void put(List<Object> key, Event event, long version) {
    Preconditions.checkArgument(key != null, "key may not be null");
    Preconditions.checkArgument(event != null, "event may not be null");

    var toPut =
        new ContextCacheItem<>(key, new ObjectListEventValue(event, attributeNames), version);
    final var existing = cache.putIfAbsent(key, toPut);
    logger.trace("{} put {}: existing {}", LocalDateTime.now(), toPut, existing);

    final ContextCacheItem current = existing != null ? existing : toPut;
    synchronized (current) {
      if (toPut.getVersion() > current.getVersion()) {
        current.update(toPut.getValue(), toPut.getVersion());
        logger.trace("{} updated {}", LocalDateTime.now(), toPut);
      }
    }
  }

  @Override
  public void delete(List<Object> key, long version) {
    Preconditions.checkArgument(key != null, "key may not be null");
    // We cannot simply remove the target because two delete requests may come close in time and
    // these may be in different order on the other nodes when running a multi-node service. Our
    // policy to maintain write priority is "the latter wins". So we need to leave a cache item with
    // null cache value to keep version.
    final var toPut = new ContextCacheItem<ObjectListEventValue>(key, null, version);
    final var existing = cache.putIfAbsent(key, toPut);
    logger.trace("{} delete {}: existing {}", LocalDateTime.now(), toPut, existing);
    final var current = existing != null ? existing : toPut;
    synchronized (current) {
      if (toPut.getVersion() > current.getVersion()) {
        current.update(null, version);
        logger.trace("{} deleted {}", LocalDateTime.now(), toPut);
      }
    }
  }

  @Override
  public int count() {
    return cache.size();
  }

  @Override
  public int capacity() {
    return maxItems;
  }

  @Override
  public Collection<List<Object>> getKeys() {
    return cache.keySet();
  }
}
