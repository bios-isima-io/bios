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
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache class to manage and provide in-memory context entry lookup.
 *
 * <p>We keep the implementation simple for the first implementation, but we will have more features
 * eventually such as:
 *
 * <dl>
 *   <li>Capability to limit number of cache items
 *   <li>Cache item TTL
 *   <li>Support for special match functions (masks, case insensitive match, etc.)
 *   <li>Metrics, such as cache hit rate
 * </dl>
 *
 * <p>All methods must be implemented to be thread safe.
 */
class SimpleContextCache implements ContextCache {

  private final Map<List<Object>, ContextCacheItem<Event>> cache;

  public SimpleContextCache() {
    cache = new ConcurrentHashMap<>();
  }

  @Override
  public Event get(List<Object> key) {
    Preconditions.checkArgument(key != null, "Lookup key may not be null");
    final var item = cache.get(key);
    return item != null && item.getValue() != null ? new EventJson(item.getValue()) : null;
  }

  @Override
  public void put(List<Object> key, Event event, long version) {
    Preconditions.checkArgument(key != null, "key may not be null");
    Preconditions.checkArgument(event != null, "event may not be null");
    ContextCacheItem item = new ContextCacheItem(key, event, version);
    // We need to lock the map for updates to prevent the item for the key from changing while
    // checking versions.
    synchronized (cache) {
      ContextCacheItem existing = cache.putIfAbsent(key, item);
      // Newer item can override existing.
      if (existing != null && item.getVersion() > existing.getVersion()) {
        cache.put(key, item);
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
    synchronized (cache) {
      ContextCacheItem existing = cache.get(key);
      // Newer item can override existing.
      if (existing == null || version > existing.getVersion()) {
        ContextCacheItem newItem = new ContextCacheItem(key, null, version);
        cache.put(key, newItem);
      }
    }
  }

  @Override
  public int count() {
    return cache.size();
  }

  @Override
  public int capacity() {
    return cache.size();
  }

  @Override
  public Collection<List<Object>> getKeys() {
    return cache.keySet();
  }
}
