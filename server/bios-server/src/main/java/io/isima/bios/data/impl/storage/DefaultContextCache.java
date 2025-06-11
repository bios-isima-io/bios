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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache class to manage and provide in-memory context entry lookup.
 *
 * <p>All methods must be implemented to be thread safe.
 *
 * @deprecated This class would be replaced by {@link ConcurrentLinkedHashMapContextCache}, but
 *     we'll keep this implementation for a while as a reference implementation.
 */
@Deprecated
public class DefaultContextCache implements ContextCache {

  protected final Map<List<Object>, ContextCacheItem<Event>> cache;
  private final int maxItems;

  // Head and tail pointers of context cache items chain.
  protected final ContextCacheItem head;
  protected final ContextCacheItem tail;

  public static final long STALE_TIME_WINDOW = 10000;

  /**
   * Constructor of the class.
   *
   * @param maxItems Maximum number of cache items.
   * @deprecated Replaced by class {@link ConcurrentLinkedHashMapContextCache}
   */
  @Deprecated
  public DefaultContextCache(int maxItems) {
    cache = new HashMap<>();
    head = new ContextCacheItem("_head", null, 0);
    tail = new ContextCacheItem("_tail", null, 0);
    head.next = tail;
    tail.prev = head;
    this.maxItems = maxItems;
  }

  @Override
  public Event get(List<Object> key) {
    Preconditions.checkArgument(key != null, "Lookup key may not be null");
    synchronized (cache) {
      final var item = cache.get(key);
      if (item == null) {
        return null;
      }
      Event result = item.getValue() != null ? new EventJson(item.getValue()) : null;
      // we refresh the item only when the entry is not a deleted item.
      if (result != null) {
        item.removeFromChain();
        head.insertAfter(item);
      }
      return result;
    }
  }

  @Override
  public void put(List<Object> key, Event event, long version) {
    Preconditions.checkArgument(key != null, "key may not be null");
    Preconditions.checkArgument(event != null, "event may not be null");

    ContextCacheItem toPut = new ContextCacheItem(key, event, version);
    ContextCassStream.getMetrics().cacheItemCreated();

    synchronized (cache) {
      // delete the oldest one if number of items reaches maximum
      ContextCacheItem existing = cache.putIfAbsent(key, toPut);
      // Newer item can override existing.
      if (existing != null) {
        ContextCassStream.getMetrics().cacheItemReleased();
        if (toPut.getVersion() > existing.getVersion()) {
          cache.put(key, toPut);
        } else {
          toPut = existing;
        }
        existing.removeFromChain();
      }
      head.insertAfter(toPut);

      // delete the oldest item if overflow
      if (cache.size() > maxItems) {
        ContextCacheItem toRemove = tail.prev;
        cache.remove(toRemove.getKey());
        toRemove.removeFromChain();
        ContextCassStream.getMetrics().cacheItemReleased();
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
      if (existing != null) {
        existing.removeFromChain();
      }
      // Newer item can override existing.
      if (existing == null || version > existing.getVersion()) {
        ContextCacheItem newItem = new ContextCacheItem(key, null, version);
        cache.put(key, newItem);
        existing = newItem;
      }
      // move the item to the head so that the entry won't be flushed soon
      head.insertAfter(existing);
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
