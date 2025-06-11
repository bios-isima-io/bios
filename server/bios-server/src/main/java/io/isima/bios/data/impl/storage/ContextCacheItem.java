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

import lombok.ToString;

@ToString
public class ContextCacheItem<ValueT> {

  private final Object key;
  private ValueT value;
  private long version;

  // Required only by DefaultContextCache. These won't be necessary when the implementation is
  // removed.
  @Deprecated ContextCacheItem prev;
  @Deprecated ContextCacheItem next;

  /**
   * Cache item constructor.
   *
   * <p>Properties of this class are immutable. Only the way to specify properties is via this
   * constructor.
   *
   * @param key Cache key
   * @param entry Cache entry
   * @param version Value of versionAttribute / Timestamp of this item
   */
  public ContextCacheItem(Object key, ValueT entry, long version) {
    this.key = key;
    this.value = entry;
    this.version = version;
    prev = null;
    next = null;
  }

  void update(ValueT entry, long version) {
    this.value = entry;
    this.version = version;
  }

  public Object getKey() {
    return key;
  }

  public ValueT getValue() {
    return value;
  }

  public long getVersion() {
    return version;
  }

  /**
   * Remove this item from the chain of links.
   *
   * <p>The method assumes non-null prev and next members. User of this method must guarantee it.
   */
  void removeFromChain() {
    next.prev = prev;
    prev.next = next;
  }

  /**
   * Insert an item after this item.
   *
   * <p>The method assumes non-null member 'next'. User of this method must guarantee it.
   *
   * @param toInsert Item to insert
   */
  void insertAfter(ContextCacheItem toInsert) {
    toInsert.next = next;
    toInsert.prev = this;
    next.prev = toInsert;
    next = toInsert;
  }
}
