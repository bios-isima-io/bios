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

import io.isima.bios.models.Event;
import java.util.Collection;
import java.util.List;

interface ContextCache {

  static ContextCache create() {
    return new SimpleContextCache();
  }

  /**
   * Method to get an event in the cache specified by a key.
   *
   * @param key Key that specifies an event.
   * @return Event object if an entry with the specified key exists, otherwise null.
   */
  Event get(List<Object> key);

  /**
   * Put an entry associated with the given key.
   *
   * <p>If an existing entry with the specified key exists, the method replaces it by the given
   * event if the version of the event is newer than existing. If the version of the given event is
   * older than existing, the method leaves the existing entry.
   *
   * @param key Lookup key
   * @param event The event to put.
   * @param version Version attribute value or Timestamp, depending on context's versionAttribute.
   * @throws IllegalArgumentException When key or entry is null.
   */
  void put(List<Object> key, Event event, long version);

  /**
   * Delete an entry associated with the given key.
   *
   * <p>If an entry for the key exists and its version is newer than given version, the method skips
   * deleting the entry.
   *
   * @param key Key that specifies the target entry.
   * @param version Version attribute value or Timestamp, depending on context's versionAttribute.
   */
  void delete(List<Object> key, long version);

  /**
   * Return number of context entries.
   *
   * @return number of context entries
   */
  int count();

  /** Returns capacity. */
  int capacity();

  /** Returns a collection of primary keys. */
  Collection<List<Object>> getKeys();
}
