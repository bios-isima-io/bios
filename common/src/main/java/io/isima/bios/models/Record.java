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
package io.isima.bios.models;

import java.util.Collection;
import java.util.UUID;

/** Represents a record composed of a set of attributes/columns. */
public interface Record {
  /**
   * Get eventId (optional). Returns null if no UUID in record.
   *
   * @return eventId, null if no eventId
   */
  UUID getEventId();

  /**
   * Return time stamp (optional). Returns null if no event Id.
   *
   * @return ingest time stamp
   */
  Long getTimestamp();

  /**
   * Returns the entire attribute collection within the record along with their type information.
   *
   * @return an attribute collection.
   */
  Collection<? extends Attribute> attributes();

  /**
   * Get an attribute by name.
   *
   * <p>Assumption: There is only one attribute for a given name in a given record. So type is not
   * necessary to be provided to fetch the attribute.
   *
   * @param name name of the attribute to fetch
   * @return attribute value
   */
  AttributeValue getAttribute(String name);
}
