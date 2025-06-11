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

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public interface Event {
  /**
   * Gets the eventId uniquely identifying this event.
   *
   * @return the eventId
   */
  UUID getEventId();

  /**
   * Sets the eventId that uniquely identifies this event.
   *
   * @param eventId a UUID uniquely identifying the event
   * @return this event
   */
  Event setEventId(UUID eventId);

  /**
   * Gets the time at which this event was ingested.
   *
   * @return the ingestionTimestamp
   */
  Date getIngestTimestamp();

  /**
   * Sets the time at which this event was ingested.
   *
   * @param ingestionTimestamp Ingestion timestamp
   * @return this event
   */
  Event setIngestTimestamp(Date ingestionTimestamp);

  /**
   * Gets a map of attributes associated with this event along with attribute name.
   *
   * @return a map of attributes
   */
  Map<String, Object> getAttributes();

  /**
   * Sets a map of attributes associated with this event along with attribute name.
   *
   * @param attributes map of attributes along with attribute name of each
   */
  void setAttributes(Map<String, Object> attributes);

  /**
   * Gets a map of attributes.
   *
   * <p>Retaining this as it was available in the original model to avoid breaking API. Maybe
   * removed in the future.
   *
   * @return a map of objects
   */
  Map<String, Object> getAny();

  /**
   * Sets a given attribute, given name and attribute.
   *
   * @param name Name of the attribute
   * @param value attribute value
   */
  void set(String name, Object value);

  /**
   * Get an attribute specified by the name.
   *
   * @param name name of the attribute
   * @return attribute value
   */
  Object get(String name);
}
