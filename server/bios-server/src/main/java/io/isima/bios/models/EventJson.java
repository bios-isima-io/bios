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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

/** POJO to carry an event. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class EventJson implements Event {
  private UUID eventId;
  private Date ingestTimestamp;
  @JsonIgnore // handled by JsonAnyGetter and JsonAnySetter
  private Map<String, Object> attributes = new LinkedHashMap<>();

  public EventJson() {}

  /**
   * Construct with event ID and timestamp.
   *
   * <p>This constructor is package scope and meant to be called by Events.createEvent in the server
   * package.
   *
   * @param eventId Event ID
   * @param ingestTimestamp ingest timestamp
   */
  EventJson(final UUID eventId, final Date ingestTimestamp) {
    if (eventId == null || ingestTimestamp == null) {
      throw new IllegalArgumentException("UUID constructor parameters may not be null");
    }
    this.eventId = eventId;
    this.ingestTimestamp = ingestTimestamp;
  }

  public EventJson(UUID eventId, Date ingestTimestamp, Map<String, Object> attributes) {
    Objects.requireNonNull(attributes);
    this.eventId = eventId;
    this.ingestTimestamp = ingestTimestamp;
    this.attributes = attributes;
  }

  public EventJson(Event entry) {
    eventId = entry.getEventId();
    ingestTimestamp = entry.getIngestTimestamp();
    final var sourceAttributes = entry.getAttributes();
    attributes = new LinkedHashMap<>();
    for (var name : sourceAttributes.keySet()) {
      attributes.put(name, sourceAttributes.get(name));
    }
  }

  /**
   * Method to get eventId.
   *
   * @return the eventId
   */
  @Override
  public UUID getEventId() {
    return eventId;
  }

  /**
   * Method to set event Id.
   *
   * @param eventId the eventId to set
   */
  @Override
  public Event setEventId(final UUID eventId) {
    this.eventId = eventId;
    return this;
  }

  /**
   * Method to get timestamp when the event was ingested.
   *
   * @return the ingestionTimestamp
   */
  @Override
  public Date getIngestTimestamp() {
    return ingestTimestamp;
  }

  /**
   * Method to set timestamp when the event was ingested.
   *
   * @param ingestionTimestamp the ingestionTimestamp to set
   */
  @Override
  public Event setIngestTimestamp(final Date ingestionTimestamp) {
    this.ingestTimestamp = ingestionTimestamp;
    return this;
  }

  /**
   * Method to get map of attributes.
   *
   * @return the attributes
   */
  @Override
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  /**
   * Method to set a map of attributes.
   *
   * @param attributes the attributes to set
   */
  @Override
  public void setAttributes(final Map<String, Object> attributes) {
    Objects.requireNonNull(attributes);
    this.attributes = attributes;
  }

  /**
   * Jackson any getter for serializing attributes in flat manner.
   *
   * @return Key-value pairs of attributes
   */
  @Override
  @JsonAnyGetter
  public Map<String, Object> getAny() {
    return attributes;
  }

  /**
   * Jackson any setter for deserializing arbitrary properties in the event into attributes map.
   *
   * @param name Property name
   * @param value Property value
   */
  @Override
  @JsonAnySetter
  public void set(String name, Object value) {
    attributes.put(name, value);
  }

  @Override
  public Object get(String name) {
    return attributes.get(name);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    String delim = "";
    if (eventId != null) {
      sb.append("eventId=").append(eventId);
      delim = ", ";
    }
    if (ingestTimestamp != null) {
      sb.append(delim).append("ingestTimestamp=").append(ingestTimestamp.getTime());
      delim = ", ";
    }
    sb.append(delim).append("[");
    delim = "";
    for (Entry<String, Object> entry : attributes.entrySet()) {
      sb.append(delim).append(entry.getKey()).append("=").append(entry.getValue());
      delim = ", ";
    }
    return sb.append("]}").toString();
  }
}
