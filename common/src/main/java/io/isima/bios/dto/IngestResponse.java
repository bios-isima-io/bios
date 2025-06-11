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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.isima.bios.models.Attribute;
import io.isima.bios.models.AttributeValue;
import io.isima.bios.models.Record;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

/** A POJO class which holds the response of ingest action. */
public class IngestResponse implements IngestResult, Record {
  UUID eventId;
  long timestamp;

  public IngestResponse() {}

  public IngestResponse(UUID eventId, Long timestamp) {
    this.eventId = eventId;
    this.timestamp = timestamp;
  }

  public IngestResponse(UUID eventId, Date timestamp) {
    this.eventId = eventId;
    this.timestamp = timestamp.getTime();
  }

  /**
   * Get event id.
   *
   * @return the eventId
   */
  @Override
  public UUID getEventId() {
    return eventId;
  }

  @JsonIgnore
  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @JsonIgnore
  @Override
  public Collection<? extends Attribute> attributes() {
    return Collections.emptyList();
  }

  @JsonIgnore
  @Override
  public AttributeValue getAttribute(String name) {
    // TODO(BIOS-1636): Should we throw an exception?
    return null;
  }

  /**
   * Set event id.
   *
   * @param eventId the eventId to set
   */
  public void setEventId(UUID eventId) {
    this.eventId = eventId;
  }

  /**
   * Get ingest timetamp.
   *
   * @return the timestamp
   */
  @Override
  public Date getIngestTimestamp() {
    return timestamp > 0 ? new Date(timestamp) : null;
  }

  /**
   * Set ingest timetamp.
   *
   * @param timestamp the timestamp to set
   */
  public void setIngestTimestamp(Date timestamp) {
    this.timestamp = timestamp.getTime();
  }

  @Override
  public String toString() {
    return "{eventId=" + eventId + ", ingestTimestamp=" + timestamp + "}";
  }
}
