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

import io.isima.bios.req.IngestBulkRequest;
import java.util.List;
import java.util.UUID;
import javax.validation.constraints.NotNull;

/** DTO to carry IngestBulk operation request. */
public class IngestBulkRequestJson implements IngestBulkRequest {

  @NotNull private List<UUID> eventIds;

  @NotNull private List<String> events;

  @NotNull private Long streamVersion;

  public IngestBulkRequestJson() {}

  /**
   * Constructs a bulk ingest request.
   *
   * @param eventIds List of event IDs.
   * @param events List of event texts.
   * @param streamVersion Version of the target stream.
   * @throws IllegalArgumentException when one of the input parameters are null or sizes of lists of
   *     event IDs and event texts are different.
   */
  public IngestBulkRequestJson(List<UUID> eventIds, List<String> events, Long streamVersion) {
    if (eventIds == null || events == null || streamVersion == null) {
      throw new IllegalArgumentException(
          "BulkIngestRequest constructor parameters may not be null");
    }
    if (eventIds.size() != events.size()) {
      throw new IllegalArgumentException(
          String.format(
              "sizes of eventIds and eventTexts are different (%d and %d)",
              eventIds.size(), events.size()));
    }
    this.eventIds = eventIds;
    this.events = events;
    this.streamVersion = streamVersion;
  }

  public List<UUID> getEventIds() {
    return eventIds;
  }

  public List<String> getEvents() {
    return events;
  }

  public Long getStreamVersion() {
    return streamVersion;
  }
}
