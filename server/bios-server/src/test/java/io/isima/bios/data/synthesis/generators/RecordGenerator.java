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
package io.isima.bios.data.synthesis.generators;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.util.Date;
import java.util.Map;

/** Per-stream synthetic record generator. */
public class RecordGenerator {

  private final String tenantName;
  private final BiosStreamConfig streamConfig;
  private final Map<String, AttributeGenerator<?>> attributeGenerators;

  public RecordGenerator(AttributeGeneratorsCreator creator)
      throws TfosException, ApplicationException {
    this.tenantName = creator.getTenantName();
    this.streamConfig = creator.getStreamConfig();
    attributeGenerators = creator.create();
  }

  public Event generate() {
    final EventJson event = new EventJson();
    final var eventId = UUIDs.timeBased();
    event.setEventId(eventId);
    event.setIngestTimestamp(new Date(UUIDs.unixTimestamp(eventId)));
    attributeGenerators
        .keySet()
        .forEach((attr) -> event.set(attr, attributeGenerators.get(attr).generate()));
    return event;
  }

  public String getTenantName() {
    return tenantName;
  }

  public BiosStreamConfig getStreamConfig() {
    return streamConfig;
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{")
            .append("tenant=")
            .append(tenantName)
            .append(", stream=")
            .append(streamConfig.getName())
            .append("}");
    return sb.toString();
  }
}
