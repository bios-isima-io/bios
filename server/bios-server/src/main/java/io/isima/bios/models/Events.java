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

import com.datastax.driver.core.utils.UUIDs;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

/** Class that provides utility methods to handle Event class objects. */
public class Events {

  /**
   * Create an event with v1 UUID eventId.
   *
   * <p>This method creates an event with specified UUID v1 type eventId. Ingest timestamp is also
   * set with timestamp of the eventId.
   *
   * @param eventId Event ID. Its type must be version 1 UUID
   * @return Created Event object.
   * @throws IllegalArgumentException This exception is thrown when specified eventId is null or the
   *     UUID version is not 1 (time UUID).
   */
  public static Event createEvent(final UUID eventId) {
    if (eventId == null) {
      throw new IllegalArgumentException("eventId may not be null");
    }
    if (eventId.version() != 1) {
      throw new IllegalArgumentException(
          "eventId must be v1 UUID but v" + eventId.version() + "was given");
    }
    return new EventJson(eventId, new Date(UUIDs.unixTimestamp(eventId)));
  }

  public static boolean equalAttributesOf(Event e1, Event e2) {
    if (e1 == null || e2 == null) {
      return e1 == e2;
    }
    final var attributes1 = e1.getAttributes();
    final var attributes2 = e2.getAttributes();
    if (attributes1.size() != attributes2.size()) {
      return false;
    }
    for (var name : attributes1.keySet()) {
      if (!Objects.equals(attributes1.get(name), attributes2.get(name))) {
        return false;
      }
    }
    return true;
  }
}
