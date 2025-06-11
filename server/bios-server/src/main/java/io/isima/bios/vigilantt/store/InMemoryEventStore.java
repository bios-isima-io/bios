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
package io.isima.bios.vigilantt.store;

import io.isima.bios.vigilantt.models.Event;
import io.isima.bios.vigilantt.models.EventFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryEventStore implements EventStore {

  Map<Long, Event> events;
  Map<Long, Event> anomalousEvents;

  public InMemoryEventStore() {
    events = new HashMap<>();
    anomalousEvents = new HashMap<>();
  }

  public void storeEvent(Event event) {
    events.put(System.nanoTime(), event);
  }

  public void storeAnomalousEvent(Event event) {
    anomalousEvents.put(System.nanoTime(), event);
  }

  public List<Event> getEvents(Long startTime, Long endTime, EventFilter filter) {
    return collectEvents(events, startTime, endTime, filter);
  }

  public List<Event> getAnomalousEvents(Long startTime, Long endTime, EventFilter filter) {
    return collectEvents(anomalousEvents, startTime, endTime, filter);
  }

  private List<Event> collectEvents(
      Map<Long, Event> eventMap, Long startTime, Long endTime, EventFilter filter) {
    List<Event> result = new ArrayList<>();
    for (Long key : eventMap.keySet()) {
      if (key >= startTime && key <= endTime) {
        result.add(eventMap.get(key));
      }
    }
    return result;
  }
}
