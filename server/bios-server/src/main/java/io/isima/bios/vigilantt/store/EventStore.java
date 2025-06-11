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
import java.util.List;

/** Interface to interact with events in storage. */
public interface EventStore {

  /**
   * Persist an event
   *
   * @param event Input event POJO
   */
  void storeEvent(Event event);

  /**
   * Persist an anomalous event
   *
   * @param event Input event POJO
   */
  void storeAnomalousEvent(Event event);

  /**
   * Get the list of events in a time range matching the filter criteria
   *
   * @param startTime start time
   * @param endTime end time
   * @param filter filter criteria
   * @return list of events
   */
  List<Event> getEvents(Long startTime, Long endTime, EventFilter filter);

  /**
   * Get the list of identified anomalous events in a time range matching the filter criteria
   *
   * @param startTime start time
   * @param endTime end time
   * @param filter filter criteria
   * @return list of events
   */
  List<Event> getAnomalousEvents(Long startTime, Long endTime, EventFilter filter);
}
