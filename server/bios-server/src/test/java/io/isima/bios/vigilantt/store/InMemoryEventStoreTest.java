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

import io.isima.bios.vigilantt.models.DefaultEventFilter;
import io.isima.bios.vigilantt.models.Event;
import io.isima.bios.vigilantt.models.EventFilter;
import io.isima.bios.vigilantt.models.SimpleEvent;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryEventStoreTest {
  private InMemoryEventStore eventStore;
  private final Event normalEvent = new SimpleEvent("Old man died");
  private final Event anomalousEvent = new SimpleEvent("Nuclear explosion");
  private final EventFilter eventFilter = new DefaultEventFilter();

  @Before
  public void setUp() {
    eventStore = new InMemoryEventStore();
  }

  @After
  public void tearDown() {}

  @Test
  public void testEventStore() throws Exception {
    // Ingest sample events in query window
    Long startTime = System.nanoTime();
    eventStore.storeEvent(normalEvent);
    eventStore.storeEvent(anomalousEvent);
    eventStore.storeAnomalousEvent(anomalousEvent);
    Long endTime = System.nanoTime();

    // Ingest sample events outside query window
    eventStore.storeEvent(normalEvent);
    eventStore.storeEvent(anomalousEvent);
    eventStore.storeAnomalousEvent(anomalousEvent);

    List<Event> eventsInQueryWindow = eventStore.getEvents(startTime, endTime, eventFilter);
    Assert.assertNotNull(eventsInQueryWindow);
    Assert.assertEquals(2, eventsInQueryWindow.size());

    List<Event> anomalousEventsInQueryWindow =
        eventStore.getAnomalousEvents(startTime, endTime, eventFilter);
    Assert.assertNotNull(anomalousEventsInQueryWindow);
    Assert.assertEquals(1, anomalousEventsInQueryWindow.size());
  }
}
