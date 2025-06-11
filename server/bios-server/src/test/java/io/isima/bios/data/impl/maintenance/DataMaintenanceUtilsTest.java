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
package io.isima.bios.data.impl.maintenance;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

public class DataMaintenanceUtilsTest {

  @Test
  public void getEventsInRangeTest() {
    final List<Event> sourceEvents =
        List.of(
            new EventJson(UUID.randomUUID(), new Date(10L), Map.of("a", "one")),
            new EventJson(UUID.randomUUID(), new Date(20L), Map.of("a", "two")),
            new EventJson(UUID.randomUUID(), new Date(30L), Map.of("a", "three")),
            new EventJson(UUID.randomUUID(), new Date(30L), Map.of("a", "four")),
            new EventJson(UUID.randomUUID(), new Date(40L), Map.of("a", "five")),
            new EventJson(UUID.randomUUID(), new Date(50L), Map.of("a", "six")),
            new EventJson(UUID.randomUUID(), new Date(50L), Map.of("a", "seven")),
            new EventJson(UUID.randomUUID(), new Date(60L), Map.of("a", "eight")));

    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 5, 65);
      assertThat(events, equalTo(sourceEvents));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 10, 60);
      assertThat(events, equalTo(sourceEvents.subList(0, 7)));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 20, 45);
      assertThat(events, equalTo(sourceEvents.subList(1, 5)));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 15, 45);
      assertThat(events, equalTo(sourceEvents.subList(1, 5)));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 30, 55);
      assertThat(events, equalTo(sourceEvents.subList(2, 7)));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 30, 50);
      assertThat(events, equalTo(sourceEvents.subList(2, 5)));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 65, 70);
      assertThat(events.size(), is(0));
    }
    {
      final var events = DataMaintenanceUtils.getEventsInRange(sourceEvents, 0, 5);
      assertThat(events.size(), is(0));
    }
  }

  @Test
  public void getEmptyEventsInRangeTest() {
    assertThat(DataMaintenanceUtils.getEventsInRange(List.of(), 5, 65), equalTo(List.of()));
  }
}
