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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class ObjectListEventValueTest {
  private Map<String, Integer> nameToIndex;
  private List<String> attributeNames;

  @Before
  public void setUp() {
    nameToIndex = new ConcurrentHashMap<>(Map.of("one", 0, "two", 1, "three", 2));
    attributeNames = List.of("one", "two", "three");
  }

  @Test
  public void createEvent() {
    final var origEvent = new EventJson(UUID.randomUUID(), new Date(System.currentTimeMillis()));
    origEvent.set("one", "11111");
    origEvent.set("two", 22222L);
    final var values = new ObjectListEventValue(origEvent, attributeNames);
    final var restoredEvent = values.toEvent(nameToIndex);
    assertThat(restoredEvent.getEventId(), equalTo(origEvent.getEventId()));
    assertThat(restoredEvent.getIngestTimestamp(), equalTo(origEvent.getIngestTimestamp()));
    assertThat(restoredEvent.get("one"), equalTo(origEvent.get("one")));
    assertThat(restoredEvent.get("two"), equalTo(origEvent.get("two")));
    assertThat(restoredEvent.get("three"), equalTo(origEvent.get("three")));
    assertThat(restoredEvent.get("one"), equalTo("11111"));
    assertThat(restoredEvent.get("two"), equalTo(22222L));
    assertThat(restoredEvent.get("three"), equalTo(null));
  }

  @Test
  public void removeAttributes() {
    final var origEvent = new EventJson(UUID.randomUUID(), new Date(System.currentTimeMillis()));
    origEvent.set("one", "11111");
    origEvent.set("two", 22222L);
    origEvent.set("three", "33333");
    final var values = new ObjectListEventValue(origEvent, attributeNames);
    final var restoredEvent = values.toEvent(nameToIndex);
    assertThat(restoredEvent.getEventId(), equalTo(origEvent.getEventId()));
    assertThat(restoredEvent.getIngestTimestamp(), equalTo(origEvent.getIngestTimestamp()));
    assertThat(restoredEvent.get("one"), equalTo(origEvent.get("one")));
    assertThat(restoredEvent.get("two"), equalTo(origEvent.get("two")));
    assertThat(restoredEvent.get("three"), equalTo(origEvent.get("three")));
    assertThat(restoredEvent.get("one"), equalTo("11111"));
    assertThat(restoredEvent.get("two"), equalTo(22222L));
    assertThat(restoredEvent.get("three"), equalTo("33333"));

    // remove attribute 3
    assertThat(restoredEvent.getAttributes().size(), equalTo(3));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value3 = restoredEvent.getAttributes().remove("three");
    assertThat(value3, equalTo("33333"));
    assertThat(restoredEvent.get("three"), equalTo(null));

    // remove non-existing attribute
    assertThat(restoredEvent.getAttributes().size(), equalTo(2));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var noSuchAttribute = restoredEvent.getAttributes().remove("amiss");
    assertThat(noSuchAttribute, equalTo(null));

    // verify iterators
    assertThat(restoredEvent.getAttributes().containsKey("one"), is(true));
    assertThat(restoredEvent.getAttributes().containsKey("three"), is(false));
    assertThat(restoredEvent.getAttributes().containsValue(22222L), is(true));
    assertThat(restoredEvent.getAttributes().containsValue("33333"), is(false));

    assertThat(restoredEvent.getAttributes().keySet(), equalTo(Set.of("one", "two")));
    assertThat(
        restoredEvent.getAttributes().values().stream().collect(Collectors.toSet()),
        equalTo(Set.of("11111", 22222L)));
    assertThat(
        restoredEvent.getAttributes().entrySet(),
        equalTo(Map.of("one", "11111", "two", 22222L).entrySet()));

    // remove attribute 1
    assertThat(restoredEvent.getAttributes().size(), equalTo(2));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value1 = restoredEvent.getAttributes().remove("one");
    assertThat(value1, equalTo("11111"));
    assertThat(restoredEvent.get("one"), equalTo(null));

    // try again
    assertThat(restoredEvent.getAttributes().size(), equalTo(1));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value12 = restoredEvent.getAttributes().remove("one");
    assertThat(value12, equalTo(null));
    assertThat(restoredEvent.get("one"), equalTo(null));

    assertThat(restoredEvent.getAttributes().size(), equalTo(1));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value2 = restoredEvent.getAttributes().remove("two");
    assertThat(value2, equalTo(22222L));
    assertThat(restoredEvent.get("two"), equalTo(null));
    assertThat(restoredEvent.getAttributes().size(), equalTo(0));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(true));

    // verify that the original did not get affected
    assertThat(origEvent.get("one"), equalTo("11111"));
    assertThat(origEvent.get("two"), equalTo(22222L));
    assertThat(origEvent.get("three"), equalTo("33333"));
    final var restoredEvent2 = values.toEvent(nameToIndex);
    assertThat(restoredEvent2.get("one"), equalTo("11111"));
    assertThat(restoredEvent2.get("two"), equalTo(22222L));
    assertThat(restoredEvent2.get("three"), equalTo("33333"));
  }

  @Test
  public void resetAttributes() {
    final var origEvent = new EventJson(UUID.randomUUID(), new Date(System.currentTimeMillis()));
    origEvent.set("one", "11111");
    origEvent.set("two", 22222L);
    origEvent.set("three", "33333");
    final var values = new ObjectListEventValue(origEvent, attributeNames);
    final var restoredEvent = values.toEvent(nameToIndex);

    // remove attribute 3
    assertThat(restoredEvent.getAttributes().size(), equalTo(3));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value3 = restoredEvent.getAttributes().remove("three");
    assertThat(value3, equalTo("33333"));
    assertThat(restoredEvent.get("three"), equalTo(null));

    // remove attribute 1
    assertThat(restoredEvent.getAttributes().size(), equalTo(2));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    final var value1 = restoredEvent.getAttributes().remove("one");
    assertThat(value1, equalTo("11111"));
    assertThat(restoredEvent.get("one"), equalTo(null));

    // reset attribute1
    assertThat(restoredEvent.getAttributes().size(), equalTo(1));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));
    restoredEvent.set("one", "ichi");
    assertThat(restoredEvent.get("one"), equalTo("ichi"));
    assertThat(restoredEvent.getAttributes().size(), equalTo(2));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));

    // overwrite
    restoredEvent.set("one", "1010101");
    assertThat(restoredEvent.get("one"), equalTo("1010101"));
    assertThat(restoredEvent.getAttributes().size(), equalTo(2));
    assertThat(restoredEvent.getAttributes().isEmpty(), is(false));

    // remove attribute 1 and overwrite attribute 3
    restoredEvent.getAttributes().remove("one");
    assertThat(restoredEvent.getAttributes().containsValue("1010101"), is(false));
    restoredEvent.set("three", "1010101");
    assertThat(restoredEvent.getAttributes().containsValue("1010101"), is(true));
  }
}
