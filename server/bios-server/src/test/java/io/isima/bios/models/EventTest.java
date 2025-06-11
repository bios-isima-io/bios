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

import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testBasic() throws JsonProcessingException {
    final UUID eventId = UUID.randomUUID();
    final long timestamp = System.currentTimeMillis();
    final Event event = new EventJson(eventId, new Date(timestamp));
    event.set("stringAttr", "hello");

    assertEquals("hello", event.get("stringAttr"));
    assertNull(event.get("noSuch"));

    final String out = mapper.writeValueAsString(event);
    System.out.println(out);
    assertThat(out, matchesPattern(".*\"eventId\":\"" + eventId + "\".*"));
    assertThat(out, matchesPattern(".*\"ingestTimestamp\":" + timestamp + ".*"));
    assertThat(out, matchesPattern(".*\"stringAttr\":\"hello\".*"));
  }

  @Test
  public void testNumbers() throws JsonProcessingException {
    final UUID eventId = UUID.randomUUID();
    final long timestamp = System.currentTimeMillis();
    final Event event = new EventJson(eventId, new Date(timestamp));
    event.set("intAttr", Integer.valueOf(1234567));
    event.set("longAttr", Long.valueOf(6507226550L));
    event.set("numberAttr", new BigInteger("3141592653189793238462643383279"));
    event.set("doubleAttr", 3.45);
    event.set("timestampAttr", new Date(timestamp));

    assertEquals(1234567, event.get("intAttr"));
    assertEquals(6507226550L, event.get("longAttr"));
    assertEquals(new BigInteger("3141592653189793238462643383279"), event.get("numberAttr"));
    assertEquals(3.45, event.get("doubleAttr"));

    final String out = mapper.writeValueAsString(event);
    System.out.println(out);
    assertThat(out, matchesPattern(".*\"eventId\":\"" + eventId + "\".*"));
    assertThat(out, matchesPattern(".*\"ingestTimestamp\":" + timestamp + ".*"));
    assertThat(out, matchesPattern(".*\"intAttr\":1234567.*"));
    assertThat(out, matchesPattern(".*\"longAttr\":6507226550.*"));
    assertThat(out, matchesPattern(".*\"numberAttr\":3141592653189793238462643383279.*"));
    assertThat(out, matchesPattern(".*\"doubleAttr\":3.45.*"));
  }

  @Test
  public void testDates() throws JsonProcessingException {
    final UUID eventId = UUID.randomUUID();
    final long timestamp = System.currentTimeMillis();
    final Event event = new EventJson(eventId, new Date(timestamp));
    event.set("timestampAttr", new Date(timestamp));
    event.set("dateAttr", LocalDate.of(2019, 1, 16));

    assertEquals(timestamp, ((Date) event.get("timestampAttr")).getTime());
    assertEquals(LocalDate.of(2019, 1, 16), event.get("dateAttr"));

    final String out = mapper.writeValueAsString(event);
    System.out.println(out);
    assertThat(out, matchesPattern(".*\"timestampAttr\":" + timestamp + ".*"));
    assertThat(out, matchesPattern(".*\"dateAttr\":\"2019-01-16\".*"));
  }

  @Test
  public void testInet() throws JsonProcessingException, UnknownHostException {
    final UUID eventId = UUID.randomUUID();
    final long timestamp = System.currentTimeMillis();
    final Event event = new EventJson(eventId, new Date(timestamp));
    event.set("inetAttr", InetAddress.getByName("12.226.94.6"));

    assertEquals(InetAddress.getByName("12.226.94.6"), event.get("inetAttr"));

    final String out = mapper.writeValueAsString(event);
    System.out.println(out);
    assertThat(out, matchesPattern(".*\"inetAttr\":\"12.226.94.6\".*"));
  }

  @Test
  public void testUuid() throws JsonProcessingException, UnknownHostException {
    final UUID eventId = UUID.randomUUID();
    final long timestamp = System.currentTimeMillis();
    final Event event = new EventJson(eventId, new Date(timestamp));
    final UUID uuid = UUID.randomUUID();
    event.set("uuidAttr", uuid);

    assertEquals(uuid, event.get("uuidAttr"));

    final String out = mapper.writeValueAsString(event);
    System.out.println(out);
    assertThat(out, matchesPattern(".*\"uuidAttr\":\"" + uuid + "\".*"));
  }
}
