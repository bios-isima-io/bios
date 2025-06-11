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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class InjectPayloadsTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  @Test
  public void defaultRequestConstructor() {
    UUID uuid = UUID.randomUUID();
    String inputText = "hello,world";
    Long timestamp = System.currentTimeMillis();
    IngestRequest request = new IngestRequest(uuid, inputText, timestamp);
    assertSame(uuid, request.getEventId());
    assertSame(inputText, request.getEventText());
    assertSame(timestamp, request.getStreamVersion());
  }

  @Test
  public void makeRequestFromJson() throws JsonParseException, JsonMappingException, IOException {
    String src =
        "{\"eventId\":\"faaae8ea-2978-11e8-b467-0ed5f89f718b\",\"eventText\":\"38.8977,77.0365\","
            + "\"unknown\":\"field\"}";
    IngestRequest request = mapper.readValue(src, IngestRequest.class);
    assertEquals(UUID.fromString("faaae8ea-2978-11e8-b467-0ed5f89f718b"), request.getEventId());
    assertEquals("38.8977,77.0365", request.getEventText());
  }

  // Purpose: incomplete JSON should not cause an exception. Internal service handler would
  // check the object validity except syntax error.
  @Test
  public void incompleteJson() throws JsonParseException, JsonMappingException, IOException {
    String eventIdMissing = "{\"eventText\":\"38.8977,77.0365\"}";
    IngestRequest request = mapper.readValue(eventIdMissing, IngestRequest.class);
    assertEquals("38.8977,77.0365", request.getEventText());
    assertNull(request.getEventId());

    String eventTextMissing = "{\"eventId\":\"faaae8ea-2978-11e8-b467-0ed5f89f718b\"}";
    request = mapper.readValue(eventTextMissing, IngestRequest.class);
    assertEquals(UUID.fromString("faaae8ea-2978-11e8-b467-0ed5f89f718b"), request.getEventId());
    assertNull(request.getEventText());
  }

  @Test
  public void defaultResponseConstructor() {
    IngestResponse resp = new IngestResponse();
    assertNotNull(resp);
    assertNull(resp.getEventId());
    assertNull(resp.getIngestTimestamp());

    UUID uuid = UUID.randomUUID();
    resp.setEventId(uuid);
    assertSame(uuid, resp.getEventId());

    Date date = new Date(System.currentTimeMillis());
    resp.setIngestTimestamp(date);
    assertEquals(date, resp.getIngestTimestamp());
  }

  @Test
  public void constructResponseFromEvent() throws JsonProcessingException {
    Event event =
        new EventJson(
            UUID.fromString("6b95b0fd-b2d8-4ffa-a545-2c55468b116c"), new Date(1521246378457L));
    IngestResponse resp = new IngestResponse(event.getEventId(), event.getIngestTimestamp());
    assertNotNull(resp);
    assertSame(event.getEventId(), resp.getEventId());
    assertEquals(event.getIngestTimestamp(), resp.getIngestTimestamp());

    String out = mapper.writeValueAsString(resp);
    assertEquals(
        "{\"eventId\":\"6b95b0fd-b2d8-4ffa-a545-2c55468b116c\",\"ingestTimestamp\":1521246378457}",
        out);
  }
}
