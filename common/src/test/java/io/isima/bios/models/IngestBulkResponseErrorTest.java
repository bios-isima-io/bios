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
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.dto.IngestBulkErrorResponse;
import io.isima.bios.dto.IngestResultJson;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.junit.BeforeClass;
import org.junit.Test;

public class IngestBulkResponseErrorTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testWithoutPartialResultOrError() throws IOException {
    // decode
    String src =
        "{\"status\":\"BAD_REQUEST\","
            + "\"message\":\"invalid request: UUID version of eventId must be 1\"}";
    IngestBulkErrorResponse payload = mapper.readValue(src, IngestBulkErrorResponse.class);

    assertEquals(Response.Status.BAD_REQUEST, payload.getStatus());
    assertEquals("invalid request: UUID version of eventId must be 1", payload.getMessage());

    // encode
    String out = mapper.writeValueAsString(payload);
    assertEquals(src, out);
  }

  @Test
  public void testWithPartialResultOrError() throws IOException {
    List<IngestResultJson> results = new ArrayList<>();
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    results.add(createDummyResult(true, uuid1));
    results.add(createDummyResult(false, uuid2));

    IngestBulkErrorResponse errorResponse =
        new IngestBulkErrorResponse(Response.Status.BAD_REQUEST, "Ingest bulk failure", results);

    String out = mapper.writeValueAsString(errorResponse);
    assertTrue(out.contains(uuid1.toString()));
    assertTrue(out.contains(uuid2.toString()));
  }

  private IngestResultJson createDummyResult(boolean success, UUID eventId) {
    IngestResultJson result = new IngestResultJson();
    result.setEventId(eventId);
    if (success) {
      result.setTimestamp(System.currentTimeMillis());
    } else {
      result.setStatusCode(10101);
      result.setErrorMessage("Test Error");
    }
    return result;
  }
}
