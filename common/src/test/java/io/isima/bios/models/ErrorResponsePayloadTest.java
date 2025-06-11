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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ErrorResponsePayloadTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = BiosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() throws JsonParseException, JsonMappingException, IOException {
    // decode
    String src =
        "{\"status\":\"BAD_REQUEST\","
            + "\"message\":\"invalid request: UUID version of eventId must be 1\"}";
    ErrorResponsePayload payload = mapper.readValue(src, ErrorResponsePayload.class);

    assertEquals(Response.Status.BAD_REQUEST, payload.getStatus());
    assertEquals("invalid request: UUID version of eventId must be 1", payload.getMessage());

    // encode
    String out = mapper.writeValueAsString(payload);
    assertEquals(src, out);
  }
}
