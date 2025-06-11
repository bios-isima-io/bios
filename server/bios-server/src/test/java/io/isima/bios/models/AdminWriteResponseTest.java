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
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminWriteResponseTest {

  static ObjectMapper mapper;

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
  public void testConstructors() {
    final AdminWriteResponse resp1 = new AdminWriteResponse();
    assertNull(resp1.getTimestamp());
    assertNull(resp1.getEndpoints());
    final AdminWriteResponse resp2 = new AdminWriteResponse(12345L, Arrays.asList("abc"));
    assertEquals(Long.valueOf(12345L), resp2.getTimestamp());
    assertEquals(Arrays.asList("abc"), resp2.getEndpoints());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullTimestamp() {
    new AdminWriteResponse(null, Arrays.asList("abc"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullEndpoints() {
    new AdminWriteResponse(12345L, null);
  }

  @Test
  public void testSerialization() throws Exception {
    final AdminWriteResponse adminWriteResponse =
        new AdminWriteResponse(
            1572469144950L, Arrays.asList("https://10.20.30.40:443", "https://10.20.30.31:443"));
    final String encoded = mapper.writeValueAsString(adminWriteResponse);

    String expected =
        "{'timestamp':1572469144950,'endpoints':['https://10.20.30.40:443','https://10.20.30.31:443']}";
    assertEquals(expected.replaceAll("'", "\""), encoded);

    final AdminWriteResponse restored = mapper.readValue(encoded, AdminWriteResponse.class);
    assertEquals(Long.valueOf(1572469144950L), restored.getTimestamp());
    assertEquals(
        Arrays.asList("https://10.20.30.40:443", "https://10.20.30.31:443"),
        restored.getEndpoints());
  }
}
