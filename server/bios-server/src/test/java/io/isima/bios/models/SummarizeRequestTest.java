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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import org.hamcrest.junit.ExpectedException;
import org.hamcrest.text.MatchesPattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class SummarizeRequestTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'interval': 600, 'endTime': 12448, 'horizon': 1500, 'limit': 10,"
            + " 'filter': null, 'aggregates': [{'function': 'Sum', 'by': 'sales', 'as': null}],"
            + " 'timezone': 'UTC+5:30', 'group': ['country', 'state'], 'startTime': 12345,"
            + " 'sort': {'function': 'Sort', 'reverse': false, 'by': 'abc', 'caseSensitive': false}}";

    SummarizeRequest req = mapper.readValue(src.replaceAll("'", "\""), SummarizeRequest.class);
    assertEquals(Long.valueOf(12345L), req.getStartTime());
    assertEquals(Long.valueOf(12448L), req.getEndTime());
    assertEquals(Long.valueOf(600), req.getInterval());
    assertEquals(Long.valueOf(1500), req.getHorizon());
    assertEquals(Integer.valueOf(10), req.getLimit());
    assertEquals(
        req.getTimezone().toString(), (5 * 60 + 30) * 60 * 1000, req.getTimezone().getRawOffset());
  }

  @Test
  public void testNullTimeZone() throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'interval': 600, 'endTime': 12448, 'horizon': 1500, 'limit': 10,"
            + " 'filter': null, 'aggregates': [{'function': 'Sum', 'by': 'sales', 'as': null}],"
            + " 'group': ['country', 'state'], 'startTime': 12345,"
            + " 'sort': {'function': 'Sort', 'reverse': false, 'by': 'abc', 'caseSensitive': false}}";

    SummarizeRequest req = mapper.readValue(src.replaceAll("'", "\""), SummarizeRequest.class);
    assertEquals(Long.valueOf(12345L), req.getStartTime());
    assertEquals(Long.valueOf(12448L), req.getEndTime());
    assertEquals(Long.valueOf(600), req.getInterval());
    assertEquals(Long.valueOf(1500), req.getHorizon());
    assertEquals(Integer.valueOf(10), req.getLimit());
    assertNull(req.getTimezone());
  }

  @Test
  public void testInvalidTimeZone() throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{'interval': 600, 'endTime': 12448, 'horizon': 1500, 'limit': 10,"
            + " 'filter': null, 'aggregates': [{'function': 'Sum', 'by': 'sales', 'as': null}],"
            + " 'timezone': 'RedWoodCity', 'group': ['country', 'state'], 'startTime': 12345,"
            + " 'sort': {'function': 'Sort', 'reverse': false, 'by': 'abc', 'caseSensitive': false}}";

    thrown.expect(InvalidFormatException.class);
    thrown.expectMessage(MatchesPattern.matchesPattern("not a valid TimeZone value.*"));

    mapper.readValue(src.replaceAll("'", "\""), SummarizeRequest.class);
  }
}
