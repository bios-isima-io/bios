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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.dto.IngestBulkRequestJson;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IngestBulkRequestTest {

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
  public void testSerialization() throws Exception {
    final List<String> uuidsSrc =
        Arrays.asList(
            "0d3a492a-921d-11e9-b683-526af7764f64",
            "0d3a4b96-921d-11e9-b683-526af7764f64",
            "0d3a4cea-921d-11e9-b683-526af7764f64",
            "3a48f04c-921d-11e9-bc42-526af7764f64");
    final List<UUID> uuids = uuidsSrc.stream().map(UUID::fromString).collect(Collectors.toList());
    final List<String> events =
        Arrays.asList("thomas,edison", "nikola,tesla", "marie,curie", "albert,einstein");
    final Long streamVersion = 12345L;

    final IngestBulkRequestJson request = new IngestBulkRequestJson(uuids, events, streamVersion);
    final String encoded = mapper.writeValueAsString(request);

    final StringBuilder expected =
        new StringBuilder("{'eventIds':")
            .append(buildStringArray(uuidsSrc))
            .append(",'events':")
            .append(buildStringArray(events))
            .append(",'streamVersion':")
            .append(streamVersion)
            .append("}");
    assertEquals(expected.toString().replaceAll("'", "\""), encoded);

    final IngestBulkRequestJson restored = mapper.readValue(encoded, IngestBulkRequestJson.class);
    assertEquals(uuids, restored.getEventIds());
    assertEquals(events, restored.getEvents());
    assertEquals(streamVersion, restored.getStreamVersion());
  }

  private String buildStringArray(List<String> src) {
    final StringJoiner joiner = new StringJoiner("','");
    src.forEach(item -> joiner.add(item));
    return "['" + joiner.toString() + "']";
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullEventIds() {
    new IngestBulkRequestJson(null, Collections.emptyList(), 12345L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullEvents() {
    new IngestBulkRequestJson(Collections.emptyList(), null, 12345L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorNullStreamVersion() {
    new IngestBulkRequestJson(Collections.emptyList(), Collections.emptyList(), null);
  }

  @Test
  public void testConstructorEmptyInput() {
    final IngestBulkRequestJson request =
        new IngestBulkRequestJson(Collections.emptyList(), Collections.emptyList(), 33452L);
    assertEquals(0, request.getEventIds().size());
    assertEquals(0, request.getEvents().size());
    assertEquals(Long.valueOf(33452), request.getStreamVersion());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorUnbalancedInput() {
    new IngestBulkRequestJson(
        Arrays.asList(UUID.randomUUID(), UUID.randomUUID()),
        Arrays.asList("abc", "def", "ghi"),
        51359L);
  }
}
