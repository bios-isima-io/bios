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
package io.isima.bios.dto;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailureReportTest {
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

  /**
   * Test deserializing a FailureReport object.
   *
   * @throws Exception for any errors
   */
  @Test
  public void testDeserialization() throws Exception {
    final String src =
        "{"
            + "  'timestamp': 1554379852381,"
            + "  'operation': 'POST /tenants',"
            + "  'payload': '{`name`: `new_tenant`}',"
            + "  'endpoints': ['https://node1:443', 'https://node2:443'],"
            + "  'reasons': ['Timeout', 'Internal Server Error'],"
            + "  'reporter': 'Python SDK from 10.20.30.40'"
            + "}";
    final String converted = src.replaceAll("'", "\"").replaceAll("`", "\\\\\"");
    FailureReport report = mapper.readValue(converted, FailureReport.class);
    assertEquals(Long.valueOf(1554379852381L), report.getTimestamp());
    assertEquals("POST /tenants", report.getOperation());
    assertEquals("{\"name\": \"new_tenant\"}", report.getPayload());
    assertEquals(Arrays.asList("https://node1:443", "https://node2:443"), report.getEndpoints());
    assertEquals(Arrays.asList("Timeout", "Internal Server Error"), report.getReasons());
    assertEquals("Python SDK from 10.20.30.40", report.getReporter());
  }

  /**
   * Using a string encoded by Python SDK.
   *
   * @throws Exception for any errors
   */
  @Test
  public void testDeserialization2() throws Exception {
    final String src =
        "{"
            + "'reasons': ["
            + "'error_code=TIMEOUT, message=TimedOut', "
            + "'error_code=SERVER_CHANNEL_ERROR, message=channel failure'],"
            + " 'payload': '{`name`: `test_tenant`}',"
            + " 'reporter': 'Python SDK v1.0.0 from 10.20.30.40',"
            + " 'endpoints': ['https://signal_1', 'https://signal_3'],"
            + " 'operation': 'POST /tenants',"
            + " 'timestamp': 1554379852381"
            + "}";
    final String converted = src.replaceAll("'", "\"").replaceAll("`", "\\\\\"");
    FailureReport report = mapper.readValue(converted, FailureReport.class);
    assertEquals(Long.valueOf(1554379852381L), report.getTimestamp());
    assertEquals("POST /tenants", report.getOperation());
    assertEquals("{\"name\": \"test_tenant\"}", report.getPayload());
    assertEquals(Arrays.asList("https://signal_1", "https://signal_3"), report.getEndpoints());
    assertEquals(
        Arrays.asList(
            "error_code=TIMEOUT, message=TimedOut",
            "error_code=SERVER_CHANNEL_ERROR, message=channel failure"),
        report.getReasons());
    assertEquals("Python SDK v1.0.0 from 10.20.30.40", report.getReporter());
  }

  /**
   * Tests serializing a FailureReport object, decodes it and validates the object reproduction.
   *
   * @throws Exception for any errors.
   */
  @Test
  public void testSerialization() throws Exception {
    final Long timestamp = 1555379852381L;
    final String operation = "POST /tenants/new_tenant";
    final String payload = "{\"name\": \"modified\"}";
    final List<String> endpoints =
        Arrays.asList("https://node_a.example.com:8443", "https://node_b.example.com:8443");
    final List<String> reasons = Arrays.asList("Timeout", "Too Busy");
    final String reporter = "Java SDK from 123.45.67.89";

    FailureReport report =
        new FailureReport(timestamp, operation, payload, endpoints, reasons, reporter);
    final String serialized = mapper.writeValueAsString(report);

    final FailureReport decoded = mapper.readValue(serialized, FailureReport.class);

    assertEquals(timestamp, decoded.getTimestamp());
    assertEquals(operation, decoded.getOperation());
    assertEquals(payload, decoded.getPayload());
    assertEquals(endpoints, decoded.getEndpoints());
    assertEquals(reasons, decoded.getReasons());
    assertEquals(reporter, decoded.getReporter());
  }
}
