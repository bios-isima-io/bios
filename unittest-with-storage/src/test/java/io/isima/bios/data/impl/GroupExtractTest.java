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
package io.isima.bios.data.impl;

import static org.junit.Assert.assertEquals;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Count;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.Last;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupExtractTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataServiceHandler dataServiceHandler;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static StreamConfig signal;

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  long startTime = 0;
  long endTime = 0;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(GroupExtractTest.class);
    admin = BiosModules.getAdminInternal();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    tenantName = "extractWithGroupTest";
    signalName = "Signal";

    timestamp = System.currentTimeMillis();

    signal = new StreamConfig(signalName).setVersion(timestamp);
    signal.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc("inet", InternalAttributeType.STRING));
    tenantConfig = new TenantConfig(tenantName);
    tenantConfig.addStream(signal);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    timestamp = System.currentTimeMillis();
    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Throwable {
    startTime = ingest("United States,California,Richmond,10,123.45.67.89").getTimeStamp();
    ingest("United States,California,Palo Alto,10,123.45.67.34");
    ingest("United States,California,Richmond,7,123.45.67.56");
    ingest("United States,Virginia,Richmond,13,123.45.67.78");

    UUID uuidA = UUIDs.timeBased();
    UUID uuidB = UUIDs.timeBased();
    ingest("Japan,Kanagawa,Yokohama,21,223.45.67.78", uuidA);
    endTime = ingest("Japan,Aomori,Yokohama,31,223.45.67.89", uuidB).getTimeStamp() + 10;
  }

  private InsertResponseRecord ingest(String data) throws Throwable {
    return ingest(data, UUIDs.timeBased());
  }

  private InsertResponseRecord ingest(String data, UUID eventId) throws Throwable {
    return TestUtils.insert(dataServiceHandler, tenantName, signalName, eventId, data);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGroupCountryStateCity() throws Throwable {
    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    final Aggregate sum = new Sum("value").as("total");
    final Aggregate count = new Count().as("count");
    request.setAggregates(Arrays.asList(sum, count));
    final View view = new Group("country", "state", "city");
    request.setView(view);
    request.setAttributes(new ArrayList<>());

    final var out = TestUtils.extract(dataServiceHandler, tenantName, signalName, request);

    assertEquals(5, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals("Aomori", out.get(0).getAttributes().get("state"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("city"));
    assertEquals(31L, out.get(0).getAttributes().get("total"));
    assertEquals(1L, out.get(0).getAttributes().get("count"));

    assertEquals("Japan", out.get(1).getAttributes().get("country"));
    assertEquals("Kanagawa", out.get(1).getAttributes().get("state"));
    assertEquals("Yokohama", out.get(1).getAttributes().get("city"));
    assertEquals(21L, out.get(1).getAttributes().get("total"));
    assertEquals(1L, out.get(1).getAttributes().get("count"));

    assertEquals("United States", out.get(2).getAttributes().get("country"));
    assertEquals("California", out.get(2).getAttributes().get("state"));
    assertEquals("Palo Alto", out.get(2).getAttributes().get("city"));
    assertEquals(10L, out.get(2).getAttributes().get("total"));
    assertEquals(1L, out.get(2).getAttributes().get("count"));

    assertEquals("United States", out.get(3).getAttributes().get("country"));
    assertEquals("California", out.get(3).getAttributes().get("state"));
    assertEquals("Richmond", out.get(3).getAttributes().get("city"));
    assertEquals(17L, out.get(3).getAttributes().get("total"));
    assertEquals(2L, out.get(3).getAttributes().get("count"));

    assertEquals("United States", out.get(4).getAttributes().get("country"));
    assertEquals("Virginia", out.get(4).getAttributes().get("state"));
    assertEquals("Richmond", out.get(4).getAttributes().get("city"));
    assertEquals(13L, out.get(4).getAttributes().get("total"));
    assertEquals(1L, out.get(4).getAttributes().get("count"));
  }

  @Test
  public void testGroupCountryCity() throws Throwable {
    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    final Aggregate sum = new Sum("value").as("total");
    final Aggregate count = new Count().as("count");
    request.setAggregates(Arrays.asList(sum, count));
    final View view = new Group("country", "city");
    request.setView(view);
    request.setAttributes(new ArrayList<>());

    final var out = TestUtils.extract(dataServiceHandler, tenantName, signalName, request);

    assertEquals(3, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("city"));
    assertEquals(52L, out.get(0).getAttributes().get("total"));
    assertEquals(2L, out.get(0).getAttributes().get("count"));

    assertEquals("United States", out.get(1).getAttributes().get("country"));
    assertEquals("Palo Alto", out.get(1).getAttributes().get("city"));
    assertEquals(10L, out.get(1).getAttributes().get("total"));
    assertEquals(1L, out.get(1).getAttributes().get("count"));

    assertEquals("United States", out.get(2).getAttributes().get("country"));
    assertEquals("Richmond", out.get(2).getAttributes().get("city"));
    assertEquals(30L, out.get(2).getAttributes().get("total"));
    assertEquals(3L, out.get(2).getAttributes().get("count"));
  }

  @Test
  public void testGroupCountry() throws Throwable {
    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    final Aggregate sum = new Sum("value");
    final Aggregate count = new Count();
    final Aggregate lastCity = new Last("city");
    final Aggregate lastValue = new Last("value");
    final Aggregate lastInet = new Last("inet");
    request.setAggregates(Arrays.asList(sum, count, lastCity, lastValue, lastInet));
    final View view = new Group("country");
    request.setView(view);
    request.setAttributes(Collections.emptyList());

    final var out = TestUtils.extract(dataServiceHandler, tenantName, signalName, request);

    assertEquals(2, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals(52L, out.get(0).getAttributes().get("sum(value)"));
    assertEquals(2L, out.get(0).getAttributes().get("count()"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("last(city)"));
    assertEquals(31L, out.get(0).getAttributes().get("last(value)"));
    assertEquals("223.45.67.89", out.get(0).getAttributes().get("last(inet)"));

    assertEquals("United States", out.get(1).getAttributes().get("country"));
    assertEquals(40L, out.get(1).getAttributes().get("sum(value)"));
    assertEquals(4L, out.get(1).getAttributes().get("count()"));
    assertEquals("Richmond", out.get(1).getAttributes().get("last(city)"));
    assertEquals(13L, out.get(1).getAttributes().get("last(value)"));
    assertEquals("123.45.67.78", out.get(1).getAttributes().get("last(inet)"));
  }

  @Test
  public void testGroupZeroDimensions() throws Throwable {
    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    final Aggregate sum = new Sum("value").as("total");
    final Aggregate count = new Count().as("count");
    final Aggregate lastCity = new Last("city").as("last_city");
    final Aggregate lastValue = new Last("value").as("last_value");
    final Aggregate lastInet = new Last("inet").as("last_inet");
    request.setAggregates(Arrays.asList(sum, count, lastCity, lastValue, lastInet));
    final View view = new Group();
    request.setView(view);
    request.setAttributes(new ArrayList<>());

    final var out = TestUtils.extract(dataServiceHandler, tenantName, signalName, request);

    assertEquals(1, out.size());

    assertEquals(92L, out.get(0).getAttributes().get("total"));
    assertEquals(6L, out.get(0).getAttributes().get("count"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("last_city"));
    assertEquals(31L, out.get(0).getAttributes().get("last_value"));
    assertEquals("223.45.67.89", out.get(0).getAttributes().get("last_inet"));
  }
}
