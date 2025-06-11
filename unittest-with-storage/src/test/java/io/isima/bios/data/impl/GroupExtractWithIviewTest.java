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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Count;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO(TFOS-1187): The test cases must be enhanced when group extraction using index tables is
// supported.
public class GroupExtractWithIviewTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataServiceHandler handler;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static StreamConfig signal;
  private static String allTypesName;
  private static StreamConfig allTypes;

  private static long startTime = 0;

  // Async process executor
  private static ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Throwable {
    Bios2TestModules.startModules(
        true,
        GroupExtractWithIviewTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "3000",
            "prop.rollupDebugSignal",
            "testStreamAAA"));
    admin = BiosModules.getAdminInternal();
    handler = BiosModules.getDataServiceHandler();

    tenantName = "extractWithGroupWithIndexTest";

    timestamp = System.currentTimeMillis();

    signalName = "Signal";
    signal = new StreamConfig(signalName).setVersion(timestamp);
    signal.addAttribute(new AttributeDesc("region", InternalAttributeType.STRING));
    signal.addAttribute(
        new AttributeDesc("category", InternalAttributeType.ENUM)
            .setEnum(Arrays.asList("Transistors", "Diodes", "Opamps")));
    signal.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc("inet", InternalAttributeType.STRING));

    final ViewDesc viewDesc = new ViewDesc();
    viewDesc.setName("view_by_region_category");
    viewDesc.setGroupBy(Arrays.asList("region", "category"));
    viewDesc.setAttributes(Arrays.asList("value"));
    signal.addView(viewDesc);

    tenantConfig = new TenantConfig(tenantName);
    tenantConfig.addStream(signal);

    allTypesName = "AllTypes";
    allTypes = new StreamConfig(allTypesName).setVersion(timestamp);
    allTypes.addAttribute(new AttributeDesc("string", InternalAttributeType.STRING));
    allTypes.addAttribute(new AttributeDesc("long", InternalAttributeType.LONG));
    allTypes.addAttribute(new AttributeDesc("boolean", InternalAttributeType.BOOLEAN));
    allTypes.addAttribute(new AttributeDesc("blob", InternalAttributeType.BLOB));
    allTypes.addAttribute(new AttributeDesc("longValue", InternalAttributeType.LONG));
    allTypes.addAttribute(new AttributeDesc("doubleValue", InternalAttributeType.DOUBLE));

    final ViewDesc allTypesView = new ViewDesc();
    allTypesView.setName("view_all_types");
    allTypesView.setGroupBy(Arrays.asList("string", "long", "boolean", "blob"));
    allTypesView.setAttributes(Arrays.asList("longValue", "doubleValue"));
    allTypes.addView(allTypesView);
    tenantConfig.addStream(allTypes);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    startTime =
        TestUtils.insert(handler, tenantName, signalName, "EMEA,Transistors,10,123.45.67.89")
            .getTimeStamp();
    TestUtils.insert(handler, tenantName, signalName, "EMEA,Diodes,10,123.45.67.34");
    TestUtils.insert(handler, tenantName, signalName, "EMEA,Transistors,7,123.45.67.56");
    TestUtils.insert(handler, tenantName, signalName, "EMEA,Opamps,13,123.45.67.78");
    TestUtils.insert(handler, tenantName, signalName, "APAC,Transistors,21,223.45.67.78");
    TestUtils.insert(handler, tenantName, signalName, "APAC,Diodes,31,223.45.67.89");

    // Thread.sleep(7000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    timestamp = System.currentTimeMillis();
    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Throwable {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGroupCountryStateCity() throws Throwable {
    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(startTime);
    request.setEndTime(startTime + 10 * 1000);
    final Aggregate sum = new Sum("value").as("total");
    final Aggregate count = new Count().as("count");
    request.setAggregates(Arrays.asList(sum, count));
    final var group = new Group("region", "category");
    request.setView(group);
    request.setAttributes(new ArrayList<>());

    final var out = TestUtils.extract(handler, tenantName, signalName, request);

    assertEquals(5, out.size());

    int i = 0;
    assertEquals("APAC", out.get(i).getAttributes().get("region"));
    assertEquals("Transistors", out.get(i).getAttributes().get("category"));
    assertEquals(21L, out.get(i).getAttributes().get("total"));
    assertEquals(1L, out.get(i).getAttributes().get("count"));
    ++i;
    assertEquals("APAC", out.get(i).getAttributes().get("region"));
    assertEquals("Diodes", out.get(i).getAttributes().get("category"));
    assertEquals(31L, out.get(i).getAttributes().get("total"));
    assertEquals(1L, out.get(i).getAttributes().get("count"));
    ++i;
    assertEquals("EMEA", out.get(i).getAttributes().get("region"));
    assertEquals("Transistors", out.get(i).getAttributes().get("category"));
    assertEquals(17L, out.get(i).getAttributes().get("total"));
    assertEquals(2L, out.get(i).getAttributes().get("count"));
    ++i;
    assertEquals("EMEA", out.get(i).getAttributes().get("region"));
    assertEquals("Diodes", out.get(i).getAttributes().get("category"));
    assertEquals(10L, out.get(i).getAttributes().get("total"));
    assertEquals(1L, out.get(i).getAttributes().get("count"));
    ++i;
    assertEquals("EMEA", out.get(i).getAttributes().get("region"));
    assertEquals("Opamps", out.get(i).getAttributes().get("category"));
    assertEquals(13L, out.get(i).getAttributes().get("total"));
    assertEquals(1L, out.get(i).getAttributes().get("count"));
  }

  @Test
  public void testGroupBlob() throws Throwable {
    final var resp =
        TestUtils.insert(
            handler, tenantName, allTypesName, "orange,222,true,aGVsbG8gd29ybGQK,8,9.1");
    TestUtils.insert(handler, tenantName, allTypesName, "orange,222,true,aGVsbG8gd29ybGQK,20,10.2");
    TestUtils.insert(
        handler, tenantName, allTypesName, "persimmon,222,true,aGVsbG8gd29ybGQK,20,10.4");
    TestUtils.insert(
        handler, tenantName, allTypesName, "orange,222,true,Z29vZGJ5IHdvcmxkCg==,200,100.0");

    long ts = resp.getTimeStamp();

    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(ts);
      request.setEndTime(ts + 10 * 1000);
      final Aggregate sumLong = new Sum("longValue").as("totalLong");
      final Aggregate sumDouble = new Sum("doubleValue").as("totalDouble");
      final Aggregate count = new Count().as("count");
      request.setAggregates(Arrays.asList(sumLong, sumDouble, count));

      final View view = new Group("string", "long", "boolean", "blob");
      request.setView(view);
      request.setAttributes(new ArrayList<>());

      final var out = TestUtils.extract(handler, tenantName, allTypesName, request);

      assertEquals(3, out.size());
      int i = 0;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertArrayEquals(
          "goodby world\n".getBytes(),
          ((ByteBuffer) out.get(i).getAttributes().get("blob")).array());
      assertEquals(1L, out.get(i).getAttributes().get("count"));
      assertEquals(200L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(100.0, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);

      ++i;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertArrayEquals(
          "hello world\n".getBytes(),
          ((ByteBuffer) out.get(i).getAttributes().get("blob")).array());
      assertEquals(2L, out.get(i).getAttributes().get("count"));
      assertEquals(28L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(19.3, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);

      ++i;
      assertEquals("persimmon", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertArrayEquals(
          "hello world\n".getBytes(),
          ((ByteBuffer) out.get(i).getAttributes().get("blob")).array());
      assertEquals(1L, out.get(i).getAttributes().get("count"));
      assertEquals(20L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(10.4, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);
    }
    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(ts);
      request.setEndTime(ts + 10 * 1000);
      final Aggregate sumLong = new Sum("longValue").as("totalLong");
      final Aggregate sumDouble = new Sum("doubleValue").as("totalDouble");
      final Aggregate count = new Count().as("count");
      request.setAggregates(Arrays.asList(sumLong, sumDouble, count));

      final View view = new Group("string", "long", "boolean");
      request.setView(view);
      request.setAttributes(new ArrayList<>());

      final var out = TestUtils.extract(handler, tenantName, allTypesName, request);

      assertEquals(2, out.size());
      int i = 0;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertEquals(3L, out.get(i).getAttributes().get("count"));
      assertEquals(228L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(119.3, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);

      ++i;
      assertEquals("persimmon", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertEquals(1L, out.get(i).getAttributes().get("count"));
      assertEquals(20L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(10.4, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);
    }
  }

  @Test
  public void testGroupBoolean() throws Throwable {
    final var resp =
        TestUtils.insert(
            handler, tenantName, allTypesName, "orange,222,true,aGVsbG8gd29ybGQK,20,1.6");
    TestUtils.insert(handler, tenantName, allTypesName, "orange,222,false,aGVsbG8gd29ybGQK,40,3.2");
    TestUtils.insert(handler, tenantName, allTypesName, "orange,222,true,aGVsbG8gd29ybGQK,80,6.4");
    TestUtils.insert(
        handler, tenantName, allTypesName, "orange,222,false,Z29vZGJ5IHdvcmxkCg==,160,12.8");
    TestUtils.insert(
        handler, tenantName, allTypesName, "orange,222,true,Z29vZGJ5IHdvcmxkCg==,320,25.6");

    long ts = resp.getTimeStamp();

    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(ts);
      request.setEndTime(ts + 10 * 1000);
      final Aggregate sumLong = new Sum("longValue").as("totalLong");
      final Aggregate sumDouble = new Sum("doubleValue").as("totalDouble");
      final Aggregate count = new Count().as("count");
      request.setAggregates(Arrays.asList(sumLong, sumDouble, count));

      final View view = new Group("string", "long", "boolean");
      request.setView(view);
      request.setAttributes(new ArrayList<>());

      final var out = TestUtils.extract(handler, tenantName, allTypesName, request);

      assertEquals(2, out.size());
      int i = 0;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.FALSE, out.get(i).getAttributes().get("boolean"));
      assertEquals(2L, out.get(i).getAttributes().get("count"));
      assertEquals(200L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(16.0, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);

      ++i;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(Boolean.TRUE, out.get(i).getAttributes().get("boolean"));
      assertEquals(3L, out.get(i).getAttributes().get("count"));
      assertEquals(420L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(33.6, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);
    }
    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(ts);
      request.setEndTime(ts + 10 * 1000);
      final Aggregate sumLong = new Sum("longValue").as("totalLong");
      final Aggregate sumDouble = new Sum("doubleValue").as("totalDouble");
      final Aggregate count = new Count().as("count");
      request.setAggregates(Arrays.asList(sumLong, sumDouble, count));

      final View view = new Group("string", "long");
      request.setView(view);
      request.setAttributes(new ArrayList<>());

      final var out = TestUtils.extract(handler, tenantName, allTypesName, request);

      assertEquals(1, out.size());
      int i = 0;
      assertEquals("orange", out.get(i).getAttributes().get("string"));
      assertEquals(222L, out.get(i).getAttributes().get("long"));
      assertEquals(5L, out.get(i).getAttributes().get("count"));
      assertEquals(620L, out.get(i).getAttributes().get("totalLong"));
      assertEquals(49.6, (Double) out.get(i).getAttributes().get("totalDouble"), 1.e-5);
    }
  }
}
