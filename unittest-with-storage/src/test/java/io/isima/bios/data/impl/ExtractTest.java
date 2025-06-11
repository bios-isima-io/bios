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
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExtractTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataServiceHandler dataServiceHandler;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static StreamConfig signal;
  private static long startTime;

  private static final String ATTR_NAME = "sequence";

  // Async process executor
  private static ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Throwable {
    Bios2TestModules.startModulesWithoutMaintenance(ExtractTest.class);
    admin = BiosModules.getAdminInternal();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    tenantName = "extractWideRange";
    signalName = "Signal";

    // 2018/8/11 00:00:00.000 UTC
    startTime = 1533945600000L;

    // create the tenant at one hour before the start time
    timestamp = startTime - 3600000;

    signal = new StreamConfig(signalName).setVersion(timestamp);
    signal.addAttribute(new AttributeDesc(ATTR_NAME, InternalAttributeType.LONG));
    tenantConfig = new TenantConfig(tenantName);
    tenantConfig.addStream(signal);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // Ingest events distributed within four time indexes
    long sequence = 0;
    for (long diff = 0; diff < 24 * 3600 * 1000; diff += 60 * 1000) {
      final long ingestTime = startTime + diff;
      TestUtils.insert(
          dataServiceHandler,
          tenantName,
          signalName,
          UUIDs.startOf(ingestTime),
          Long.toString(sequence++));
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  /** Test extracting signals that were ingested with timestamp within four hours. */
  @Test
  public void testExtractFirstHour() throws Throwable {

    // Try extracting events in the first hour
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(startTime + 3600 * 1000);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
    assertEquals(60, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < 60; ++i) {
      assertEquals(Long.valueOf(i), ((Event) result.get(i)).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtractSecondHour() throws Throwable {
    // Try extracting events in the second hour
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime + 3600 * 1000);
    extractRequest.setEndTime(startTime + 7200 * 1000);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(60, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < 60; ++i) {
      assertEquals(Long.valueOf(i) + 60, ((Event) result.get(i)).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtractFourHours() throws Throwable {
    // Try extract events for all 4 hours since the start time
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(startTime + 4 * 3600 * 1000);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(240, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < 240; ++i) {
      assertEquals(Long.valueOf(i), ((Event) result.get(i)).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtractTenHours() throws Throwable {
    // Try extract for 10 hours since the start time
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(startTime + 10 * 3600 * 1000);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(600, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < 600; ++i) {
      assertEquals(Long.valueOf(i), ((Event) result.get(i)).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtract24Hours() throws Throwable {
    // Try extract for 10 hours since the start time
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(startTime + 24 * 3600 * 1000);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(1440, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < 1440; ++i) {
      assertEquals(Long.valueOf(i), ((Event) result.get(i)).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtractZeroLength() throws Throwable {
    // Try extract with zero width time window
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime + 100);
    extractRequest.setEndTime(startTime + 100);

    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(0, result.size());
  }
}
