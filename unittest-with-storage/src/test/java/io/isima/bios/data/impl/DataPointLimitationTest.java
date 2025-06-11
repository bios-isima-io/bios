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
import static org.junit.Assert.assertThrows;

import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
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

public class DataPointLimitationTest {
  private static final int LIMITATION = 10000;
  private static String previousDataPointLimitation;

  private static long timestamp;
  private static AdminInternal admin;
  private static DataServiceHandler dataServiceHandler;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static StreamConfig signal;
  private static long startTime;
  private static long endTimeFirstTier;
  private static long endTimeSecondTier;

  private static final String ATTR_NAME = "sequence";

  // Async process executor
  private static ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Throwable {
    Bios2TestModules.setProperty(
        TfosConfig.SELECT_MAX_NUM_DATA_POINTS, Integer.toString(LIMITATION));
    Bios2TestModules.startModulesWithoutMaintenance(DataPointLimitationTest.class);
    admin = BiosModules.getAdminInternal();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    tenantName = "dataPointLimitationTest";
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

    // Ingest 10,000 events
    final long ingestDelta = 1000;
    long ingestTime = startTime;
    long sequence = 0;
    for (int i = 0; i < LIMITATION; ++i) {
      TestUtils.insert(
          dataServiceHandler,
          tenantName,
          signalName,
          UUIDs.startOf(ingestTime),
          Long.toString(sequence++));
      endTimeFirstTier = ingestTime;
      ingestTime += ingestDelta;
    }

    for (int i = 0; i < 2000; ++i) {
      TestUtils.insert(
          dataServiceHandler,
          tenantName,
          signalName,
          UUIDs.startOf(ingestTime),
          Long.toString(sequence++));
      endTimeSecondTier = ingestTime;
      ingestTime += ingestDelta;
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

  @Test
  public void testExtractWithinLimitation() throws Throwable {

    // Try extracting events in the first tier
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(endTimeFirstTier + 1);
    final var result =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);

    assertEquals(LIMITATION, result.size());
    // extracted value must be sorted by timestamp
    for (int i = 0; i < LIMITATION; ++i) {
      assertEquals(Long.valueOf(i), result.get(i).getAttributes().get(ATTR_NAME));
    }
  }

  @Test
  public void testExtractBeyondLimitation() throws Throwable {
    // Try extracting events in the first tier
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(startTime);
    extractRequest.setEndTime(endTimeSecondTier + 1);

    assertThrows(
        TfosException.class,
        () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
  }
}
