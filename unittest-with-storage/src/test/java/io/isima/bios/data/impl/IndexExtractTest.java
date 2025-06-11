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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexExtractTest {

  private static AdminImpl admin;
  private static DataServiceHandler dataServiceHandler;
  private static DataEngine dataEngine;
  private static long timestamp;

  private String testTenant;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(
        true,
        IndexExtractTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "5000",
            "prop.rollupDebugSignal",
            "testStreamAAA"));
    admin = (AdminImpl) BiosModules.getAdminInternal();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    dataEngine = BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws Exception {
    System.out.println("TEAR DOWN");
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
  }

  /**
   * Simple index extraction test.
   *
   * <p>This case tests the Extract operation with a request that should trigger an index-and-view
   * extraction. Verifies the extraction output and execution path (by checking the execution path).
   */
  @Test
  public void testSimple() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_testSimple";
    TenantDesc temp = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    admin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());

    final String signalSrc =
        "{"
            + "  'name': 'testSimple',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'state', 'type': 'string'},"
            + "      {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "      'name': 'by_country_state',"
            + "      'groupBy': ['country', 'state'],"
            + "      'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "      'view': 'by_country_state',"
            + "      'rollups': [{"
            + "          'name': 'rollup_by_country_state',"
            + "          'interval': {'value': 1, 'timeunit': 'minute'},"
            + "          'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "      }]"
            + "  }]"
            + "}";
    AdminTestUtils.populateStream(admin, testTenant, signalSrc, ++timestamp);
    final var signalName = "testSimple";
    StreamDesc streamDesc = admin.getStream(testTenant, signalName);

    final var resp = TestUtils.insert(dataServiceHandler, testTenant, signalName, "USA,CA,10");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "USA,MA,100");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "USA,CA,20");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "Japan,Tokyo,1000");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "USA,MA,800");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "Japan,Nagoya,7000");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "USA,CA,30");
    TestUtils.insert(dataServiceHandler, testTenant, signalName, "Japan,Tokyo,1000");

    final long interval = 60000;
    final long start = (resp.getTimeStamp() / interval) * interval;

    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime(start + 400000);

    Thread.sleep(10000);

    final long end = start + interval;
    final String attribute = "value";

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);
    request.setAttributes(Collections.emptyList());
    request.setAggregates(
        Arrays.asList(
            new Sum(attribute).as(attribute + "_sum"),
            new Min(attribute).as(attribute + "_min"),
            new Max(attribute).as(attribute + "_max")));
    final View view = new Group(Arrays.asList("country", "state"));
    request.setView(view);

    final var out = TestUtils.extract(dataServiceHandler, streamDesc, request);

    assertEquals(4, out.size());
    assertEquals(5, out.get(0).getAttributes().size());

    assertEquals("Japan", out.get(0).get("country"));
    assertEquals("Nagoya", out.get(0).get("state"));
    assertEquals(7000L, out.get(0).get("value_min"));
    assertEquals(7000L, out.get(0).get("value_max"));
    assertEquals(7000L, out.get(0).get("value_sum"));

    assertEquals("Japan", out.get(1).get("country"));
    assertEquals("Tokyo", out.get(1).get("state"));
    assertEquals(1000L, out.get(1).get("value_min"));
    assertEquals(1000L, out.get(1).get("value_max"));
    assertEquals(2000L, out.get(1).get("value_sum"));

    assertEquals("USA", out.get(2).get("country"));
    assertEquals("CA", out.get(2).get("state"));
    assertEquals(10L, out.get(2).get("value_min"));
    assertEquals(30L, out.get(2).get("value_max"));
    assertEquals(60L, out.get(2).get("value_sum"));

    assertEquals("USA", out.get(3).get("country"));
    assertEquals("MA", out.get(3).get("state"));
    assertEquals(100L, out.get(3).get("value_min"));
    assertEquals(800L, out.get(3).get("value_max"));
    assertEquals(900L, out.get(3).get("value_sum"));
  }
}
