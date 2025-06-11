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

import static java.lang.Thread.sleep;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.data.impl.maintenance.PostProcessScheduler;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.DataSketchDurationHelper;
import io.isima.bios.server.handlers.DataServiceHandler;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SketchesTest {
  private static AdminInternal admin;
  private static io.isima.bios.admin.Admin biosAdmin;
  private static DataServiceHandler dataServiceHandler;
  private static final String tenantName = "sketchesTest";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(
        true,
        SketchesTest.class,
        Map.of(
            DataSketchDurationHelper.MIN_FEATURE_INTERVAL_MILLIS_KEY, "100",
            PostProcessScheduler.FEATURE_DELAY_MARGIN_KEY, "100",
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY, "100"));

    admin = BiosModules.getAdminInternal();
    biosAdmin = BiosModules.getAdmin();
    dataServiceHandler = BiosModules.getDataServiceHandler();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    AdminTestUtils.addTenant(admin, tenantName);
  }

  @After
  public void tearDown() throws Exception {
    AdminTestUtils.deleteTenantIgnoreError(admin, tenantName);
  }

  @Test
  public void testSimpleDigest() throws Throwable {
    final String signalName =
        AdminTestUtils.populateSignal(
            biosAdmin, tenantName, "../tests/resources/data-sketches-signal-digest-bios.json");
    StreamDesc signal = admin.getStream(tenantName, signalName);
    for (int i = 0; i < 1000; i++) {
      final String input = String.format("%d,%f,%d,zip%d,Male", i, i * 1.0001, i * 10000, i);
      TestUtils.insert(dataServiceHandler, tenantName, signal.getName(), input);
      // sleep(1);
    }
    sleep(300);
    for (int i = 0; i < 1000; i++) {
      final String input = String.format("%d,%f,%d,zip%d,Female", i, i * 1.0001, i * 10000, i);
      TestUtils.insert(dataServiceHandler, tenantName, signal.getName(), input);
      // sleep(1);
    }
    sleep(5000);
  }
}
