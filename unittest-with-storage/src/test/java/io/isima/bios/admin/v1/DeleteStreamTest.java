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
package io.isima.bios.admin.v1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.AdminConfigCreator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DeleteStreamTest {

  private static AdminImpl admin;
  private static AdminStore adminStore;
  private static long timestamp;

  private String testTenant;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(DeleteStreamTest.class);
    admin = (AdminImpl) BiosModules.getAdminInternal();
    adminStore = BiosModules.getAdminStore();
    timestamp = System.currentTimeMillis();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
  }

  @Test
  public void testContextLookupSignalDelete() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_signalDelete";
    final String tenantSrc =
        "{"
            + "  'name': 'temp',"
            + "  'streams': [{"
            + "    'name': 'dependant',"
            + "    'attributes': [{'name': 'lookupkey', 'type': 'string'}],"
            + "    'preprocesses': [{"
            + "      'name': 'join',"
            + "      'condition': 'lookupkey',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {'actionType': 'merge', 'context': 'extension', 'attribute': 'additional'}"
            + "      ]"
            + "    }]"
            + "  }, {"
            + "    'name': 'extension',"
            + "    'type': 'context',"
            + "    'attributes': ["
            + "      {'name': 'key', 'type': 'string'},"
            + "      {'name': 'additional', 'type': 'string'}"
            + "    ]"
            + "  }]"
            + "}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final var dependant = admin.getStream(testTenant, "dependant");

    try {
      admin.removeStream(testTenant, "extension", RequestPhase.INITIAL, ++timestamp);
      fail("exception must occur");
    } catch (ConstraintViolationException e) {
      assertEquals(
          String.format(
              "Constraint violation: Context cannot be deleted because other streams depend on it;"
                  + " tenant=DeleteStreamTest_signalDelete, context=extension,"
                  + " referredBy=[dependant (%d)]",
              dependant.getVersion()),
          e.getMessage());
    }

    admin.removeStream(testTenant, "dependant", RequestPhase.INITIAL, ++timestamp);
    admin.removeStream(testTenant, "dependant", RequestPhase.FINAL, timestamp);

    try {
      admin.getStream(testTenant, "dependant");
      fail("exception must happen");
    } catch (NoSuchStreamException e) {
      assertEquals(
          "Stream not found: DeleteStreamTest_signalDelete.dependant", e.getErrorMessage());
    }

    admin.removeStream(testTenant, "extension", RequestPhase.INITIAL, ++timestamp);
    admin.removeStream(testTenant, "extension", RequestPhase.FINAL, timestamp);

    final AdminImpl reloaded = new AdminImpl(adminStore, new MetricsStreamProvider());

    try {
      reloaded.getStream(testTenant, "dependant");
      fail("exception must happen");
    } catch (NoSuchStreamException e) {
      assertEquals(
          "Stream not found: DeleteStreamTest_signalDelete.dependant", e.getErrorMessage());
    }
  }
}
