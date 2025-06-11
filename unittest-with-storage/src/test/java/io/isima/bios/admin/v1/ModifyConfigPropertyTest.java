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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The objective of this class is to test that changes in properties exposed through class
 * TfosConfig are reflected properly in further calls to server
 */
public class ModifyConfigPropertyTest {
  private static ObjectMapper mapper;
  private static long timestamp;
  private AdminInternal admin;

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantConfig> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() {
    mapper = TfosObjectMapperProvider.get();
    Bios2TestModules.startModules(false, ModifyConfigPropertyTest.class, Map.of());
    timestamp = System.currentTimeMillis();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() {
    testTenants.clear();
  }

  @After
  public void tearDown() {
    for (TenantConfig tenantConfig : testTenants) {
      final AdminInternal admin = BiosModules.getAdminInternal();
      try {
        admin.removeTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
      try {
        admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
    }
  }

  /**
   * Register a stream Validate that default property is chosen Stop the server Change the property
   * Restart the server Validate that property change is reflected
   */
  @Test
  public void testSignalTimeIndexPropertyChangeGetsReflectedOnSignalCreation() throws Exception {
    final String tenantSourceJson = "{'name': 'signalTimeIndexPropertyChangeTest'}";
    TenantConfig tenantConfig =
        mapper.readValue(tenantSourceJson.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String firstStreamSource =
        "{"
            + "  'name': 'mysignalfirst',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig firstStream =
        mapper.readValue(firstStreamSource.replaceAll("'", "\""), StreamConfig.class);
    firstStream.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), firstStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), firstStream, RequestPhase.FINAL);

    final StreamDesc firstStreamDesc =
        admin.getStream(tenantConfig.getName(), firstStream.getName());
    Assert.assertEquals(300000L, firstStreamDesc.getIndexWindowLength().longValue());

    Bios2TestModules.shutdown();
    Bios2TestModules.setProperty(TfosConfig.TIME_INDEX_WIDTH, "100000");
    Bios2TestModules.startModules(false, ModifyConfigPropertyTest.class + " 2", Map.of());
    admin = BiosModules.getAdminInternal();

    final String secondStreamSrc =
        "{"
            + "  'name': 'mysignalSecond',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        mapper.readValue(secondStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    secondStream.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), secondStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), secondStream, RequestPhase.FINAL);

    final StreamDesc secondStreamDesc =
        admin.getStream(tenantConfig.getName(), secondStream.getName());
    Assert.assertEquals(100000L, secondStreamDesc.getIndexWindowLength().longValue());
  }
}
