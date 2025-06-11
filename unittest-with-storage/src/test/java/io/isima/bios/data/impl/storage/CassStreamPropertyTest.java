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
package io.isima.bios.data.impl.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassStreamPropertyTest {

  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static long timestamp;
  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(CassStreamPropertyTest.class);
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis() - 50;

    tenantName = CassStreamPropertyTest.class.getSimpleName();
    final TenantDesc tenantDesc = new TenantDesc(tenantName, timestamp, false);

    tenantConfig = tenantDesc.toTenantConfig();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());
    } catch (TfosException e) {
      // ignore
    }
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());
    } catch (TfosException e) {
      // ignore
    }
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testTableProperty() throws Exception {
    // preparation
    final String contextName = "contextForPropertyTest";
    {
      final String keyName = "key";
      final String valueName = "value";
      final StreamDesc inputContext = new StreamDesc(contextName, ++timestamp);
      inputContext.setType(StreamType.CONTEXT);
      inputContext.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
      inputContext.addAttribute(new AttributeDesc(valueName, InternalAttributeType.LONG));
      admin.addStream(tenantName, inputContext, RequestPhase.INITIAL);
      admin.addStream(tenantName, inputContext, RequestPhase.FINAL);
    }
    final CassStream cassContext = getCassStream(contextName);

    final String signalName = "signalForPropertyTest";
    {
      final String firstAttribute = "first";
      final String secondAttribute = "second";
      final StreamDesc inputContext = new StreamDesc(signalName, ++timestamp);
      inputContext.setType(StreamType.SIGNAL);
      inputContext.addAttribute(new AttributeDesc(firstAttribute, InternalAttributeType.STRING));
      inputContext.addAttribute(new AttributeDesc(secondAttribute, InternalAttributeType.LONG));
      admin.addStream(tenantName, inputContext, RequestPhase.INITIAL);
      admin.addStream(tenantName, inputContext, RequestPhase.FINAL);
    }
    final CassStream cassSignal = getCassStream(signalName);

    // test getting and setting properties
    assertNull(cassContext.getProperty("hello"));
    cassContext.setProperty("hello", "world");
    cassContext.setProperty("foo", "bar");
    assertEquals("world", cassContext.getProperty("hello"));
    assertEquals("bar", cassContext.getProperty("foo"));

    // test long property value type
    assertEquals(-1L, cassContext.getPropertyAsLong("normalLongValue", -1L));
    cassContext.setProperty("normalLongValue", "6507226550");
    assertEquals(6507226550L, cassContext.getPropertyAsLong("normalLongValue", -1L));
    cassContext.setProperty("invalidLongValue", "this is not a number");
    assertEquals(100L, cassContext.getPropertyAsLong("invalidLongValue", 100L));

    // test adding properties
    final Properties properties = new Properties();
    properties.setProperty("string", "aabbcc");
    properties.setProperty("int", "112233");
    cassContext.addProperties(properties);

    // reload and verify set properties again
    reload();
    final CassStream reloadedCassContext = getCassStream(contextName);
    assertEquals("world", reloadedCassContext.getProperty("hello"));
    assertEquals("bar", reloadedCassContext.getProperty("foo"));
    assertEquals(6507226550L, reloadedCassContext.getPropertyAsLong("normalLongValue", -1L));
    assertEquals(-1L, reloadedCassContext.getPropertyAsLong("invalidLongValue", -1L));
    assertEquals("aabbcc", reloadedCassContext.getProperty("string"));
    assertEquals(112233L, reloadedCassContext.getPropertyAsLong("int", -1));
  }

  // utilities
  private void reload() throws Exception {
    Bios2TestModules.shutdown();
    Bios2TestModules.startModulesWithoutMaintenance(CassStreamPropertyTest.class.toString() + " 2");
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis() - 50;
  }

  private CassStream getCassStream(String name) throws Exception {
    final StreamDesc streamDesc = admin.getStream(tenantName, name);
    return dataEngine.getCassStream(streamDesc);
  }
}
