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
package io.isima.bios.admin.v1.store.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import io.isima.bios.admin.v1.impl.Tenants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminStoreTest {

  private static AdminStore adminStore;
  private static long timestamp;

  // tenants used for tests
  private List<String> testTenants = new ArrayList<>();

  /** Class setup: Load server modules. */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AdminStoreTest.class);
    adminStore = BiosModules.getAdminStore();
    timestamp = System.currentTimeMillis();
  }

  /** Class teardown: Shutdown server modules. */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  /** test setup: clear tenant names used by tenant. */
  @Before
  public void setUp() throws Exception {
    testTenants.clear();
  }

  /** test teardown: delete tenants used by the test. */
  @After
  public void tearDown() throws Exception {
    for (String tenant : testTenants) {
      adminStore.deleteTenant(tenant);
    }
  }

  @Test
  public void testStoreTenant() throws ApplicationException {
    // add one tenant
    final String firstTenant = "first_tenant";
    final String stream1 = "one";
    final String stream2 = "2two";
    final String stream3 = "THREE";
    final String stream4 = "4four";
    final String stream5 = "five";
    testTenants.add(firstTenant);
    TenantStoreDesc tenantStoreDesc = new TenantStoreDesc(firstTenant);
    tenantStoreDesc.addStream(new StreamStoreDesc(stream1));
    tenantStoreDesc
        .getStreams()
        .get(0)
        .addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    tenantStoreDesc.addStream(new StreamStoreDesc(stream2));
    Long firstTimestamp = ++timestamp;
    tenantStoreDesc.setVersion(firstTimestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(firstTenant));
    TenantStoreDesc retrieved = storeDescMap.get(firstTenant).peek();
    assertNotNull(retrieved);
    assertNotSame(tenantStoreDesc, retrieved);
    StreamStoreDesc found1 = Tenants.findStreamStoreDesc(retrieved, stream1, false);
    assertNotNull(found1);
    assertEquals(stream1, found1.getName());
    assertEquals("hello", found1.getAttributes().get(0).getName());
    StreamConfig found2 = Tenants.findStreamStoreDesc(retrieved, stream2, false);
    assertNotNull(found2);
    assertEquals(stream2, found2.getName());
    assertEquals((Long) timestamp, retrieved.getVersion());
    assertEquals((Long) timestamp, found1.getVersion());
    assertEquals((Long) timestamp, found2.getVersion());

    // add another tenant
    String secondTenant = "second_tenant";
    testTenants.add(secondTenant);
    tenantStoreDesc = new TenantStoreDesc(secondTenant);
    tenantStoreDesc.addStream(new StreamStoreDesc("streamA"));
    tenantStoreDesc.addStream(new StreamStoreDesc("streamB"));
    tenantStoreDesc.setVersion(++timestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    // verify
    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(firstTenant));
    assertTrue(storeDescMap.containsKey(secondTenant));
    retrieved = storeDescMap.get(firstTenant).peek();
    found1 = Tenants.findStreamStoreDesc(retrieved, stream1, false);
    assertNotNull(found1);
    assertEquals(stream1, found1.getName());
    retrieved = storeDescMap.get(secondTenant).peek();
    assertEquals(2, retrieved.getStreams().size());

    // fetch the second tenant
    final TenantConfig config =
        adminStore.getTenant(tenantStoreDesc.getName(), tenantStoreDesc.getVersion());
    assertNotNull(config);
    assertEquals(tenantStoreDesc.getName(), config.getName());
    assertEquals(tenantStoreDesc.getVersion(), config.getVersion());

    // fetch the second stream of the first tenant
    found1 = adminStore.getStream("first_tenant", stream2, firstTimestamp);
    assertNotNull(found1);
    assertEquals(stream2, found1.getName());

    // overwrite
    tenantStoreDesc = new TenantStoreDesc(firstTenant);
    tenantStoreDesc.addStream(new StreamStoreDesc(stream3));
    tenantStoreDesc.addStream(new StreamStoreDesc(stream4));
    tenantStoreDesc.setVersion(++timestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(firstTenant));
    retrieved = storeDescMap.get(firstTenant).peek();
    assertNotSame(tenantStoreDesc, retrieved);
    found1 = Tenants.findStreamStoreDesc(retrieved, stream1, false);
    assertNull(found1);
    found2 = Tenants.findStreamStoreDesc(retrieved, stream2, false);
    assertNull(found1);
    StreamStoreDesc found3 = Tenants.findStreamStoreDesc(retrieved, stream3.toLowerCase(), false);
    assertNotNull(found3);
    assertEquals(stream3, found3.getName());
    StreamStoreDesc found4 = Tenants.findStreamStoreDesc(retrieved, stream4.toUpperCase(), false);
    assertNotNull(found4);
    assertEquals(stream4, found4.getName());
    assertEquals((Long) timestamp, retrieved.getVersion());

    // delete
    tenantStoreDesc = new TenantStoreDesc(firstTenant);
    tenantStoreDesc.setDeleted(true);
    tenantStoreDesc.setVersion(++timestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(firstTenant));
    assertTrue(storeDescMap.get(firstTenant).peek().isDeleted());
    assertTrue(storeDescMap.containsKey(secondTenant));
    assertFalse(storeDescMap.get(secondTenant).peek().isDeleted());

    // create again
    tenantStoreDesc = new TenantStoreDesc(firstTenant);
    tenantStoreDesc.addStream(new StreamStoreDesc(stream5));
    tenantStoreDesc.setVersion(++timestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(firstTenant));
    assertFalse(storeDescMap.get(firstTenant).peek().isDeleted());
    assertTrue(storeDescMap.containsKey(secondTenant));
    assertTrue(storeDescMap.containsKey(secondTenant));
    retrieved = storeDescMap.get(firstTenant).peek();
    assertEquals(1, retrieved.getStreams().size());
    assertEquals(stream5, retrieved.getStreams().get(0).getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddTenantSanityCheck() throws ApplicationException {
    adminStore.storeTenant(null);
  }

  @Test
  public void testStoreStream() throws ApplicationException {
    // add base tenant
    String tenantName = "stream_store_test";
    testTenants.add(tenantName);
    TenantStoreDesc tenantStoreDesc = new TenantStoreDesc(tenantName);
    tenantStoreDesc.addStream(new StreamStoreDesc("one"));
    tenantStoreDesc
        .getStreams()
        .get(0)
        .addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    tenantStoreDesc.setVersion(++timestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    adminStore.storeTenant(tenantStoreDesc);

    // 1. Add a stream
    StreamStoreDesc streamStoreDesc = new StreamStoreDesc("two", ++timestamp, false);
    adminStore.storeStream(tenantName, streamStoreDesc);

    // Verify the added stream
    Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(tenantName));
    TenantStoreDesc retrieved = storeDescMap.get(tenantName).peek();
    assertNotNull(retrieved);
    assertNotSame(tenantStoreDesc, retrieved);
    StreamStoreDesc streamOne = Tenants.findStreamStoreDesc(retrieved, "one", false);
    assertNotNull(streamOne);
    assertEquals("hello", streamOne.getAttributes().get(0).getName());
    StreamStoreDesc streamTwo = Tenants.findStreamStoreDesc(retrieved, "two", false);
    assertNotNull(streamTwo);

    // 2. Delete a stream
    StreamStoreDesc toDelete = new StreamStoreDesc("one", ++timestamp, true);
    adminStore.storeStream(tenantName, toDelete);

    // verify deletion
    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(tenantName));
    retrieved = storeDescMap.get(tenantName).peek();
    assertNotNull(retrieved);
    assertNotSame(tenantStoreDesc, retrieved);
    streamOne = Tenants.findStreamStoreDesc(retrieved, "one", false);
    assertNull(streamOne);
    streamTwo = Tenants.findStreamStoreDesc(retrieved, "two", false);
    assertNotNull(streamTwo);

    // 3. Add a stream with the same name as deleted
    StreamStoreDesc recreated = new StreamStoreDesc("one", ++timestamp, false);
    recreated.addAttribute(new AttributeDesc("new_attr", InternalAttributeType.INT));
    adminStore.storeStream(tenantName, recreated);

    storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(tenantName));
    retrieved = storeDescMap.get(tenantName).peek();
    assertNotNull(retrieved);
    assertNotSame(tenantStoreDesc, retrieved);
    streamOne = Tenants.findStreamStoreDesc(retrieved, "one", false);
    assertNotNull(streamOne);
    assertEquals("new_attr", streamOne.getAttributes().get(0).getName());
    streamTwo = Tenants.findStreamStoreDesc(retrieved, "two", false);
    assertNotNull(streamTwo);
  }

  /**
   * We have changed stream name for tenant in the admin table from "_root_" to ".root" to correct
   * order of the rows (TFOS-1952). This test verifies whether the tenants with old stream name are
   * loaded properly.
   */
  @Test
  public void testBackwardCompatibility() throws ApplicationException {
    // add one tenant
    final String tenant = "tenant_reloading_test";
    final String stream1 = "one";
    final String stream2 = "2two";
    testTenants.add(tenant);
    final TenantStoreDesc tenantStoreDesc = new TenantStoreDesc(tenant);
    tenantStoreDesc.addStream(new StreamStoreDesc(stream1));
    tenantStoreDesc
        .getStreams()
        .get(0)
        .addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    tenantStoreDesc.addStream(new StreamStoreDesc(stream2));
    Long firstTimestamp = ++timestamp;
    tenantStoreDesc.setVersion(firstTimestamp);
    tenantStoreDesc.getStreams().forEach(streamStoreDesc -> streamStoreDesc.setVersion(timestamp));
    ((AdminStoreImpl) adminStore).storeTenantCore(tenantStoreDesc, AdminStoreImpl.ROOT_ENTRY_OLD);

    final Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap.containsKey(tenant));
    final TenantStoreDesc retrieved = storeDescMap.get(tenant).peek();
    assertNotNull(retrieved);
    assertNotSame(tenantStoreDesc, retrieved);
    StreamStoreDesc found1 = Tenants.findStreamStoreDesc(retrieved, stream1, false);
    assertNotNull(found1);
    assertEquals(stream1, found1.getName());
    assertEquals("hello", found1.getAttributes().get(0).getName());
    StreamConfig found2 = Tenants.findStreamStoreDesc(retrieved, stream2, false);
    assertNotNull(found2);
    assertEquals(stream2, found2.getName());
    assertEquals((Long) timestamp, retrieved.getVersion());
    assertEquals((Long) timestamp, found1.getVersion());
    assertEquals((Long) timestamp, found2.getVersion());

    final TenantStoreDesc deletion =
        new TenantStoreDesc(tenant).setVersion(++timestamp).setDeleted(true);
    adminStore.storeTenant(deletion);

    final Map<String, Deque<TenantStoreDesc>> storeDescMap2 = adminStore.getTenantStoreDescMap();
    assertTrue(storeDescMap2.containsKey(tenant));
    final TenantStoreDesc retrieved2 = storeDescMap2.get(tenant).peek();
    assertNotNull(retrieved2);
    assertTrue(retrieved2.isDeleted());
  }
}
