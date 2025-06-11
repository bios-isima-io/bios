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
package io.isima.bios.apps;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.ResourceAllocator;
import io.isima.bios.admin.ResourceType;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.errors.exception.InvalidConfigurationException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.AppsInfo;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.TenantConfig;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AppsManagerTest {

  private static AppsManager appsManager;
  private static Admin admin;
  private static SharedConfig sharedConfig;
  private static ResourceAllocator resourceAllocator;

  private static String defaultTenantName;
  private static List<String> createdTenants;

  /** Start modules, initialize the test tenant management. */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AppsManagerTest.class);
    appsManager = BiosModules.getAppsManager();
    admin = BiosModules.getAdmin();
    sharedConfig = BiosModules.getSharedConfig();
    resourceAllocator = BiosModules.getResourceAllocator();
    createdTenants = new ArrayList<>();
    defaultTenantName = AppsManagerTest.class.getSimpleName();
  }

  /** Clear test specific shared property and shutdown modules. */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  /** Set ports-reservation-after-deregistration time to 0, create the default test tenant. */
  @Before
  public void setUp() throws Exception {
    sharedConfig.setProperty(AppsManager.PROP_APPS_RESOURCE_RESERVATION_SECONDS, "0");

    final var tenant = new TenantConfig(defaultTenantName);
    Long timestamp = System.currentTimeMillis();
    try {
      admin.deleteTenant(defaultTenantName, RequestPhase.INITIAL, timestamp);
      admin.deleteTenant(defaultTenantName, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // it's fine
    }
    timestamp = System.currentTimeMillis();
    admin.createTenant(tenant, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(tenant, RequestPhase.FINAL, timestamp, false);
    createdTenants.clear();
    createdTenants.add(tenant.getName());
  }

  /** Delete all created test tenants, clear test specific shared properties. */
  @After
  public void tearDown() throws Exception {
    final Long timestamp = System.currentTimeMillis();
    for (var tenantName : createdTenants) {
      try {
        admin.deleteTenant(tenantName, RequestPhase.INITIAL, timestamp);
        admin.deleteTenant(tenantName, RequestPhase.FINAL, timestamp);
      } catch (NoSuchTenantException e) {
        // it's fine
      }
    }
    final var sharedProperty = BiosModules.getSharedProperties();
    sharedProperty.deleteProperty(AppsManager.PROP_APPS_RESOURCE_RESERVATION_SECONDS);
    sharedProperty.deleteProperty(AppsManager.PROP_APPS_HOSTS);
  }

  @Test
  public void testFundamentalFlow() throws Exception {
    sharedConfig.setProperty(AppsManager.PROP_APPS_HOSTS, "fundamental-1, fundamental-2");
    final TenantConfig tenantConfig =
        admin.getTenant(defaultTenantName, false, false, false, List.of());
    final var tenantId = new EntityId(tenantConfig);

    // test registration
    final var appsInfo = new AppsInfo(defaultTenantName);
    final var registered = appsManager.register(tenantId, appsInfo);
    assertEquals(defaultTenantName, registered.getTenantName());
    assertEquals(List.of("fundamental-1", "fundamental-2"), registered.getHosts());
    assertEquals(Integer.valueOf(9001), registered.getControlPort());
    assertEquals(Integer.valueOf(8081), registered.getWebhookPort());

    // test apps info retrieval
    final var retrieved = appsManager.getAppsInfo(tenantId);
    assertThat(retrieved, equalTo(registered));

    // test deregistration
    appsManager.deregister(tenantId);

    assertThrows(NoSuchEntityException.class, () -> appsManager.getAppsInfo(tenantId));
  }

  @Test
  public void testMultipleTenants() throws Exception {
    sharedConfig.setProperty(AppsManager.PROP_APPS_RESOURCE_RESERVATION_SECONDS, "10");
    sharedConfig.setProperty(
        AppsManager.PROP_APPS_HOSTS, "multi-tenant-1, multi-tenant-2, multi-tenant-3");

    final TenantConfig tenantConfig1 =
        admin.getTenant(defaultTenantName, false, false, false, List.of());
    final var tenantId1 = new EntityId(tenantConfig1);

    final var tenantName2 = defaultTenantName + "2";
    final var secondTenant = new TenantConfig(tenantName2);
    createdTenants.add(tenantName2);
    Long timestamp = System.currentTimeMillis();
    admin.createTenant(secondTenant, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(secondTenant, RequestPhase.FINAL, timestamp, false);
    final TenantConfig tenantConfig2 = admin.getTenant(tenantName2, false, false, false, List.of());
    final var tenantId2 = new EntityId(tenantConfig2);

    // test registration
    final var appsInfo1 = new AppsInfo(defaultTenantName);
    final var registered1 = appsManager.register(tenantId1, appsInfo1);
    final var hosts = List.of("multi-tenant-1", "multi-tenant-2", "multi-tenant-3");
    assertEquals(defaultTenantName, registered1.getTenantName());
    assertEquals(hosts, registered1.getHosts());
    assertEquals(Integer.valueOf(9001), registered1.getControlPort());
    assertEquals(Integer.valueOf(8081), registered1.getWebhookPort());

    assertThrows(
        AlreadyExistsException.class,
        () -> appsManager.register(tenantId1, new AppsInfo(defaultTenantName, hosts, null, null)));

    final var hosts2 = List.of("hello", "world");
    final var appsInfo2 = new AppsInfo(tenantName2, hosts2, null, null);
    final var registered2 = appsManager.register(tenantId2, appsInfo2);
    assertEquals(tenantName2, registered2.getTenantName());
    assertEquals(hosts2, registered2.getHosts());
    assertEquals(Integer.valueOf(9002), registered2.getControlPort());
    assertEquals(Integer.valueOf(8082), registered2.getWebhookPort());

    // test apps info retrieval
    final var retrieved1 = appsManager.getAppsInfo(tenantId1);
    assertThat(retrieved1, equalTo(registered1));
    final var retrieved2 = appsManager.getAppsInfo(tenantId2);
    assertThat(retrieved2, equalTo(registered2));

    // test deregistration
    appsManager.deregister(tenantId1);

    // Try registering again with the previous port numbers.
    // This should fail because the port numbers are still reserved.
    assertThrows(
        AlreadyExistsException.class,
        () -> appsManager.register(tenantId1, new AppsInfo(defaultTenantName, hosts, 9001, 8081)));

    // Try again with port number unspecified
    final var registered1Again = appsManager.register(tenantId1, appsInfo1);
    assertThat(registered1Again.getTenantName(), is(defaultTenantName));
    assertThat(registered1Again.getHosts(), equalTo(hosts));
    assertThat(registered1Again.getControlPort(), is(9003));
    assertThat(registered1Again.getWebhookPort(), is(8083));

    // Delete a tenant and see the ports are also released after reservation seconds are over
    timestamp = System.currentTimeMillis();
    admin.deleteTenant(defaultTenantName, RequestPhase.INITIAL, timestamp);
    admin.deleteTenant(defaultTenantName, RequestPhase.FINAL, timestamp);

    assertNotNull(resourceAllocator.checkResource(ResourceType.APPS_CONTROL_PORT, "9003"));
    assertNotNull(resourceAllocator.checkResource(ResourceType.APPS_WEBHOOK_PORT, "8083"));

    Thread.sleep(15000);

    assertNull(resourceAllocator.checkResource(ResourceType.APPS_CONTROL_PORT, "9003"));
    assertNull(resourceAllocator.checkResource(ResourceType.APPS_WEBHOOK_PORT, "8083"));

    // To release resources quickly after the test
    sharedConfig.setProperty(AppsManager.PROP_APPS_RESOURCE_RESERVATION_SECONDS, "0");
  }

  @Test
  public void testMissingDefaultHosts() throws Exception {
    sharedConfig.setProperty(AppsManager.PROP_APPS_RESOURCE_RESERVATION_SECONDS, "0");
    final TenantConfig tenantConfig =
        admin.getTenant(defaultTenantName, false, false, false, List.of());
    final var tenantId = new EntityId(tenantConfig);

    // registration should fail if the default hosts are not set in the shared properties
    final var appsInfo = new AppsInfo(defaultTenantName);
    assertThrows(
        InvalidConfigurationException.class, () -> appsManager.register(tenantId, appsInfo));

    // but it's ok if the hosts are specified explicitly
    final var appsInfo2 = new AppsInfo(defaultTenantName, List.of("abc", "def"), 9090, 8080);
    final var registered = appsManager.register(tenantId, appsInfo2);
    assertEquals(defaultTenantName, registered.getTenantName());
    assertEquals(List.of("abc", "def"), registered.getHosts());
    assertEquals(Integer.valueOf(9090), registered.getControlPort());
    assertEquals(Integer.valueOf(8080), registered.getWebhookPort());

    appsManager.deregister(tenantId);
  }
}
