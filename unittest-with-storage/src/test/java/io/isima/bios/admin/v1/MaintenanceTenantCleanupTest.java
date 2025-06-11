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
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.TaskSlots;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.storage.cassandra.CassandraDataStoreUtils;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MaintenanceTenantCleanupTest {
  private static ObjectMapper mapper;

  private static AdminInternal admin;
  private static AdminStore adminStore;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection conn;
  private static long timestamp;
  private static TaskSlots taskSlots;
  private String testTenant;

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantConfig> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() {
    mapper = TfosObjectMapperProvider.get();
    Bios2TestModules.startModulesWithoutMaintenance(MaintenanceTenantCleanupTest.class);
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    conn = BiosModules.getCassandraConnection();
    timestamp = 1577940100000L;
    admin = BiosModules.getAdminInternal();
    adminStore = BiosModules.getAdminStore();
    taskSlots = new TaskSlots(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    testTenants.clear();
  }

  @After
  public void tearDown() throws Exception {
    for (TenantConfig tenantConfig : testTenants) {
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
      adminStore.deleteTenant(tenantConfig.getName());
    }
  }

  @Test
  public void testKeyspaceRemovalForTenantWithAddedStream() throws Exception {
    final String tenantSrc = "{'name': '_delete_tenant_with_added_stream'}";
    TenantConfig tenantConfig =
        mapper.readValue(tenantSrc.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    final String initialStreamSrc =
        "{"
            + "  'name': 'test_stream',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        mapper.readValue(initialStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    initialStream.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, initialStream.getName(), initialStream.getVersion());
    String tableQualifiedName = keyspaceName + "." + tableName;

    // Verify that the stream table has been created under tenant keyspace
    Assert.assertTrue(conn.verifyQualifiedTable(tableQualifiedName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertFalse(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyQualifiedTable(tableQualifiedName));
  }

  @Test
  public void testKeyspaceRemovalForTenantWithInitialStream() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_delete_tenant_with_initial_stream";
    final String tenantSrc =
        "{"
            + "  'name': 'temp',"
            + "  'streams': [{"
            + "    'name': 'test_stream',"
            + "    'attributes': [{'name': 'test_attribute', 'type': 'string'}]"
            + "  }]"
            + "}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertFalse(conn.verifyKeyspace(keyspaceName));
  }

  @Test
  public void testKeyspaceRemovalForTenantWithNoStream() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_delete_tenant_no_stream";
    final String tenantSrc = "{'name': 'temp'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);
    Assert.assertFalse(conn.verifyKeyspace(keyspaceName));
  }

  @Test
  public void testKeyspaceForTenantAddedWithSameNameMultipleTimes() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_delete_tenant_add_with_same_name";
    final String tenantSrc =
        "{"
            + "  'name': 'temp',"
            + "  'streams': [{"
            + "    'name': 'test_stream',"
            + "    'attributes': [{'name': 'test_attribute', 'type': 'string'}]"
            + "  }]"
            + "}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    tenantConfig.setVersion(++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String secondKeyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(secondKeyspaceName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    tenantConfig.setVersion(++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String thirdKeyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(thirdKeyspaceName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertFalse(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyKeyspace(secondKeyspaceName));
    Assert.assertTrue(conn.verifyKeyspace(thirdKeyspaceName));
  }

  @Test
  public void testKeyspacesForTenantAddedWithSameNameWithAddedStream() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    final String tenantSrc = "{'name': '_delete_tenant_add_with_same_name_with_added_stream'}";
    TenantConfig tenantConfig =
        mapper.readValue(tenantSrc.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    final String initialStreamSrc =
        "{"
            + "  'name': 'test_stream',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig streamConfig =
        mapper.readValue(initialStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    String tableQualifiedName = keyspaceName + "." + tableName;

    // Verify that the stream table has been created under tenant keyspace
    Assert.assertTrue(conn.verifyQualifiedTable(tableQualifiedName));

    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    tenantConfig.setVersion(++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String newKeyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(newKeyspaceName));

    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    tableQualifiedName = newKeyspaceName + "." + tableName;

    // Verify that the stream table has been created under tenant keyspace
    Assert.assertTrue(conn.verifyQualifiedTable(tableQualifiedName));

    dataEngine.maintainInternal(taskSlots);

    // Verify that old keyspace is removed and new keyspace is present
    Assert.assertFalse(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyKeyspace(newKeyspaceName));
  }
}
