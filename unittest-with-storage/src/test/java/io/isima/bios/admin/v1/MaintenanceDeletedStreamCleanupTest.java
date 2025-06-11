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
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MaintenanceDeletedStreamCleanupTest {
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
    Bios2TestModules.startModules(true, MaintenanceDeletedStreamCleanupTest.class, Map.of());
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
  public void testTableRemovalForOnlyDeletedStream() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_stream";
    String streamToBeRemoved = this.getClass().getSimpleName() + "_test_stream_to_be_deleted";
    String streamToBeRetained = this.getClass().getSimpleName() + "_test_stream_to_be_retained";
    final String tenantSrc =
        "{"
            + "  'name': 'temp',"
            + "  'streams': [{"
            + "    'name': '"
            + streamToBeRemoved
            + "',"
            + "    'attributes': [{'name': 'test_attribute', 'type': 'string'}]"
            + "  },{"
            + "    'name': '"
            + streamToBeRetained
            + "',"
            + "    'attributes': [{'name': 'test_attribute', 'type': 'string'}]"
            + "}]"
            + "}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    StreamStoreDesc firstStreamDesc = admin.getStream(testTenant, streamToBeRemoved);
    String firstTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, firstStreamDesc.getName(), firstStreamDesc.getVersion());

    StreamStoreDesc secondStreamDesc = admin.getStream(testTenant, streamToBeRetained);
    String secondTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, secondStreamDesc.getName(), secondStreamDesc.getVersion());

    Assert.assertTrue(conn.verifyTable(keyspaceName, firstTableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, secondTableName));

    admin.removeStream(
        tenantConfig.getName(),
        firstStreamDesc.getName(),
        RequestPhase.INITIAL,
        firstStreamDesc.getVersion());
    admin.removeStream(
        tenantConfig.getName(),
        firstStreamDesc.getName(),
        RequestPhase.FINAL,
        firstStreamDesc.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, firstTableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, secondTableName));
  }

  @Test
  public void testTableRemovalForOnlyDeletedStreamWithPreprocess() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_stream_with_preprocess";
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    StreamConfig contextConfig =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);
    admin.addStream(testTenant, contextConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, contextConfig, RequestPhase.FINAL);

    final String signalSource =
        "{"
            + "  'name': 'stream_with_pre_process',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'inet'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'merge_geo_location',"
            + "    'condition': 'ip',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'merge',"
            + "      'context': 'geo_location',"
            + "      'attribute': 'country',"
            + "      'defaultValue': 'n/a'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'context': 'geo_location',"
            + "      'attribute': 'state',"
            + "      'defaultValue': 'n/a'"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig signalConfig =
        AdminConfigCreator.makeStreamConfig(signalSource, ++timestamp);
    admin.addStream(testTenant, signalConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, signalConfig, RequestPhase.FINAL);

    String contextTableName =
        CassStream.generateTableName(
            StreamType.CONTEXT, contextConfig.getName(), contextConfig.getVersion());
    String signalTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, signalConfig.getName(), signalConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, contextTableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, signalTableName));

    admin.removeStream(
        testTenant, signalConfig.getName(), RequestPhase.INITIAL, signalConfig.getVersion());
    admin.removeStream(
        testTenant, signalConfig.getName(), RequestPhase.FINAL, signalConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, contextTableName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, signalTableName));
  }

  @Test
  public void testTableRemovalForDeletedContext() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_context";
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(testTenant, tenantConfig.getVersion());
    // Verify that the keyspace for the tenant has been created
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    StreamConfig contextConfig =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);
    admin.addStream(testTenant, contextConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, contextConfig, RequestPhase.FINAL);

    String contextTableName =
        CassStream.generateTableName(
            StreamType.CONTEXT, contextConfig.getName(), contextConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, contextTableName));

    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.INITIAL, contextConfig.getVersion());
    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.FINAL, contextConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, contextTableName));
  }

  @Test
  public void testTableRemovalForStreamWithPostProcess() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_stream_with_postprocess";
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    final String streamSrc =
        " {"
            + "  'name': 'stream_with_post_process',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'int'},"
            + "    {'name': 'two', 'type': 'int'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'test_view',"
            + "    'groupBy': [],"
            + "    'attributes': ['one']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'test_view',"
            + "    'rollups': [{"
            + "      'name': 'rollup_test_view',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig streamConfig =
        mapper.readValue(streamSrc.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());

    Assert.assertTrue(conn.verifyQualifiedTable(keyspaceName + "." + tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);

    admin.removeStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        RequestPhase.INITIAL,
        streamConfig.getVersion());
    admin.removeStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        RequestPhase.FINAL,
        streamConfig.getVersion());
    dataEngine.maintainInternal(taskSlots);

    Assert.assertFalse(conn.verifyQualifiedTable(keyspaceName + "." + tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, false, false);
  }

  @Test
  public void testOldTableRemovalWhenSameStreamIsRemovedAndAddedAgain() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_with_stream_added_again_after_removal";
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    final String streamSrc =
        " {"
            + "  'name': 'stream_with_attributes',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'int'},"
            + "    {'name': 'two', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig streamConfig =
        mapper.readValue(streamSrc.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());

    Assert.assertTrue(conn.verifyQualifiedTable(keyspaceName + "." + tableName));

    admin.removeStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        RequestPhase.INITIAL,
        streamConfig.getVersion());
    admin.removeStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        RequestPhase.FINAL,
        streamConfig.getVersion());

    Thread.sleep(1000);

    streamConfig.setVersion(++timestamp);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String newTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertFalse(conn.verifyTable(keyspaceName, tableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, newTableName));
  }

  @Test
  public void testOldTableRemovalWhenSameContextIsRemovedAndAddedAgain() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_with_context_added_again_after_removal";
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    StreamConfig contextConfig =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);
    admin.addStream(testTenant, contextConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, contextConfig, RequestPhase.FINAL);

    String contextTableName =
        CassStream.generateTableName(
            StreamType.CONTEXT, contextConfig.getName(), contextConfig.getVersion());
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, contextTableName));

    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.INITIAL, contextConfig.getVersion());
    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.FINAL, contextConfig.getVersion());

    Thread.sleep(1000);

    contextConfig.setVersion(++timestamp);
    admin.addStream(testTenant, contextConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, contextConfig, RequestPhase.FINAL);

    String newContextTableName =
        CassStream.generateTableName(
            StreamType.CONTEXT, contextConfig.getName(), contextConfig.getVersion());

    Assert.assertTrue(conn.verifyTable(keyspaceName, newContextTableName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, contextTableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, newContextTableName));
  }

  @Test
  public void testOldTableRemovalWhenContextRemovedAndSignalAddedWithSameName() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant =
        this.getClass().getSimpleName() + "_tenant_with_signal_added_after_context_removal";
    final String commonName = "someName_" + System.currentTimeMillis();
    final String tenantSrc = "{'name': '" + testTenant + "'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(tenantSrc, ++timestamp);
    tenantConfig.setName(testTenant);
    testTenants.add(tenantConfig);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    String keyspaceName =
        CassandraDataStoreUtils.generateKeyspaceName(
            tenantConfig.getName(), tenantConfig.getVersion());

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));

    StreamConfig contextConfig =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);
    contextConfig.setName(commonName);
    admin.addStream(testTenant, contextConfig, RequestPhase.INITIAL);
    admin.addStream(testTenant, contextConfig, RequestPhase.FINAL);

    String contextTableName =
        CassStream.generateTableName(
            StreamType.CONTEXT, contextConfig.getName(), contextConfig.getVersion());
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, contextTableName));

    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.INITIAL, contextConfig.getVersion());
    admin.removeStream(
        testTenant, contextConfig.getName(), RequestPhase.FINAL, contextConfig.getVersion());

    Thread.sleep(1000);

    final String streamSrc =
        " {"
            + "  'name': 'stream_with_attributes',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'int'},"
            + "    {'name': 'two', 'type': 'int'}"
            + "  ]"
            + "}";
    final StreamConfig streamConfig =
        mapper.readValue(streamSrc.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setName(commonName);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String signalTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    Assert.assertTrue(conn.verifyTable(keyspaceName, signalTableName));
    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, contextTableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, signalTableName));
  }
}
