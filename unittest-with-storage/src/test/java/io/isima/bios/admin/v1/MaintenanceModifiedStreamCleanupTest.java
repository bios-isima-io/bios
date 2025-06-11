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

import static io.isima.bios.models.AttributeModAllowance.CONVERTIBLES_ONLY;
import static io.isima.bios.models.AttributeModAllowance.FORCE;

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
import io.isima.bios.models.Rollup;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MaintenanceModifiedStreamCleanupTest {
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
    Bios2TestModules.startModules(true, MaintenanceModifiedStreamCleanupTest.class, Map.of());
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
  public void testNoTableRemovalForStreamWhenAttributeIsAdded() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_no_delete_when_attribute_added";
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

    final String updatedStreamSrc =
        " {"
            + "  'name': 'stream_with_attributes',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'int'},"
            + "    {'name': 'two', 'type': 'int'},"
            + "    {'name': 'three', 'type': 'string', 'defaultValue':''}"
            + "  ]"
            + "}";
    final StreamConfig updatedStreamConfig =
        mapper.readValue(updatedStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    updatedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        updatedStreamConfig.getName(),
        updatedStreamConfig,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        updatedStreamConfig.getName(),
        updatedStreamConfig,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    String updatedTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, updatedStreamConfig.getName(), updatedStreamConfig.getVersion());

    dataEngine.maintainInternal(taskSlots);
    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, updatedTableName));
  }

  @Test
  public void testNoTableRemovalForStreamWhenPostProcessIsAdded() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_no_delete_when_postprocess_added";
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
            + "  'name': 'stream_with_postprocess_added_later',"
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
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));

    final String updatedStreamSrc =
        " {"
            + "  'name': 'stream_with_postprocess_added_later',"
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
    final StreamConfig updatedStreamConfig =
        mapper.readValue(updatedStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    updatedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        updatedStreamConfig.getName(),
        updatedStreamConfig,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        updatedStreamConfig.getName(),
        updatedStreamConfig,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, updatedStreamConfig, keyspaceName, true, true);
  }

  @Test
  public void testNoTableRemovalWhenRollupIsModified() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_when_rollup_modified";
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

    final String streamDescSource =
        " {"
            + "  'name': 'stream_with_postprocess_modified',"
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
        mapper.readValue(streamDescSource.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);

    final String modifiedStreamDescSource =
        " {"
            + "  'name': 'stream_with_postprocess_modified',"
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
            + "      'name': 'rollup_test_view_modified',"
            + "      'interval': {'value': 60, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 60, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig modifiedStreamConfig =
        mapper.readValue(modifiedStreamDescSource.replaceAll("'", "\""), StreamConfig.class);
    modifiedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    String modifiedTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, modifiedStreamConfig.getName(), modifiedStreamConfig.getVersion());
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));
    MaintenanceTestUtils.verifySubstreamTables(
        conn, modifiedStreamConfig, keyspaceName, false, true);
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);
  }

  @Test
  public void testTableRemovalWhenViewIsModified() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_when_view_modified";
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
    final String streamDescSource =
        " {"
            + "  'name': 'stream_with_view_modified',"
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
        mapper.readValue(streamDescSource.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);

    final String modifiedStreamDescSource =
        " {"
            + "  'name': 'stream_with_view_modified',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'int'},"
            + "    {'name': 'two', 'type': 'int'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'test_view_modified',"
            + "    'groupBy': [],"
            + "    'attributes': ['one']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'test_view_modified',"
            + "    'rollups': [{"
            + "      'name': 'rollup_test_view_modified',"
            + "      'interval': {'value': 60, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 60, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig modifiedStreamConfig =
        mapper.readValue(modifiedStreamDescSource.replaceAll("'", "\""), StreamConfig.class);
    modifiedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    String modifiedTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, modifiedStreamConfig.getName(), modifiedStreamConfig.getVersion());
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));
    MaintenanceTestUtils.verifySubstreamTables(
        conn, modifiedStreamConfig, keyspaceName, true, true);
  }

  @Test
  public void testNoTableRemovalWhenNewPostProcessIsadded() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    final String nameOfNewPostProcess = "roll_up_" + System.currentTimeMillis();
    testTenant = this.getClass().getSimpleName() + "_tenant_delete_when_postprocess_is_added";
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
    final String streamDescSource =
        " {"
            + "  'name': 'stream_with_new_postprocess',"
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
        mapper.readValue(streamDescSource.replaceAll("'", "\""), StreamConfig.class);
    streamConfig.setVersion(++timestamp);

    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), streamConfig, RequestPhase.FINAL);

    String tableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, streamConfig.getName(), streamConfig.getVersion());
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);

    final String modifiedStreamDescSource =
        " {"
            + "  'name': 'stream_with_new_postprocess',"
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
            + "    }]}"
            + ",{"
            + "    'view': 'test_view',"
            + "    'rollups': [{"
            + "      'name': '"
            + nameOfNewPostProcess
            + "',"
            + "      'interval': {'value': 60, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 720, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig modifiedStreamConfig =
        mapper.readValue(modifiedStreamDescSource.replaceAll("'", "\""), StreamConfig.class);
    modifiedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        modifiedStreamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    String modifiedTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, modifiedStreamConfig.getName(), modifiedStreamConfig.getVersion());
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyKeyspace(keyspaceName));
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, modifiedTableName));
    MaintenanceTestUtils.verifySubstreamTables(conn, streamConfig, keyspaceName, true, true);
    modifiedStreamConfig
        .getPostprocesses()
        .forEach(
            postprocessDesc -> {
              List<Rollup> rollupList = postprocessDesc.getRollups();
              if (rollupList != null) {
                rollupList.forEach(
                    rollup -> {
                      if (rollup.getName().equals("another_rollup_test_view")) {
                        String rollUpQualifiedName =
                            CassStream.generateQualifiedNameSubstream(
                                StreamType.ROLLUP,
                                modifiedStreamConfig.getName(),
                                rollup.getName());
                        String rollUpTableName =
                            CassStream.generateTableName(
                                StreamType.ROLLUP,
                                rollUpQualifiedName,
                                modifiedStreamConfig.getVersion());
                        Assert.assertTrue(conn.verifyTable(keyspaceName, rollUpTableName));
                      }
                    });
              }
            });
  }

  @Ignore
  @Test
  public void testTableRemovalWhenStreamModifiedInInconvertibleWay() throws Exception {
    final AdminInternal admin = BiosModules.getAdminInternal();
    testTenant =
        this.getClass().getSimpleName() + "_tenant_with_stream_modified_in_incovertible_way";
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
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'}"
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
    Assert.assertTrue(conn.verifyTable(keyspaceName, tableName));

    final String modifiedStreamSrc =
        " {"
            + "  'name': 'stream_with_attributes',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'boolean', 'defaultValue': 'true'}"
            + "  ]"
            + "}";
    final StreamConfig modifiedStreamConfig =
        mapper.readValue(modifiedStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    modifiedStreamConfig.setVersion(++timestamp);

    admin.modifyStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.INITIAL,
        FORCE,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        streamConfig.getName(),
        modifiedStreamConfig,
        RequestPhase.FINAL,
        FORCE,
        Set.of());

    String newTableName =
        CassStream.generateTableName(
            StreamType.SIGNAL, modifiedStreamConfig.getName(), modifiedStreamConfig.getVersion());
    Assert.assertTrue(conn.verifyTable(keyspaceName, newTableName));

    dataEngine.maintainInternal(taskSlots);

    Assert.assertTrue(conn.verifyTable(keyspaceName, newTableName));
    Assert.assertFalse(conn.verifyTable(keyspaceName, tableName));
  }
}
