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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.errors.exception.AdminChangeRequestToSameException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.StreamModVerifier;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ReloadingModifiedStreamTest {

  private static AdminImpl admin;
  private static AdminStore adminStore;
  private static DataEngineImpl dataEngine;
  private static long timestamp;
  private static TestAdminChangeListener listener;
  private static CassandraConnection conn;

  // tenant used for a test
  private String testTenant;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    listener = new TestAdminChangeListener();
    Bios2TestModules.startModulesWithoutMaintenance(ReloadingModifiedStreamTest.class);
    admin = (AdminImpl) BiosModules.getAdminInternal();
    admin.addSubscriber(listener);
    adminStore = BiosModules.getAdminStore();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    timestamp = System.currentTimeMillis();
    conn = BiosModules.getCassandraConnection();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    listener.clear();
    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws Exception {
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
    listener.disableRecord();
    listener.unsetVerbose();
    listener.clear();
  }

  @Test
  public void testBasic() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testBasic";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSrc, ++timestamp);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'value', 'type': 'double'}"
            + "]}";
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    admin.modifyStream(
        testTenant, "testStream", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, "testStream", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    final Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    final Deque<TenantStoreDesc> deq = storeDescMap.get(testTenant.toLowerCase());
    assertNotNull(deq);
    assertThat(deq.size(), greaterThanOrEqualTo(1));
    TenantStoreDesc tdesc = deq.remove();
    assertFalse(tdesc.isDeleted());
    assertEquals(testTenant, tdesc.getName());
    assertEquals(7, tdesc.getStreams().size());
    assertEquals(tdesc.getStreams().get(0).getType(), StreamType.SIGNAL);
    assertEquals(tdesc.getStreams().get(0).getVersion(), tdesc.getVersion());
    assertFalse(tdesc.getStreams().get(0).isDeleted());
    assertEquals(origConf.getName(), tdesc.getStreams().get(5).getName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(5).getVersion());
    assertFalse(tdesc.getStreams().get(5).isDeleted());
    assertNull(tdesc.getStreams().get(5).getPrevName());
    assertNull(tdesc.getStreams().get(5).getPrevVersion());
    assertNull(tdesc.getStreams().get(5).getSchemaName());
    assertNull(tdesc.getStreams().get(5).getSchemaVersion());
    assertEquals(FormatVersion.LATEST, tdesc.getStreams().get(5).getFormatVersion());

    assertEquals(newConf.getName(), tdesc.getStreams().get(6).getName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(6).getVersion());
    assertFalse(tdesc.getStreams().get(6).isDeleted());
    assertEquals(origConf.getName(), tdesc.getStreams().get(6).getPrevName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(6).getPrevVersion());
    assertEquals(newConf.getName(), tdesc.getStreams().get(6).getSchemaName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(6).getSchemaVersion());
    assertEquals(FormatVersion.LATEST, tdesc.getStreams().get(6).getFormatVersion());

    final StreamModVerifier verifier =
        new BasicVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class BasicVerifier extends StreamModVerifier {

    public BasicVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc newDesc, StreamConfig newConf, StreamConfig origConf) {
      assertFalse(newDesc.isDeleted());
      assertEquals(origConf.getName(), newDesc.getPrevName());
      assertEquals(origConf.getVersion(), newDesc.getPrevVersion());
      assertEquals(newConf.getName(), newDesc.getSchemaName());
      assertEquals(newConf.getVersion(), newDesc.getSchemaVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc prev, StreamConfig newConf, StreamConfig origConf) {
      assertFalse(prev.isDeleted());
      assertNull(prev.getPrev());
      assertNull(prev.getPrevName());
      assertNull(prev.getPrevVersion());
      assertEquals(origConf.getName(), prev.getSchemaName());
      assertEquals(origConf.getVersion(), prev.getSchemaVersion());

      assertNotNull(prev.getStreamConversion());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          prev.getStreamConversion().getAttributeConversion("country").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          prev.getStreamConversion().getAttributeConversion("state").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.ADD,
          prev.getStreamConversion().getAttributeConversion("city").getConversionType());
      assertEquals(
          "n/a", prev.getStreamConversion().getAttributeConversion("city").getDefaultValue());
      assertEquals(
          StreamConversion.ConversionType.CONVERT,
          prev.getStreamConversion().getAttributeConversion("value").getConversionType());
      assertEquals(
          InternalAttributeType.LONG,
          prev.getStreamConversion()
              .getAttributeConversion("value")
              .getOldDesc()
              .getAttributeType());
    }
  }

  @Test
  public void testDeleteAttr() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testDeleteAttr";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'city', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSrc, ++timestamp);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "testStream", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "testStream", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    List<AdminChange> record = listener.getRecords();
    assertEquals(record.toString(), 2, listener.getRecords().size());

    int i = 0;
    assertEquals("DELETE", listener.getRecords().get(i).op);
    assertEquals("testStream", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("testStream", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("testStream", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    final StreamModVerifier verifier =
        new DeleteAttrVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class DeleteAttrVerifier extends StreamModVerifier {
    public DeleteAttrVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(newConf.getVersion(), modified.getSchemaVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("country").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("state").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("value").getConversionType());
    }
  }

  @Test
  public void testNoChange() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testNoChange";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'missingValuePolicy': 'use_default',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': ''},"
            + "  {'name': 'value', 'type': 'long', 'defaultValue': 0}"
            + "]}";
    AdminTestUtils.populateStream(admin, testTenant, origSrc, timestamp);

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'missingValuePolicy': 'use_default',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': ''},"
            + "  {'name': 'value', 'type': 'long', 'defaultValue': 0}"
            + "]}";
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);

    try {
      admin.modifyStream(
          testTenant, "testStream", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
      fail("exception must be thrown");
    } catch (AdminChangeRequestToSameException e) {
      // expected
    }
  }

  @Test
  public void testAddPreProcess() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testAddPreProcess";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(
        admin, testTenant, TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);

    final String origSignalSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSignalSrc, ++timestamp);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'}"
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
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    final Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    final Deque<TenantStoreDesc> deq = storeDescMap.get(testTenant.toLowerCase());
    assertNotNull(deq);
    assertThat(deq.size(), greaterThanOrEqualTo(1));
    TenantStoreDesc tdesc = deq.remove();
    assertFalse(tdesc.isDeleted());
    assertEquals(testTenant, tdesc.getName());
    assertEquals(8, tdesc.getStreams().size());
    assertEquals(origConf.getName(), tdesc.getStreams().get(6).getName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(6).getVersion());
    assertFalse(tdesc.getStreams().get(6).isDeleted());
    assertNull(tdesc.getStreams().get(6).getPrevName());
    assertNull(tdesc.getStreams().get(6).getPrevVersion());
    assertNull(tdesc.getStreams().get(6).getSchemaName());
    assertNull(tdesc.getStreams().get(6).getSchemaVersion());

    assertEquals(newConf.getName(), tdesc.getStreams().get(7).getName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(7).getVersion());
    assertFalse(tdesc.getStreams().get(7).isDeleted());
    assertEquals(origConf.getName(), tdesc.getStreams().get(7).getPrevName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(7).getPrevVersion());
    assertEquals(newConf.getName(), tdesc.getStreams().get(7).getSchemaName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(7).getSchemaVersion());

    List<AdminChange> record = listener.getRecords();
    assertEquals(record.toString(), 2, listener.getRecords().size());

    assertEquals("DELETE", listener.getRecords().get(0).op);
    assertEquals("access", listener.getRecords().get(0).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(0).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(1).op);
    assertEquals("access", listener.getRecords().get(1).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(1).streamDesc.getVersion());

    final StreamModVerifier verifier =
        new AddPreProcessVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class AddPreProcessVerifier extends StreamModVerifier {
    public AddPreProcessVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(newConf.getVersion(), modified.getSchemaVersion());
      assertEquals("access", modified.getPrevName());
      assertEquals(origConf.getVersion(), modified.getPrevVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {
      assertNotNull(oldDesc);
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("ip").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.ADD,
          oldDesc.getStreamConversion().getAttributeConversion("country").getConversionType());
      assertEquals(
          "n/a", oldDesc.getStreamConversion().getAttributeConversion("country").getDefaultValue());
      assertEquals(
          StreamConversion.ConversionType.ADD,
          oldDesc.getStreamConversion().getAttributeConversion("state").getConversionType());
      assertEquals(
          "n/a", oldDesc.getStreamConversion().getAttributeConversion("state").getDefaultValue());
    }
  }

  @Test
  public void testAddRollup() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testAddRollup";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_GEO_LOCATION_SRC, timestamp);

    final String origSignalSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'}"
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
    final Long origVersion = Long.valueOf(++timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSignalSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long', 'defaultValue': 0}"
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
            + "  }],"
            + "  'views': [{"
            + "    'name': 'groupByRegion',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'groupByRegion',"
            + "    'rollups': [{"
            + "      'name': 'rollupRegion',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    final Map<String, Deque<TenantStoreDesc>> storeDescMap = adminStore.getTenantStoreDescMap();
    final Deque<TenantStoreDesc> deq = storeDescMap.get(testTenant.toLowerCase());
    assertNotNull(deq);
    assertThat(deq.size(), greaterThanOrEqualTo(1));
    TenantStoreDesc tdesc = deq.remove();
    assertFalse(tdesc.isDeleted());
    assertEquals(testTenant, tdesc.getName());
    assertEquals(8, tdesc.getStreams().size());
    assertEquals(tdesc.getStreams().get(0).getType(), StreamType.SIGNAL);
    assertEquals(tdesc.getStreams().get(0).getVersion(), tdesc.getVersion());
    assertFalse(tdesc.getStreams().get(0).isDeleted());
    assertEquals(tdesc.getStreams().get(5).getType(), StreamType.CONTEXT);
    assertEquals(tdesc.getStreams().get(5).getName(), "geo_location");
    assertFalse(tdesc.getStreams().get(5).isDeleted());
    assertEquals(origConf.getName(), tdesc.getStreams().get(6).getName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(6).getVersion());
    assertFalse(tdesc.getStreams().get(6).isDeleted());
    assertNull(tdesc.getStreams().get(6).getPrevName());
    assertNull(tdesc.getStreams().get(6).getPrevVersion());
    assertNull(tdesc.getStreams().get(6).getSchemaName());
    assertNull(tdesc.getStreams().get(6).getSchemaVersion());

    assertEquals(newConf.getName(), tdesc.getStreams().get(7).getName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(7).getVersion());
    assertFalse(tdesc.getStreams().get(7).isDeleted());
    assertEquals(origConf.getName(), tdesc.getStreams().get(7).getPrevName());
    assertEquals(origConf.getVersion(), tdesc.getStreams().get(7).getPrevVersion());
    assertEquals(newConf.getName(), tdesc.getStreams().get(7).getSchemaName());
    assertEquals(newConf.getVersion(), tdesc.getStreams().get(7).getSchemaVersion());

    List<AdminChange> record = listener.getRecords();
    assertEquals(record.toString(), 5, listener.getRecords().size());

    assertEquals("DELETE", listener.getRecords().get(0).op);
    assertEquals("access", listener.getRecords().get(0).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(0).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(1).op);
    assertEquals("access", listener.getRecords().get(1).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(1).streamDesc.getVersion());
    assertEquals("access", listener.getRecords().get(1).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(1).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(2).op);
    assertEquals("access.view.groupByRegion", listener.getRecords().get(2).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(2).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(3).op);
    assertEquals("access.index.groupByRegion", listener.getRecords().get(3).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(3).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(4).op);
    assertEquals("rollupRegion", listener.getRecords().get(4).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(4).streamDesc.getVersion());

    final StreamModVerifier verifier =
        new AddRollupVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class AddRollupVerifier extends StreamModVerifier {
    public AddRollupVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(newConf.getVersion(), modified.getSchemaVersion());
      assertEquals("access", modified.getPrevName());
      assertEquals(origConf.getVersion(), modified.getPrevVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {
      assertNotNull(oldDesc);
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("ip").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("country").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("state").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.ADD,
          oldDesc.getStreamConversion().getAttributeConversion("score").getConversionType());
      assertEquals(
          Long.valueOf(0L),
          oldDesc.getStreamConversion().getAttributeConversion("score").getDefaultValue());
    }
  }

  @Test
  public void testSignalNoMod() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testSignalNoMod";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_GEO_LOCATION_SRC, timestamp);

    final String origSignalSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'}"
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
    final Long origVersion = Long.valueOf(++timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSignalSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'}"
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
            + "  }],"
            + "  'views': [{"
            + "    'name': 'groupByRegion',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'groupByRegion',"
            + "    'rollups': [{"
            + "      'name': 'rollupRegion',"
            + "      'interval': { 'value': 5, 'timeunit': 'minute' },"
            + "      'horizon': { 'value': 15, 'timeunit': 'minute' }"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    assertEquals(5, listener.getRecords().size());

    int i = 0;
    assertEquals("DELETE", listener.getRecords().get(i).op);
    assertEquals("access", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("access", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access.view.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access.index.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("rollupRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    final StreamModVerifier verifier =
        new SignalNoModVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class SignalNoModVerifier extends StreamModVerifier {
    public SignalNoModVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(origConf.getName(), modified.getSchemaName());
      assertEquals(origConf.getVersion(), modified.getSchemaVersion());
      assertNull(modified.getPrevName());
      assertNull(modified.getPrevVersion());

      assertTrue(modified.getSubStreamNames().contains("access.index.groupByRegion"));
      assertTrue(modified.getSubStreamNames().contains("access.view.groupByRegion"));
      assertTrue(modified.getSubStreamNames().contains("rollupRegion"));
    }

    @Override
    protected boolean hasPrev() {
      return false;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {}

    @Override
    protected void extraCheck(
        AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
        throws Exception {
      assertNotNull(admin.getStream(testTenant, "access.index.groupByRegion"));
      assertNotNull(admin.getStream(testTenant, "access.view.groupByRegion"));
      assertNotNull(admin.getStream(testTenant, "rollupRegion"));
    }
  }

  @Test
  public void tesNoRollupChange() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testSignalNoMod";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_GEO_LOCATION_SRC, timestamp);

    final String origSignalSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'}"
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
            + "  }],"
            + "  'views': [{"
            + "    'name': 'groupByRegion',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'groupByRegion',"
            + "    'rollups': [{"
            + "      'name': 'rollupRegion',"
            + "      'interval': { 'value': 5, 'timeunit': 'minute' },"
            + "      'horizon': { 'value': 15, 'timeunit': 'minute' }"
            + "    }]"
            + "  }]"
            + "}";
    final Long origVersion = Long.valueOf(++timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSignalSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'access',"
            + "  'attributes': ["
            + "    {'name': 'ip', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'},"
            + "    {'name': 'role', 'type': 'enum', 'enum': ['USER', 'ADMIN'], 'defaultValue': 'USER'}"
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
            + "  }],"
            + "  'views': [{"
            + "    'name': 'groupByRegion',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'groupByRegion',"
            + "    'rollups': [{"
            + "      'name': 'rollupRegion',"
            + "      'interval': { 'value': 5, 'timeunit': 'minute' },"
            + "      'horizon': { 'value': 15, 'timeunit': 'minute' }"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "access", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    List<AdminChange> record = listener.getRecords();
    assertEquals(record.toString(), 8, listener.getRecords().size());

    int i = 0;
    assertEquals("DELETE", listener.getRecords().get(i).op);
    assertEquals("access.view.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("access.index.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("rollupRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("access", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("access", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access.view.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals(
        "access.view.groupByRegion", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("access.index.groupByRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals(
        "access.index.groupByRegion", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("rollupRegion", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("rollupRegion", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(origVersion, listener.getRecords().get(i).streamDesc.getSchemaVersion());

    final StreamModVerifier verifier =
        new NoRollupChangeVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class NoRollupChangeVerifier extends StreamModVerifier {
    public NoRollupChangeVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(newConf.getVersion(), modified.getSchemaVersion());
      assertEquals("access", modified.getPrevName());
      assertEquals(origConf.getVersion(), modified.getPrevVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("ip").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("country").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("state").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.NO_CHANGE,
          oldDesc.getStreamConversion().getAttributeConversion("score").getConversionType());
      assertEquals(
          StreamConversion.ConversionType.ADD,
          oldDesc.getStreamConversion().getAttributeConversion("role").getConversionType());
      assertEquals(
          0, oldDesc.getStreamConversion().getAttributeConversion("role").getDefaultValue());
    }

    @Override
    protected void extraCheck(
        AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
        throws Exception {
      final StreamDesc rollup = admin.getStream(testTenant, "rollupRegion");
      assertNotNull(rollup);
      assertEquals("rollupRegion", rollup.getName());
      assertEquals(newConf.getVersion(), rollup.getVersion());
      assertEquals("rollupRegion", rollup.getSchemaName());
      assertEquals(origConf.getVersion(), rollup.getSchemaVersion());
      assertNull(rollup.getPrev());

      TenantDesc tenantDesc = admin.getTenant(testTenant);

      final CassStream cassStream = dataEngine.getCassStream(rollup);
      assertNotNull(cassStream);
      final var s1 = rollup.duplicateWithoutIndeterminateFields();
      final var s2 = cassStream.getStreamDesc().duplicateWithoutIndeterminateFields();
      assertEquals(s1.getParentStream().getName(), s2.getParentStream().getName());
      s1.setParentStream(null);
      s2.setParentStream(null);
      assertEquals(s1, s2);
      assertNull(cassStream.getPrev());
    }
  }

  @Test
  public void tesModifyView() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testModifyView";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_ALL_TYPES_SRC, timestamp);

    final String origSrc =
        "{"
            + "  'name': 'signal_25',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttribute', 'type': 'string'},"
            + "    {'name': 'int', 'type': 'long'},"
            + "    {'name': 'number', 'type': 'number'},"
            + "    {'name': 'double', 'type': 'double'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'comment': 'strict_pp',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'name': 'ctx_update_pp',"
            + "    'condition': 'int',"
            + "    'actions': [{"
            + "      'actionType': 'merge',"
            + "      'context': 'ctx_all_types',"
            + "      'attribute': 'intAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute'"
            + "    }]"
            + "  }],"
            + "  'views': [{"
            + "    'name': 'view_25',"
            + "    'groupBy': ['stringAttribute', 'number'],"
            + "    'attributes': ['int', 'doubleAttribute']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_25',"
            + "    'rollups': [{"
            + "      'name': 'rollup_25',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final Long origVersion = Long.valueOf(++timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'signal_25',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttribute', 'type': 'string'},"
            + "    {'name': 'int', 'type': 'long'},"
            + "    {'name': 'number', 'type': 'number'},"
            + "    {'name': 'double', 'type': 'double'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'comment': 'strict_pp',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'name': 'ctx_update_pp',"
            + "    'condition': 'int',"
            + "    'actions': [{"
            + "      'actionType': 'merge',"
            + "      'context': 'ctx_all_types',"
            + "      'attribute': 'intAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute'"
            + "    }]"
            + "  }],"
            + "  'views': [{"
            + "    'name': 'view_25',"
            + "    'groupBy': ['stringAttribute', 'doubleAttribute'],"
            + "    'attributes': []"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_25',"
            + "    'rollups': [{"
            + "      'name': 'rollup_25',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, "signal_25", newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, "signal_25", newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    List<AdminChange> record = listener.getRecords();
    assertEquals(record.toString(), 8, listener.getRecords().size());

    int i = 0;
    assertEquals("DELETE", listener.getRecords().get(i).op);
    assertEquals("signal_25.index.view_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("rollup_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("signal_25.view.view_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("DELETE", listener.getRecords().get(++i).op);
    assertEquals("signal_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("signal_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("signal_25", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(origConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("signal_25.view.view_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("signal_25.view.view_25", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("signal_25.index.view_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals(
        "signal_25.index.view_25", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    assertEquals("CREATE", listener.getRecords().get(++i).op);
    assertEquals("rollup_25", listener.getRecords().get(i).streamDesc.getName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getVersion());
    assertEquals("rollup_25", listener.getRecords().get(i).streamDesc.getSchemaName());
    assertEquals(newConf.getVersion(), listener.getRecords().get(i).streamDesc.getSchemaVersion());

    final StreamModVerifier verifier =
        new ModifyViewVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class ModifyViewVerifier extends StreamModVerifier {
    public ModifyViewVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals("signal_25", modified.getName());
      assertEquals(newConf.getVersion(), modified.getVersion());
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(origConf.getVersion(), modified.getSchemaVersion());
      assertNull(modified.getPrevName());
      assertNull(modified.getPrevVersion());
    }

    @Override
    protected boolean hasPrev() {
      return false;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {}

    @Override
    protected void extraCheck(
        AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
        throws Exception {

      final StreamDesc rollup = admin.getStream(testTenant, "rollup_25");
      assertNotNull(rollup);
      assertEquals("rollup_25", rollup.getName());
      assertEquals(newConf.getVersion(), rollup.getVersion());
      assertEquals("rollup_25", rollup.getSchemaName());
      assertEquals(newConf.getVersion(), rollup.getSchemaVersion());
      assertNull(rollup.getPrev());

      TenantDesc tenantDesc = admin.getTenant(testTenant);

      CassStream cassStream = dataEngine.getCassStream(rollup);
      final var s1 = rollup.duplicateWithoutIndeterminateFields();
      final var s2 = cassStream.getStreamDesc().duplicateWithoutIndeterminateFields();
      assertEquals(s1.getParentStream().getName(), s2.getParentStream().getName());
      s1.setParentStream(null);
      s2.setParentStream(null);
      assertEquals(s1, s2);
      assertNull(cassStream.getPrev());
    }
  }

  @Test
  public void testSignalNoModRollupMod() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testSignalNoModRollupMod";
    timestamp = System.currentTimeMillis();
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    timestamp = System.currentTimeMillis();
    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_ALL_TYPES_SRC, timestamp);

    final String origSrc =
        "{"
            + "  'name': 'rollup_update_prp_pop',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'intAttribute', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view1',"
            + "    'groupBy': ['stringAttribute', 'intAttribute1'],"
            + "    'attributes': ['intAttribute']"
            + "  }, {"
            /*
             * + "    'name': 'view2',"
             * + "    'groupBy': ['enumAttribute'],"
             * + "    'attributes': ['numberAttribute1']"
             * + "  }, {"
             * + "    'name': 'view3',"
             * + "    'groupBy': ['booleanAttribute', 'uuidAttribute'],"
             * + "    'attributes': ['intAttribute']"
             * + "  }, {"
             * + "    'name': 'view4',"
             * + "    'groupBy': ['blobAttribute'],"
             * + "    'attributes': ['intAttribute']"
             * + "  }, {"
             */
            + "    'name': 'view5',"
            + "    'groupBy': ['stringAttribute'],"
            + "    'attributes': []"
            /*
             * + "  }, {"
             * + "    'name': 'view6',"
             * + "    'groupBy': [],"
             * + "    'attributes': []"
             * + "  }, {"
             * + "    'name': 'view7',"
             * + "    'groupBy': [],"
             * + "    'attributes': ['intAttribute1']"
             * + "  }, {"
             * + "    'name': 'view8',"
             * + "    'groupBy': [],"
             * + "    'attributes': ['numberAttribute1']"
             * + "  }, {"
             * + "    'name': 'view9',"
             * + "    'groupBy': ['stringAttribute', 'enumAttribute'],"
             * + "    'attributes': []"
             */
            + "  }],"
            + "  'preprocesses': [{"
            + "    'name': 'ctx_all_types',"
            + "    'condition': 'intAttribute',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'merge',"
            + "      'attribute': 'intAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'stringAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'numberAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'numberAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'doubleAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'doubleAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'inetAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'dateAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'timestampAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'uuidAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'booleanAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'enumAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'blobAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }]"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view1',"
            + "    'rollups': [{"
            + "      'name': 'rollup_prp_pop1',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5,'timeunit': 'minute'}"
            + "    }, {"
            + "      'name': 'rollup_prp_pop10',"
            + "      'interval': {'value': 5,'timeunit': 'minute'},"
            + "      'horizon': {'value': 15,'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            /*
             * + "    'view': 'view2',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop2',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view3',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop3',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view4',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop4',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             */
            + "    'view': 'view5',"
            + "    'rollups': [{"
            + "      'name': 'rollup_prp_pop5',"
            + "      'interval': {'value': 5,'timeunit': 'minute'},"
            + "      'horizon': {'value': 15,'timeunit': 'minute'}"
            + "    }]"
            /*
             * + "  }, {"
             * + "    'view': 'view6',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop6',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view7',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop7',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view8',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop8',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view9',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop9',"
             * + "      'interval': {'value': 5,'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15,'timeunit': 'minute'}"
             * + "    }]"
             */
            + "  }]"
            + "}";
    timestamp = System.currentTimeMillis();
    final Long origVersion = Long.valueOf(timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'rollup_update_prp_pop',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': [{'name': 'intAttribute', 'type': 'long'}],"
            + "  'views': [{"
            + "    'name': 'view1',"
            + "    'groupBy': ['stringAttribute', 'intAttribute1', 'dateAttribute',"
            + "                'timestampAttribute'],"
            + "    'attributes': ['intAttribute', 'doubleAttribute1']"
            + "  }, {"
            /*
             * + "    'name': 'view2',"
             * + "    'groupBy': ['enumAttribute','doubleAttribute','numberAttribute'],"
             * + "    'attributes': ['intAttribute','doubleAttribute1','numberAttribute1']"
             * + "  }, {"
             * + "    'name': 'view3',"
             * + "    'groupBy': ['booleanAttribute', 'uuidAttribute'],"
             * +
             * "    'attributes': ['intAttribute', 'doubleAttribute1', 'numberAttribute1']"
             * + "  }, {"
             * + "    'name': 'view4',"
             * + "    'groupBy': ['blobAttribute', 'inetAttribute'],"
             * +
             * "    'attributes': ['intAttribute', 'doubleAttribute1', 'numberAttribute1']"
             * + "  }, {"
             */
            + "    'name': 'view5',"
            + "    'groupBy': ['stringAttribute'],"
            + "    'attributes': []"
            /*
             * + "  }, {"
             * + "    'name': 'view6',"
             * + "    'groupBy': [],"
             * + "    'attributes': []"
             * + "  }, {"
             * + "    'name': 'view7',"
             * + "    'groupBy': [],"
             * + "    'attributes': ['intAttribute1']"
             * + "  }, {"
             * + "    'name': 'view8',"
             * + "    'groupBy': [],"
             * +
             * "    'attributes': ['intAttribute1', 'doubleAttribute1', 'numberAttribute1']"
             * + "  }, {"
             * + "    'name': 'view9',"
             * + "    'groupBy': ['stringAttribute', 'enumAttribute', 'inetAttribute'],"
             * + "    'attributes': []"
             */
            + "  }],"
            + "  'preprocesses': [{"
            + "    'name': 'ctx_all_types',"
            + "    'condition': 'intAttribute',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'merge',"
            + "      'attribute': 'intAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'stringAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'numberAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'numberAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'doubleAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'doubleAttribute1',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'inetAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'dateAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'timestampAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'uuidAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'booleanAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'enumAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }, {"
            + "      'actionType': 'merge',"
            + "      'attribute': 'blobAttribute',"
            + "      'context': 'ctx_all_types',"
            + "      'missingLookupPolicy': 'strict'"
            + "    }]"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view1',"
            + "    'rollups': [{"
            + "      'name': 'rollup_prp_pop1',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }, {"
            + "      'name': 'rollup_prp_pop10',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            /*
             * + "    'view': 'view2',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop2',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view3',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop3',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view4',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop4',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             */
            + "    'view': 'view5',"
            + "    'rollups': [{"
            + "      'name': 'rollup_prp_pop5',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            /*
             * + "  }, {"
             * + "    'view': 'view6',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop6',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view7',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop7',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view8',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop8',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             * + "  }, {"
             * + "    'view': 'view9',"
             * + "    'rollups': [{"
             * + "      'name': 'rollup_prp_pop9',"
             * + "      'interval': {'value': 5, 'timeunit': 'minute'},"
             * + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
             * + "    }]"
             */
            + "  }],"
            + "  'schemaName': 'rollup_update_prp_pop',"
            + "  'schemaVersion': 1550093442024"
            + "}";
    timestamp = System.currentTimeMillis();
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, timestamp);
    // listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(
        testTenant, origConf.getName(), newConf, RequestPhase.INITIAL, FORCE, Set.of());
    listener.disableRecord();
    // listener.unsetVerbose();
    admin.modifyStream(
        testTenant, origConf.getName(), newConf, RequestPhase.FINAL, FORCE, Set.of());

    List<AdminChange> records = listener.getRecords();
    // assertEquals(60, records.size());

    boolean foundPop1Delete = false;
    boolean foundPop1Create = false;
    boolean foundPop5Delete = false;
    boolean foundPop5Create = false;
    for (AdminChange record : records) {
      if (record.streamDesc.getName().equals("rollup_prp_pop1")) {
        if (record.op.equals("DELETE")) {
          foundPop1Delete = true;
          assertEquals(origConf.getVersion(), record.streamDesc.getVersion());
          assertNull(record.streamDesc.getPrev());
          assertEquals("rollup_prp_pop1", record.streamDesc.getSchemaName());
          assertEquals(origConf.getVersion(), record.streamDesc.getSchemaVersion());
        } else if (record.op.equals("CREATE")) {
          foundPop1Create = true;
          assertEquals(newConf.getVersion(), record.streamDesc.getVersion());
          assertNull(record.streamDesc.getPrev());
          assertEquals("rollup_prp_pop1", record.streamDesc.getSchemaName());
          assertEquals(newConf.getVersion(), record.streamDesc.getSchemaVersion());
        }
      } else if (record.streamDesc.getName().equals("rollup_prp_pop5")) {
        if (record.op.equals("DELETE")) {
          foundPop5Delete = true;
          assertEquals(origConf.getVersion(), record.streamDesc.getVersion());
          assertNull(record.streamDesc.getPrev());
          assertEquals("rollup_prp_pop5", record.streamDesc.getSchemaName());
          assertEquals(origConf.getVersion(), record.streamDesc.getSchemaVersion());
        } else if (record.op.equals("CREATE")) {
          foundPop5Create = true;
          assertEquals(newConf.getVersion(), record.streamDesc.getVersion());
          assertNull(record.streamDesc.getPrev());
          assertEquals("rollup_prp_pop5", record.streamDesc.getSchemaName());
          assertEquals(origConf.getVersion(), record.streamDesc.getSchemaVersion());
        }
      }
    }
    assertTrue(foundPop1Delete);
    assertTrue(foundPop1Create);
    assertTrue(foundPop5Delete);
    assertTrue(foundPop5Create);

    final StreamModVerifier verifier =
        new SignalNoModRollupModVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class SignalNoModRollupModVerifier extends StreamModVerifier {
    public SignalNoModRollupModVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getName());
      assertEquals(newConf.getVersion(), modified.getVersion());
      assertEquals(origConf.getName(), modified.getSchemaName());
      assertEquals(origConf.getVersion(), modified.getSchemaVersion());
      assertNull(modified.getPrevName());
      assertNull(modified.getPrevVersion());
    }

    @Override
    protected boolean hasPrev() {
      return false;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {}

    @Override
    protected void extraCheck(
        AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
        throws Exception {
      final StreamDesc rollupPop1 = admin.getStream(testTenant, "rollup_prp_pop1");
      assertNotNull(rollupPop1);
      assertEquals("rollup_prp_pop1", rollupPop1.getName());
      assertEquals(newConf.getVersion(), rollupPop1.getVersion());
      assertEquals("rollup_prp_pop1", rollupPop1.getSchemaName());
      assertEquals(newConf.getVersion(), rollupPop1.getSchemaVersion());
      assertNull(rollupPop1.getPrev());

      final StreamDesc rollupPop5 = admin.getStream(testTenant, "rollup_prp_pop5");
      assertNotNull(rollupPop5);
      assertEquals("rollup_prp_pop5", rollupPop5.getName());
      assertEquals(newConf.getVersion(), rollupPop5.getVersion());
      assertEquals("rollup_prp_pop5", rollupPop5.getSchemaName());
      assertEquals(origConf.getVersion(), rollupPop5.getSchemaVersion());
      assertNull(rollupPop5.getPrev());
    }
  }

  @Test
  public void testAllMod() throws Exception {
    testTenant = this.getClass().getSimpleName() + "_testAllMod";
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    AdminTestUtils.populateStream(admin, testTenant, TestUtils.CONTEXT_ALL_TYPES_SRC, timestamp);

    final String origSrc =
        "{"
            + "  'name': 'attr_m_pre_k_view_m_rlp_m',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes':["
            + "    {'name': 'intAttribute', 'type': 'long'},"
            + "    {'name': 'modFromLongToDouble', 'type': 'long'},"
            + "    {'name': 'modFromDoubleToLong', 'type': 'double'}"
            + "  ],"
            + "  'preprocesses':[{"
            + "    'name': 'ctx_all_types',"
            + "    'condition': 'intAttribute',"
            + "    'actions':[{"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'intAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'stringAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'numberAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'numberAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'inetAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'dateAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'timestampAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'uuidAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'booleanAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'enumAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'blobAttribute'"
            + "    }],"
            + "   'missingLookupPolicy': 'strict'"
            + "  }],"
            + "  'views':[{"
            + "    'name': 'view1',"
            + "    'groupBy':['stringAttribute', 'intAttribute1', 'dateAttribute',"
            + "               'timestampAttribute'],"
            + "    'attributes':['intAttribute', 'modFromLongToDouble', 'modFromDoubleToLong']"
            + "  }, {"
            + "    'name': 'view2',"
            + "    'groupBy':['stringAttribute', 'intAttribute1'],"
            + "    'attributes':['modFromDoubleToLong']"
            + "  }],"
            + "  'postprocesses':[{"
            + "    'view': 'view1',"
            + "    'rollups':[{"
            + "      'name': 'rollup_1',"
            + "      'interval':{'value':5, 'timeunit': 'minute'},"
            + "      'horizon':{'value':5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view2',"
            + "    'rollups':[{"
            + "      'name': 'rollup_prp_pop2',"
            + "      'interval':{'value':5, 'timeunit': 'minute'},"
            + "      'horizon':{'value':5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final Long origVersion = Long.valueOf(++timestamp);
    final StreamConfig origConf = AdminConfigCreator.makeStreamConfig(origSrc, origVersion);
    admin.addStream(testTenant, origConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, origConf, RequestPhase.FINAL);

    final String newSrc =
        "{"
            + "  'name': 'attr_m_pre_k_view_m_rlp_m',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'intAttribute', 'type': 'long'},"
            + "    {'name': 'modFromLongToDouble', 'type': 'double'}," // was long
            + "    {'name': 'modFromDoubleToLong', 'type': 'long'," // was double
            + "     'defaultValue': -1}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'ctx_all_types',"
            + "    'condition': 'intAttribute',"
            + "    'actions':[{"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'intAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'stringAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'numberAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'numberAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'doubleAttribute1'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'inetAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'dateAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'timestampAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'uuidAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'booleanAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'enumAttribute'"
            + "    }, {"
            + "      'actionType': 'merge', 'context': 'ctx_all_types',"
            + "      'attribute': 'blobAttribute'"
            + "    }],"
            + "    'missingLookupPolicy': 'strict'"
            + "  }],"
            + "  'views': [{"
            + "    'name': 'view1'," // keep but modFromLongToDouble and modFromDoubleToLong are
            // changed
            + "    'groupBy': ['stringAttribute', 'intAttribute1', 'dateAttribute',"
            + "                'timestampAttribute'],"
            + "    'attributes': ['intAttribute', 'modFromLongToDouble', 'modFromDoubleToLong']"
            + "  }, {"
            + "    'name': 'view2'," // modified
            + "    'groupBy': ['stringAttribute', 'dateAttribute'],"
            + "    'attributes': ['modFromLongToDouble', 'modFromDoubleToLong']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view1',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1'," // keep but view1 has changed
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view2',"
            + "    'rollups': [{"
            + "      'name': 'rollup_2'," // new
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, ++timestamp);
    final String testStream = newConf.getName();
    listener.setVerbose();
    listener.enableRecord();
    admin.modifyStream(testTenant, testStream, newConf, RequestPhase.INITIAL, FORCE, Set.of());
    listener.disableRecord();
    listener.unsetVerbose();
    admin.modifyStream(testTenant, testStream, newConf, RequestPhase.FINAL, FORCE, Set.of());

    assertEquals(14, listener.getRecords().size());

    Set<String> deleting =
        new HashSet<>(
            Arrays.asList(
                "rollup_1",
                "rollup_prp_pop2",
                testStream + ".view.view1",
                testStream + ".index.view1",
                testStream + ".view.view2",
                testStream + ".index.view2",
                testStream));

    Set<String> adding =
        new HashSet<>(
            Arrays.asList(
                "rollup_1",
                "rollup_2",
                testStream + ".view.view1",
                testStream + ".index.view1",
                testStream + ".view.view2",
                testStream + ".index.view2",
                testStream));

    for (int i = 0; i < 7; ++i) {
      final AdminChange record = listener.getRecords().get(i);
      assertEquals("DELETE", record.op);
      assertEquals(origConf.getVersion(), record.streamDesc.getVersion());
      assertTrue(record.streamDesc.getName(), deleting.remove(record.streamDesc.getName()));
    }
    assertTrue(deleting.isEmpty());

    Map<String, AdminChange> records = new HashMap<>();
    for (int i = 7; i < 14; ++i) {
      final AdminChange record = listener.getRecords().get(i);
      assertEquals("CREATE", record.op);
      assertEquals(newConf.getVersion(), record.streamDesc.getVersion());
      records.put(record.streamDesc.getName(), record);
    }
    assertEquals(7, records.size());

    { // verify stream desc
      final AdminChange currentChange = records.get(testStream);
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals(testStream, currentChange.streamDesc.getSchemaName());

      assertNotNull(currentChange.streamDesc.getPrev());
      final StreamConversion streamConv = currentChange.streamDesc.getPrev().getStreamConversion();
      assertNotNull(streamConv);

      StreamConversion.AttributeConversion attrConv =
          streamConv.getAttributeConversion("intAttribute");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.NO_CHANGE, attrConv.getConversionType());

      attrConv =
          currentChange
              .streamDesc
              .getPrev()
              .getStreamConversion()
              .getAttributeConversion("modFromLongToDouble");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.CONVERT, attrConv.getConversionType());
      assertEquals(InternalAttributeType.LONG, attrConv.getOldDesc().getAttributeType());

      attrConv =
          currentChange
              .streamDesc
              .getPrev()
              .getStreamConversion()
              .getAttributeConversion("modFromDoubleToLong");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.ADD, attrConv.getConversionType());
      assertEquals(-1L, attrConv.getDefaultValue());
    }
    {
      final AdminChange currentChange = records.get(testStream + ".view.view1");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals(testStream + ".view.view1", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }
    {
      final AdminChange currentChange = records.get(testStream + ".index.view1");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals(testStream + ".index.view1", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }
    {
      final AdminChange currentChange = records.get(testStream + ".view.view2");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals(testStream + ".view.view2", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }
    {
      final AdminChange currentChange = records.get(testStream + ".index.view2");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals(testStream + ".index.view2", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }
    {
      final AdminChange currentChange = records.get("rollup_1");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals("rollup_1", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }
    {
      final AdminChange currentChange = records.get("rollup_2");
      assertNotNull(currentChange);
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getVersion());
      assertEquals(newConf.getVersion(), currentChange.streamDesc.getSchemaVersion());
      assertEquals("rollup_2", currentChange.streamDesc.getSchemaName());
      assertNull(currentChange.streamDesc.getPrev());
    }

    final StreamModVerifier verifier =
        new AllModVerifier(admin, dataEngine, adminStore, conn, testTenant);
    verifier.run(newConf, origConf);
  }

  private class AllModVerifier extends StreamModVerifier {
    public AllModVerifier(
        AdminInternal admin,
        DataEngineImpl dataEngine,
        AdminStore adminStore,
        CassandraConnection conn,
        String testTenant) {
      super(admin, dataEngine, adminStore, conn, testTenant);
    }

    @Override
    protected void checkNew(StreamDesc modified, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(newConf.getName(), modified.getName());
      assertEquals(newConf.getVersion(), modified.getVersion());
      assertEquals(newConf.getName(), modified.getSchemaName());
      assertEquals(newConf.getVersion(), modified.getSchemaVersion());
    }

    @Override
    protected boolean hasPrev() {
      return true;
    }

    @Override
    protected void checkOld(StreamDesc oldDesc, StreamConfig newConf, StreamConfig origConf) {
      assertEquals(origConf.getName(), oldDesc.getName());
      assertEquals(origConf.getVersion(), oldDesc.getVersion());
      assertNull(oldDesc.getPrev());

      final StreamConversion conv = oldDesc.getStreamConversion();
      assertNotNull(conv);

      StreamConversion.AttributeConversion attrConv = conv.getAttributeConversion("intAttribute");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.NO_CHANGE, attrConv.getConversionType());

      attrConv = conv.getAttributeConversion("modFromLongToDouble");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.CONVERT, attrConv.getConversionType());
      assertEquals(InternalAttributeType.LONG, attrConv.getOldDesc().getAttributeType());

      attrConv = conv.getAttributeConversion("modFromDoubleToLong");
      assertNotNull(attrConv);
      assertEquals(StreamConversion.ConversionType.ADD, attrConv.getConversionType());
      assertEquals(-1L, attrConv.getDefaultValue());
    }

    @Override
    protected void extraCheck(
        AdminInternal admin, DataEngineImpl dataEngine, StreamConfig newConf, StreamConfig origConf)
        throws Exception {

      final StreamDesc view1 = admin.getStream(testTenant, newConf.getName() + ".view.view1");
      assertNotNull(view1);

      final CassStream cassView1 = dataEngine.getCassStream(view1);
      assertNotNull(cassView1);

      final var s1 = view1.duplicateWithoutIndeterminateFields();
      final var s2 = cassView1.getStreamDesc().duplicateWithoutIndeterminateFields();
      assertEquals(s1.getParentStream().getName(), s2.getParentStream().getName());
      s1.setParentStream(null);
      s2.setParentStream(null);
      assertEquals(s1, s2);

      AttributeDesc desc = cassView1.getAttributeDesc("modFromLongToDouble");
      assertNotNull(desc);
      assertEquals(InternalAttributeType.DOUBLE, desc.getAttributeType());

      desc = cassView1.getAttributeDesc("modFromDoubleToLong");
      assertNotNull(desc);
      assertEquals(InternalAttributeType.LONG, desc.getAttributeType());

      final StreamDesc rollup1 = admin.getStream(testTenant, "rollup_1");
      assertNotNull(view1);

      final CassStream cassRollup1 = dataEngine.getCassStream(rollup1);
      assertNotNull(cassRollup1);

      desc = cassRollup1.getAttributeDesc("modFromLongToDouble_min");
      assertNotNull(desc);
      assertEquals(InternalAttributeType.DOUBLE, desc.getAttributeType());

      desc = cassRollup1.getAttributeDesc("modFromDoubleToLong_min");
      assertNotNull(desc);
      assertEquals(InternalAttributeType.LONG, desc.getAttributeType());
    }
  }

  // end test cases ///////////////////////////////////////////////////////////

  private static class TestAdminChangeListener implements AdminChangeListener {
    private boolean recordEnabled;
    private boolean isVerbose;
    private final List<AdminChange> record;

    public TestAdminChangeListener() {
      recordEnabled = false;
      isVerbose = false;
      record = new ArrayList<>();
    }

    public void enableRecord() {
      recordEnabled = true;
    }

    public void disableRecord() {
      recordEnabled = false;
    }

    public void setVerbose() {
      isVerbose = true;
    }

    public void unsetVerbose() {
      isVerbose = false;
    }

    public List<AdminChange> getRecords() {
      return record;
    }

    public void clear() {
      record.clear();
    }

    @Override
    public void createTenant(TenantDesc tenantDesc, RequestPhase phase)
        throws ApplicationException {
      if (recordEnabled) {
        if (isVerbose) {
          System.out.println("CREATE " + tenantDesc.getName());
        }
      }
    }

    @Override
    public void deleteTenant(TenantDesc tenantDesc, RequestPhase phase)
        throws NoSuchTenantException, ApplicationException {
      if (recordEnabled) {
        if (isVerbose) {
          System.out.println("DELETE " + tenantDesc.getName());
        }
      }
    }

    @Override
    public void createStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
        throws NoSuchTenantException, ApplicationException {
      if (recordEnabled) {
        final AdminChange change = new AdminChange("CREATE", tenantName, streamDesc);
        if (isVerbose) {
          System.out.println(change.toString());
        }
        record.add(change);
      }
    }

    @Override
    public void deleteStream(String tenantName, StreamDesc streamDesc, RequestPhase phase)
        throws NoSuchTenantException, NoSuchStreamException, ApplicationException {
      if (recordEnabled) {
        final AdminChange change = new AdminChange("DELETE", tenantName, streamDesc);
        if (isVerbose) {
          System.out.println(change.toString());
        }
        record.add(change);
      }
    }

    @Override
    public void unload() {}
  }

  private static class AdminChange {
    public final String op;
    public final String tenantName;
    public final StreamDesc streamDesc;

    public AdminChange(String op, String tenantName, StreamDesc streamDesc) {
      this.op = op;
      this.tenantName = tenantName;
      this.streamDesc = streamDesc;
    }

    @Override
    public String toString() {
      StringBuilder sb =
          new StringBuilder(op).append(" ").append(tenantName).append(" ").append(streamDesc);
      return sb.toString();
    }
  }
}
