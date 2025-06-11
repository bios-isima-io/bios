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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ModifyStreamTest {

  private static ObjectMapper mapper;

  private static AdminStore adminStore;
  private static DataEngineImpl dataEngine;
  private static HttpClientManager clientManger;

  private static long timestamp;
  private static io.isima.bios.admin.Admin biosAdmin;
  private static DataServiceHandler dataServiceHandler;
  private static final String tenantName = "modifyStreamTest";

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantConfig> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
    Bios2TestModules.startModules(false, ModifyStreamTest.class, Map.of());
    adminStore = BiosModules.getAdminStore();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    clientManger = BiosModules.getHttpClientManager();
    biosAdmin = BiosModules.getAdmin();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    timestamp = System.currentTimeMillis();
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

  @Test
  public void testSimpleModify()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          ApplicationException,
          TfosException,
          FileReadException {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = mapper.readValue(src.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String initialStreamSrc =
        "{"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        mapper.readValue(initialStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    initialStream.setVersion(++timestamp);

    final String secondStreamSrc =
        "{"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string', 'defaultValue': '0.0.0.0'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        mapper.readValue(secondStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    secondStream.setVersion(++timestamp);

    final String thirdStreamSrc =
        "{"
            // TODO(TFOS-1273): rename is not supported yet
            // + " 'name': 'renamed',"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string', 'defaultValue': '0.0.0.0'},"
            + "    {'name': 'fourth', 'type': 'long', 'defaultValue': 0}"
            + "  ]"
            + "}";
    final StreamConfig thirdStream =
        mapper.readValue(thirdStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    thirdStream.setVersion(++timestamp);

    // add a stream
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    // modify the stream
    admin.modifyStream(
        tenantConfig.getName(),
        initialStream.getName(),
        secondStream,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        initialStream.getName(),
        secondStream,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    {
      final StreamDesc streamDesc = admin.getStream(tenantConfig.getName(), secondStream.getName());
      assertNotNull(streamDesc);
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(secondStream.getVersion(), streamDesc.getVersion());
      assertEquals(initialStream.getName(), streamDesc.getPrevName());
      assertEquals(initialStream.getVersion(), streamDesc.getPrevVersion());
      final StreamDesc prevDesc = streamDesc.getPrev();
      assertNotNull(prevDesc);
      assertEquals(initialStream.getName(), prevDesc.getName());
      assertEquals(initialStream.getVersion(), prevDesc.getVersion());
      assertNull(prevDesc.getPrevName());
      assertNull(prevDesc.getPrevVersion());
      assertNull(prevDesc.getPrev());
    }

    // modify the stream again
    admin.modifyStream(
        tenantConfig.getName(),
        secondStream.getName(),
        thirdStream,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        secondStream.getName(),
        thirdStream,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    {
      final StreamDesc streamDesc = admin.getStream(tenantConfig.getName(), thirdStream.getName());
      assertNotNull(streamDesc);
      // TODO(TFOS-1273): rename is not supported yet
      // assertEquals("renamed", streamDesc.getName());
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(thirdStream.getVersion(), streamDesc.getVersion());
      assertEquals(secondStream.getName(), streamDesc.getPrevName());
      assertEquals(secondStream.getVersion(), streamDesc.getPrevVersion());

      final StreamDesc prevDesc = streamDesc.getPrev();
      assertNotNull(prevDesc);
      assertEquals(secondStream.getName(), prevDesc.getName());
      assertEquals(secondStream.getVersion(), prevDesc.getVersion());

      final StreamDesc prevPrevDesc = prevDesc.getPrev();
      assertNotNull(prevPrevDesc);
      assertEquals(initialStream.getName(), prevPrevDesc.getName());
      assertEquals(initialStream.getVersion(), prevPrevDesc.getVersion());
    }

    // test reloading
    {
      AdminInternal admin2 = new AdminImpl(adminStore, new MetricsStreamProvider());
      final StreamDesc streamDesc = admin2.getStream(tenantConfig.getName(), thirdStream.getName());
      assertNotNull(streamDesc);
      // TODO(TFOS-1273): rename is not supported yet
      // assertEquals("renamed", streamDesc.getName());
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(thirdStream.getVersion(), streamDesc.getVersion());
      assertEquals(secondStream.getName(), streamDesc.getPrevName());
      assertEquals(secondStream.getVersion(), streamDesc.getPrevVersion());

      final StreamDesc prevDesc = streamDesc.getPrev();
      assertNotNull(prevDesc);
      assertEquals(secondStream.getName(), prevDesc.getName());
      assertEquals(secondStream.getVersion(), prevDesc.getVersion());

      final StreamDesc prevPrevDesc = prevDesc.getPrev();
      assertNotNull(prevPrevDesc);
      assertEquals(initialStream.getName(), prevPrevDesc.getName());
      assertEquals(initialStream.getVersion(), prevPrevDesc.getVersion());
    }
  }

  @Test
  public void testDeleteAndAdd()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          ApplicationException,
          TfosException,
          FileReadException {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = mapper.readValue(src.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String initialStreamSrc =
        "{"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        mapper.readValue(initialStreamSrc.replaceAll("'", "\""), StreamConfig.class);
    initialStream.setVersion(++timestamp);

    final String secondStreamSrc =
        "{"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string', 'defaultValue': '172.0.0.1'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        mapper.readValue(secondStreamSrc.replaceAll("'", "\""), StreamConfig.class);

    final String thirdStreamSrc =
        "{"
            + "  'name': 'mysignal',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'},"
            + "    {'name': 'fourth', 'type': 'long', 'defaultValue': -1}"
            + "  ]"
            + "}";
    final StreamConfig thirdStream =
        mapper.readValue(thirdStreamSrc.replaceAll("'", "\""), StreamConfig.class);

    // add a stream
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    // delete the stream
    admin.removeStream(
        tenantConfig.getName(), initialStream.getName(), RequestPhase.INITIAL, ++timestamp);
    admin.removeStream(
        tenantConfig.getName(), initialStream.getName(), RequestPhase.FINAL, timestamp);

    // add another stream with the same name
    secondStream.setVersion(++timestamp);
    admin.addStream(tenantConfig.getName(), secondStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), secondStream, RequestPhase.FINAL);

    {
      final StreamDesc streamDesc = admin.getStream(tenantConfig.getName(), secondStream.getName());
      assertNotNull(streamDesc);
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(secondStream.getVersion(), streamDesc.getVersion());
      assertNull(streamDesc.getPrevName());
      assertNull(streamDesc.getPrevVersion());
      assertNull(streamDesc.getPrev());
    }

    // modify the stream
    thirdStream.setVersion(++timestamp);
    admin.modifyStream(
        tenantConfig.getName(),
        secondStream.getName(),
        thirdStream,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        secondStream.getName(),
        thirdStream,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    {
      final StreamDesc streamDesc = admin.getStream(tenantConfig.getName(), thirdStream.getName());
      assertNotNull(streamDesc);
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(thirdStream.getVersion(), streamDesc.getVersion());
      assertEquals(secondStream.getName(), streamDesc.getPrevName());
      assertEquals(secondStream.getVersion(), streamDesc.getPrevVersion());

      final StreamDesc prevDesc = streamDesc.getPrev();
      assertNotNull(prevDesc);
      assertEquals(secondStream.getName(), prevDesc.getName());
      assertEquals(secondStream.getVersion(), prevDesc.getVersion());
      assertNull(prevDesc.getPrev());
    }

    // test reloading
    {
      AdminInternal admin2 = new AdminImpl(adminStore, new MetricsStreamProvider());
      final StreamDesc streamDesc = admin2.getStream(tenantConfig.getName(), thirdStream.getName());
      assertNotNull(streamDesc);
      assertEquals("mysignal", streamDesc.getName());
      assertEquals(thirdStream.getVersion(), streamDesc.getVersion());
      assertEquals(secondStream.getName(), streamDesc.getPrevName());
      assertEquals(secondStream.getVersion(), streamDesc.getPrevVersion());

      final StreamDesc prevDesc = streamDesc.getPrev();
      assertNotNull(prevDesc);
      assertEquals(secondStream.getName(), prevDesc.getName());
      assertEquals(secondStream.getVersion(), prevDesc.getVersion());
      assertNull(prevDesc.getPrev());
    }
  }

  @Test
  public void testDeleteContextKey() throws Exception {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(src, ++timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final String initialStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'string'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        AdminConfigCreator.makeStreamConfig(initialStreamSrc, ++timestamp);

    final String secondStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        AdminConfigCreator.makeStreamConfig(secondStreamSrc, ++timestamp);

    // add a stream
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    // modify the stream
    try {
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.INITIAL,
          FORCE,
          Set.of());
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.FINAL,
          FORCE,
          Set.of());
      fail("exception must occur");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation: Primary key attributes may not be modified;"
              + " tenant=modifyStreamTest, context=mycontext",
          e.getMessage());
    }
  }

  @Test
  public void testModifyContextKey() throws Exception {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(src, ++timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final String initialStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'long'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        AdminConfigCreator.makeStreamConfig(initialStreamSrc, ++timestamp);

    final String secondStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'first', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        AdminConfigCreator.makeStreamConfig(secondStreamSrc, ++timestamp);

    // add a stream
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    // modify the stream
    try {
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.INITIAL,
          FORCE,
          Set.of());
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.FINAL,
          FORCE,
          Set.of());
      fail("exception must occur");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation: Primary key attributes may not be modified;"
              + " tenant=modifyStreamTest, context=mycontext",
          e.getMessage());
    }
  }

  @Test
  public void testShuffleContextAttributes() throws Exception {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(src, ++timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final String initialStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'first', 'type': 'long'},"
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig initialStream =
        AdminConfigCreator.makeStreamConfig(initialStreamSrc, ++timestamp);

    final String secondStreamSrc =
        "{"
            + "  'name': 'mycontext',"
            + "  'type': 'context',"
            + "  'attributes': ["
            + "    {'name': 'second', 'type': 'long'},"
            + "    {'name': 'first', 'type': 'long'},"
            + "    {'name': 'third', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamConfig secondStream =
        AdminConfigCreator.makeStreamConfig(secondStreamSrc, ++timestamp);

    // add a stream
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), initialStream, RequestPhase.FINAL);

    // modify the stream
    try {
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.INITIAL,
          FORCE,
          Set.of());
      admin.modifyStream(
          tenantConfig.getName(),
          initialStream.getName(),
          secondStream,
          RequestPhase.FINAL,
          FORCE,
          Set.of());
      fail("exception must occur");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation: Primary key attributes may not be modified;"
              + " tenant=modifyStreamTest, context=mycontext",
          e.getMessage());
    }
  }

  @Test
  public void appendRollup() throws Exception {
    final String src = "{'name': 'appendRollup'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(src, ++timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final String initialSrc =
        "{"
            + "  'name': 'test_append_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'rollup_byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, ++timestamp);

    final String modifiedSrc =
        "{"
            + "  'name': 'test_append_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['value']"
            + "  }, {"
            + "    'name': 'byTwo',"
            + "    'groupBy': ['two'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'rollup_byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'byTwo',"
            + "    'rollups': [{"
            + "      'name': 'rollup_byTwo',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(modifiedSrc, ++timestamp);

    admin.addStream(tenantConfig.getName(), signalConf0, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), signalConf0, RequestPhase.FINAL);

    admin.modifyStream(
        tenantConfig.getName(),
        signalConf0.getName(),
        signalConf,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        signalConf0.getName(),
        signalConf,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    verifyAppendRollup(admin, tenantConfig, signalConf0, signalConf);

    // test reloading
    AdminInternal admin2 = new AdminImpl(adminStore, new MetricsStreamProvider());
    verifyAppendRollup(admin2, tenantConfig, signalConf0, signalConf);
  }

  private void verifyAppendRollup(
      AdminInternal admin,
      TenantConfig tenantConfig,
      StreamConfig signalConf0,
      StreamConfig signalConf)
      throws NoSuchStreamException, NoSuchTenantException {
    final StreamDesc signalDesc = admin.getStream(tenantConfig.getName(), signalConf.getName());
    assertNotNull(signalDesc);
    assertEquals("test_append_a_rollup", signalDesc.getName());
    assertEquals(signalConf.getVersion(), signalDesc.getVersion());
    assertEquals(signalConf0.getVersion(), signalDesc.getSchemaVersion());
    assertNull(signalDesc.getPrevName());
    assertNull(signalDesc.getPrevVersion());

    final StreamDesc viewDescOne =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".view.byOne");
    assertNotNull(viewDescOne);
    assertEquals(signalConf.getVersion(), viewDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), viewDescOne.getSchemaVersion());
    assertNull(viewDescOne.getPrevName());
    assertNull(viewDescOne.getPrevVersion());

    final StreamDesc indexDescOne =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".index.byOne");
    assertNotNull(indexDescOne);
    assertEquals(signalConf.getVersion(), indexDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), indexDescOne.getSchemaVersion());
    assertNull(indexDescOne.getPrevName());
    assertNull(indexDescOne.getPrevVersion());

    final StreamDesc rollupDescOne = admin.getStream(tenantConfig.getName(), "rollup_byOne");
    assertNotNull(rollupDescOne);
    assertEquals(signalConf.getVersion(), rollupDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), rollupDescOne.getSchemaVersion());
    assertNull(rollupDescOne.getPrevName());
    assertNull(rollupDescOne.getPrevVersion());

    final StreamDesc viewDescTwo =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".view.byTwo");
    assertNotNull(viewDescTwo);
    assertEquals(signalConf.getVersion(), viewDescTwo.getVersion());
    assertEquals(signalConf.getVersion(), viewDescTwo.getSchemaVersion());
    assertNull(viewDescTwo.getPrevName());
    assertNull(viewDescTwo.getPrevVersion());

    final StreamDesc indexDescTwo =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".index.byTwo");
    assertNotNull(indexDescTwo);
    assertEquals(signalConf.getVersion(), indexDescTwo.getVersion());
    assertEquals(signalConf.getVersion(), indexDescTwo.getSchemaVersion());
    assertNull(indexDescTwo.getPrevName());
    assertNull(indexDescTwo.getPrevVersion());

    final StreamDesc rollupDescTwo = admin.getStream(tenantConfig.getName(), "rollup_byTwo");
    assertNotNull(rollupDescTwo);
    assertEquals(signalConf.getVersion(), rollupDescTwo.getVersion());
    assertEquals(signalConf.getVersion(), rollupDescTwo.getSchemaVersion());
    assertNull(rollupDescTwo.getPrevName());
    assertNull(rollupDescTwo.getPrevVersion());
  }

  @Test
  public void modifyView() throws Exception {
    final String src = "{'name': 'modifyView'}";
    TenantConfig tenantConfig = AdminConfigCreator.makeTenantConfig(src, ++timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    final String initialSrc =
        "{"
            + "  'name': 'test_modify_view',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'valueA', 'type': 'long'},"
            + "    {'name': 'valueB', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view1',"
            + "    'groupBy': [],"
            + "    'attributes': []"
            + "  }, {"
            + "    'name': 'view2',"
            + "    'groupBy': [],"
            + "    'attributes': ['valueA']"
            + "  }, {"
            + "    'name': 'view3',"
            + "    'groupBy': [],"
            + "    'attributes': ['valueA']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view1',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view1',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view2',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view2',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view3',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view3',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, ++timestamp);

    final String modifiedSrc =
        "{"
            + "  'name': 'test_modify_view',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'valueA', 'type': 'long'},"
            + "    {'name': 'valueB', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view1',"
            + "    'groupBy': [],"
            + "    'attributes': []"
            + "  }, {"
            + "    'name': 'view2',"
            + "    'groupBy': [],"
            + "    'attributes': ['valueA']"
            + "  }, {"
            + "    'name': 'view3',"
            + "    'groupBy': [],"
            + "    'attributes': ['valueA', 'valueB']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view1',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view1',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view2',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view2',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view3',"
            + "    'rollups': [{"
            + "      'name': 'rollup_view3',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(modifiedSrc, ++timestamp);

    admin.addStream(tenantConfig.getName(), signalConf0, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), signalConf0, RequestPhase.FINAL);

    admin.modifyStream(
        tenantConfig.getName(),
        signalConf0.getName(),
        signalConf,
        RequestPhase.INITIAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin.modifyStream(
        tenantConfig.getName(),
        signalConf0.getName(),
        signalConf,
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    verifyModifyView(admin, tenantConfig, signalConf0, signalConf);

    // test reloading
    AdminInternal admin2 = new AdminImpl(adminStore, new MetricsStreamProvider());
    verifyModifyView(admin2, tenantConfig, signalConf0, signalConf);
  }

  private void verifyModifyView(
      AdminInternal admin,
      TenantConfig tenantConfig,
      StreamConfig signalConf0,
      StreamConfig signalConf)
      throws NoSuchStreamException, NoSuchTenantException {
    final StreamDesc signalDesc = admin.getStream(tenantConfig.getName(), signalConf.getName());
    assertNotNull(signalDesc);
    assertEquals("test_modify_view", signalDesc.getName());
    assertEquals(signalConf.getVersion(), signalDesc.getVersion());
    assertEquals(signalConf0.getVersion(), signalDesc.getSchemaVersion());
    assertNull(signalDesc.getPrevName());
    assertNull(signalDesc.getPrevVersion());

    final StreamDesc viewDescOne =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".view.view1");
    assertNotNull(viewDescOne);
    assertEquals(signalConf.getVersion(), viewDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), viewDescOne.getSchemaVersion());
    assertNull(viewDescOne.getPrevName());
    assertNull(viewDescOne.getPrevVersion());

    final StreamDesc indexDescOne =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".index.view1");
    assertNotNull(indexDescOne);
    assertEquals(signalConf.getVersion(), indexDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), indexDescOne.getSchemaVersion());
    assertNull(indexDescOne.getPrevName());
    assertNull(indexDescOne.getPrevVersion());

    final StreamDesc rollupDescOne = admin.getStream(tenantConfig.getName(), "rollup_view1");
    assertNotNull(rollupDescOne);
    assertEquals(signalConf.getVersion(), rollupDescOne.getVersion());
    assertEquals(signalConf0.getVersion(), rollupDescOne.getSchemaVersion());
    assertNull(rollupDescOne.getPrevName());
    assertNull(rollupDescOne.getPrevVersion());

    final StreamDesc viewDescTwo =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".view.view2");
    assertNotNull(viewDescTwo);
    assertEquals(signalConf.getVersion(), viewDescTwo.getVersion());
    assertEquals(signalConf0.getVersion(), viewDescTwo.getSchemaVersion());
    assertNull(viewDescTwo.getPrevName());
    assertNull(viewDescTwo.getPrevVersion());

    final StreamDesc indexDescTwo =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".index.view2");
    assertNotNull(indexDescTwo);
    assertEquals(signalConf.getVersion(), indexDescTwo.getVersion());
    assertEquals(signalConf0.getVersion(), indexDescTwo.getSchemaVersion());
    assertNull(indexDescTwo.getPrevName());
    assertNull(indexDescTwo.getPrevVersion());

    final StreamDesc rollupDescTwo = admin.getStream(tenantConfig.getName(), "rollup_view2");
    assertNotNull(rollupDescTwo);
    assertEquals(signalConf.getVersion(), rollupDescTwo.getVersion());
    assertEquals(signalConf0.getVersion(), rollupDescTwo.getSchemaVersion());
    assertNull(rollupDescTwo.getPrevName());
    assertNull(rollupDescTwo.getPrevVersion());

    final StreamDesc viewDescThree =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".view.view3");
    assertNotNull(viewDescThree);
    assertEquals(signalConf.getVersion(), viewDescThree.getVersion());
    assertEquals(signalConf.getVersion(), viewDescThree.getSchemaVersion());
    assertNull(viewDescThree.getPrevName());
    assertNull(viewDescThree.getPrevVersion());

    final StreamDesc indexDescThree =
        admin.getStream(tenantConfig.getName(), signalConf.getName() + ".index.view3");
    assertNotNull(indexDescThree);
    assertEquals(signalConf.getVersion(), indexDescThree.getVersion());
    assertEquals(signalConf.getVersion(), indexDescThree.getSchemaVersion());
    assertNull(indexDescThree.getPrevName());
    assertNull(indexDescThree.getPrevVersion());

    final StreamDesc rollupDescThree = admin.getStream(tenantConfig.getName(), "rollup_view3");
    assertNotNull(rollupDescThree);
    assertEquals(signalConf.getVersion(), rollupDescThree.getVersion());
    assertEquals(signalConf.getVersion(), rollupDescThree.getSchemaVersion());
    assertNull(rollupDescThree.getPrevName());
    assertNull(rollupDescThree.getPrevVersion());
  }

  @Test
  public void testModifySignalAttributeType() throws Throwable {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = mapper.readValue(src.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String contextName =
        AdminTestUtils.populateContext(
            biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v1.json");
    StreamDesc c = admin.getStream(tenantName, contextName);

    final String signalName =
        AdminTestUtils.populateSignal(
            biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v1.json");
    StreamDesc s = admin.getStream(tenantName, signalName);
    final long t2 = s.getVersion();
    assertEquals("visitId", s.getAttributes().get(0).getName());
    assertEquals(InternalAttributeType.LONG, s.getAttributes().get(0).getAttributeType());
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals("patientId", s.getAttributes().get(1).getName());
    assertEquals(InternalAttributeType.LONG, s.getAttributes().get(1).getAttributeType());
    assertEquals(2, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals("zipcode", s.getAttributes().get(2).getName());
    assertEquals(InternalAttributeType.LONG, s.getAttributes().get(2).getAttributeType());
    assertEquals(3, s.getAttributeProxyInfo().get("zipcode").getProxy());

    // Change patientId from Integer to Decimal and zipcode from Integer to String.
    AdminTestUtils.updateSignal(
        biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v2.json");
    s = admin.getStream(tenantName, signalName);
    final long t3 = s.getVersion();
    assertEquals("visitId", s.getAttributes().get(0).getName());
    assertEquals(InternalAttributeType.LONG, s.getAttributes().get(0).getAttributeType());
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals("patientId", s.getAttributes().get(1).getName());
    assertEquals(InternalAttributeType.DOUBLE, s.getAttributes().get(1).getAttributeType());
    assertEquals(8, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals("zipcode", s.getAttributes().get(2).getName());
    assertEquals(InternalAttributeType.STRING, s.getAttributes().get(2).getAttributeType());
    assertEquals(9, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals((Long) t3, s.getSchemaVersion());
  }

  @Ignore("BIOS-2338")
  @Test
  public void testModifyContextAttributeTypeCascadeToSignal() throws Throwable {
    final String src = "{'name': 'modifyStreamTest'}";
    TenantConfig tenantConfig = mapper.readValue(src.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    testTenants.add(tenantConfig);
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String contextName =
        AdminTestUtils.populateContext(
            biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v1.json");
    StreamDesc c = admin.getStream(tenantName, contextName);
    final long t1 = c.getVersion();
    System.out.printf(
        "stream=%s, version=%d, schemaVersion=%d%n",
        c.getName(), c.getVersion(), c.getSchemaVersion());
    assertEquals("attr1", c.getAttributes().get(1).getName());
    assertEquals(InternalAttributeType.LONG, c.getAttributes().get(1).getAttributeType());
    assertEquals(2, c.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals("attr2", c.getAttributes().get(2).getName());
    assertEquals(InternalAttributeType.DOUBLE, c.getAttributes().get(2).getAttributeType());
    assertEquals(3, c.getAttributeProxyInfo().get("attr2").getProxy());
    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManger, tenantName, contextName, Arrays.asList("a,1,5.05,hello"));

    final String signalName =
        AdminTestUtils.populateSignal(
            biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v1.json");
    StreamDesc signal = admin.getStream(tenantName, signalName);
    System.out.printf(
        "stream=%s, version=%d, schemaVersion=%d%n",
        signal.getName(), signal.getVersion(), signal.getSchemaVersion());
    final long t2 = signal.getVersion();
    assertEquals("attr1", signal.getAdditionalAttributes().get(0).getName());
    assertEquals(
        InternalAttributeType.LONG, signal.getAdditionalAttributes().get(0).getAttributeType());
    assertEquals(5, signal.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals("attr2", signal.getAdditionalAttributes().get(1).getName());
    assertEquals(
        InternalAttributeType.DOUBLE, signal.getAdditionalAttributes().get(1).getAttributeType());
    assertEquals(6, signal.getAttributeProxyInfo().get("attr2").getProxy());
    TestUtils.insert(dataServiceHandler, tenantName, signal.getName(), "0,0,0,a");
    ExtractRequest request = new ExtractRequest();
    request.setStartTime(t1);
    request.setEndTime(System.currentTimeMillis());
    List<Event> events = TestUtils.extract(dataServiceHandler, signal, request);
    System.out.println(events.toString());
    assertEquals(1, events.size());
    assertEquals(1L, events.get(0).get("attr1"));
    assertEquals(5.05, events.get(0).get("attr2"));

    // Change attr1 from Integer to Decimal, and attr2 from Decimal to Integer.
    AdminTestUtils.updateContext(
        biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v2.json");
    c = admin.getStream(tenantName, contextName);
    System.out.printf(
        "stream=%s, version=%d, schemaVersion=%d%n",
        c.getName(), c.getVersion(), c.getSchemaVersion());
    final long t4 = c.getVersion();
    assertEquals("attr1", c.getAttributes().get(1).getName());
    assertEquals(InternalAttributeType.DOUBLE, c.getAttributes().get(1).getAttributeType());
    assertEquals(5, c.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals("attr2", c.getAttributes().get(2).getName());
    assertEquals(InternalAttributeType.LONG, c.getAttributes().get(2).getAttributeType());
    assertEquals(6, c.getAttributeProxyInfo().get("attr2").getProxy());
    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManger, tenantName, contextName, Arrays.asList("b,6.06,2,world"));

    // The change in context attribute above should have a cascading effect on the signal.
    signal = admin.getStream(tenantName, signalName);
    System.out.printf(
        "stream=%s, version=%d, schemaVersion=%d%n",
        signal.getName(), signal.getVersion(), signal.getSchemaVersion());
    TestUtils.insert(dataServiceHandler, tenantName, signalName, "0,0,0,b");
    request = new ExtractRequest();
    request.setStartTime(t1);
    request.setEndTime(System.currentTimeMillis());
    events = TestUtils.extract(dataServiceHandler, signal, request);
    System.out.println(events.toString());
    assertEquals(2, events.size());
    assertEquals(1.0, events.get(0).get("attr1"));
    assertEquals(0L, events.get(0).get("attr2")); // The default value 0.
    assertEquals(6.06, events.get(1).get("attr1"));
    assertEquals(2L, events.get(1).get("attr2"));
    assertEquals("attr1", signal.getAdditionalAttributes().get(0).getName());
    assertEquals(
        InternalAttributeType.DOUBLE, signal.getAdditionalAttributes().get(0).getAttributeType());
    assertEquals(8, signal.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals("attr2", signal.getAdditionalAttributes().get(1).getName());
    assertEquals(
        InternalAttributeType.LONG, signal.getAdditionalAttributes().get(1).getAttributeType());
    assertEquals(9, signal.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals((Long) t4, signal.getSchemaVersion());
  }
}
