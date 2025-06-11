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
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SignalEnrichmentTest {

  private static String tenantName;

  private static CassandraConnection connection;
  private static AdminInternal admin;
  private static DataEngine dataEngine;
  private static HttpClientManager clientManager;
  private static DataServiceHandler dataServiceHandler;

  private static StreamConfig contextSupplierLocationsConfig;

  private static StreamConfig contextGeoLocationConfig;
  private static String enrichingContext1Name;
  private static String enrichingContext2Name;
  private static String enrichedContextName;

  private StreamConfig signalConfig;
  private StreamConfig signal2Config;

  @BeforeClass
  public static void setUpClass() throws Throwable {
    tenantName = "joins_test";

    Bios2TestModules.startModules(false, SignalEnrichmentTest.class, Map.of());
    connection = BiosModules.getCassandraConnection();
    admin = BiosModules.getAdminInternal();
    dataEngine = BiosModules.getDataEngine();
    clientManager = BiosModules.getHttpClientManager();
    dataServiceHandler = BiosModules.getDataServiceHandler();

    final TenantConfig tenant = new TenantConfig(tenantName);
    Long timestamp = System.currentTimeMillis();
    try {
      admin.removeTenant(tenant, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenant, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // This is ok
    }

    final String contextSrc =
        "{"
            + "  'name': 'city_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'city_id', 'type': 'long'},"
            + "    {'name': 'city_name', 'type': 'string'},"
            + "    {'name': 'supplier_city_id', 'type': 'long'},"
            + "    {'name': 'warehouse_city_id', 'type': 'long'}"
            + "  ]"
            + "}";
    contextSupplierLocationsConfig = AdminConfigCreator.makeStreamConfig(contextSrc);
    tenant.addStream(contextSupplierLocationsConfig);

    contextGeoLocationConfig =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC);
    tenant.addStream(contextGeoLocationConfig);

    timestamp = System.currentTimeMillis();
    admin.addTenant(tenant, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant, RequestPhase.FINAL, timestamp);

    // populate context entries
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        contextSupplierLocationsConfig.getName(),
        List.of("11,Redwood City,10,20", "10,San Mateo,10,20", "20,San Jose,20,20"));

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        contextGeoLocationConfig.getName(),
        List.of("10.20.30.40,USA,Oregon", "10.20.30.50,Japan,Tokyo"));

    // Set up contexts needed for context enrichment tests.
    enrichingContext1Name =
        AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(1));
    enrichingContext2Name =
        AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(2));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(3));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(4));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(5));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(6));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(7));

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        List.of("1,staff,1000", "2,doctor,2000"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext2Name,
        List.of("resource1,1.1,staffAlias1"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext2Name,
        List.of("resource2,2.1,doctorAlias1"));
    enrichedContextName =
        AdminTestUtils.populateStream(
            admin, tenantName, "../server/bios-server/src/test/resources/EnrichedContextTfos.json");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        List.of("resource1,1,11111", "resource2,2,22222"));
  }

  private static String supportingContextFile(int contextNum) {
    return "../server/bios-server/src/test/resources/ContextToEnrichContext"
        + String.format("%d", contextNum)
        + "Tfos.json";
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    final TenantConfig tenantConf = new TenantConfig(tenantName);
    try {
      Long timestamp = System.currentTimeMillis();
      admin.removeTenant(tenantConf, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConf, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // This is ok
    }
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {

    final String signalSrc =
        "{"
            + "  'name': '**change_name** order_signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'order_id', 'type': 'long'},"
            + "    {'name': 'city_id', 'type': 'long'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'city_preprocess_1',"
            + "    'condition': 'city_id',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'city_name',"
            + "      'as': 'city_name'"
            + "    }, {"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'supplier_city_id',"
            + "      'as': 'supplier_city_id'"
            + "    }]"
            + "  }, {"
            + "    'name': 'city_preprocess_2',"
            + "    'condition': 'supplier_city_id',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'city_name',"
            + "      'as': 'supplier_city_name'"
            + "    }]"
            + "  }]"
            + "}";

    signalConfig = AdminConfigCreator.makeStreamConfig(signalSrc, System.currentTimeMillis());
    signal2Config =
        AdminTestUtils.getStreamConfig(
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos.json");
  }

  @Test
  public void testContextJoinChaining() throws Throwable {
    signalConfig.setName("contextJoinChaining");

    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, signalConfig.getName());
    // System.out.println(registeredSignal);

    final var resp =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal.getName(), "1,11");

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(request.getStartTime() + 1);

    final List<Event> events = TestUtils.extract(dataServiceHandler, registeredSignal, request);
    // System.out.println(events);
    assertEquals(1, events.size());
    final Event event0 = events.get(0);
    assertEquals("San Mateo", event0.get("supplier_city_name"));
  }

  /**
   * Reverse the order of the first-stage actions.
   *
   * <p>The verification should pass.
   */
  @Test
  public void testContextJoinChaining2() throws Throwable {
    signalConfig.setName("contextJoinChaining2");
    final PreprocessDesc preprocess0 = signalConfig.getPreprocesses().get(0);
    final List<ActionDesc> actions = preprocess0.getActions();
    preprocess0.setActions(Arrays.asList(actions.get(1), actions.get(0)));

    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, signalConfig.getName());
    final var resp =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal.getName(), "1,11");

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(request.getStartTime() + 1);

    final List<Event> events = TestUtils.extract(dataServiceHandler, registeredSignal, request);
    assertEquals(1, events.size());
    final Event event0 = events.get(0);
    assertEquals("San Mateo", event0.get("supplier_city_name"));
  }

  /**
   * Reverse the order of the preprocess stages.
   *
   * <pre>
   *   original attributes = [order_id, city_id]
   *   1st : join supplier_city_name = city_context.get(supplier_city_id) -- unresolvable
   *   2nd : join supplier_city_id = city_context.get(city_id)
   * </pre>
   */
  @Test(expected = ConstraintViolationException.class)
  public void testContextJoinInvalidChaining() throws Exception {
    signalConfig.setName("contextJoinInvalidChaining");
    final List<PreprocessDesc> preprocesses = signalConfig.getPreprocesses();
    signalConfig.setPreprocesses(Arrays.asList(preprocesses.get(1), preprocesses.get(0)));
    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
  }

  @Test
  public void testContextWideJoin() throws Throwable {
    signalConfig.setName("contextWideJoin");

    signalConfig.addAttribute(new AttributeDesc("ip", InternalAttributeType.STRING));

    PreprocessDesc preprocess = new PreprocessDesc();
    preprocess.setName("join_datacenter_state");
    preprocess.setCondition("ip");
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("state");
    action.setAs("data_center_state");
    action.setContext("geo_location");
    preprocess.addAction(action);
    signalConfig.addPreprocess(preprocess);
    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);

    StreamDesc registeredSignal = admin.getStream(tenantName, signalConfig.getName());
    final var resp =
        TestUtils.insert(
            dataServiceHandler, tenantName, registeredSignal.getName(), "1,11,10.20.30.40");

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(request.getStartTime() + 1);

    final List<Event> events = TestUtils.extract(dataServiceHandler, registeredSignal, request);
    assertEquals(1, events.size());
    final Event event0 = events.get(0);
    assertEquals("San Mateo", event0.get("supplier_city_name"));
    assertEquals("Oregon", event0.get("data_center_state"));
  }

  /** Deep context join chaining is not allowed. */
  @Test(expected = ConstraintViolationException.class)
  public void testContextJoinDeepChain() throws Exception {
    final String signalWithChainDepth2Src =
        "{"
            + "  'name': 'contextJoinDeepChain',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'order_id', 'type': 'int'},"
            + "    {'name': 'city_id', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'join_supplier_city_by_city_id',"
            + "    'condition': 'city_id',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'city_name',"
            + "      'as': 'city_name'"
            + "    }, {"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'supplier_city_id',"
            + "      'as': 'supplier_city_id'"
            + "    }]"
            + "  }, {"
            + "    'name': 'join_supplier_city_by_supplier_city_id',"
            + "    'condition': 'supplier_city_id',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'city_name',"
            + "      'as': 'supplier_city_name'"
            + "    }, {"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'warehouse_city_id',"
            + "      'as': 'warehouse_city_id'"
            + "    }]"
            + "  }, {"
            + "    'name': 'join_warehouse_city_by_warehouse_city_id',"
            + "    'condition': 'warehouse_city_id',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': [{"
            + "      'actionType': 'MERGE',"
            + "      'context': 'city_context',"
            + "      'attribute': 'city_name',"
            + "      'as': 'warehouse_city_name'"
            + "    }]"
            + "  }]"
            + "}";

    StreamConfig signalWithChainDepth2 =
        AdminConfigCreator.makeStreamConfig(signalWithChainDepth2Src, System.currentTimeMillis());
    admin.addStream(tenantName, signalWithChainDepth2, RequestPhase.INITIAL);
  }

  @Test
  public void testContextEnrichmentBasic() throws Throwable {
    AdminTestUtils.deleteStreamIgnorePresence(admin, tenantName, signal2Config.getName());
    StreamDesc registeredSignal = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var signalName = registeredSignal.getName();
    final var resp1 =
        TestUtils.insert(dataServiceHandler, tenantName, signalName, "111,11,resource1");
    final var resp2 =
        TestUtils.insert(dataServiceHandler, tenantName, signalName, "222,22,resource2");

    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(resp1.getTimeStamp());
      request.setEndTime(resp2.getTimeStamp() + 1);

      final List<Event> events = TestUtils.extract(dataServiceHandler, registeredSignal, request);
      assertEquals(2, events.size());
      assertEquals("staff", events.get(0).get("resourceTypeCode"));
      assertEquals(1000L, events.get(0).get("myResourceCategory"));
      assertEquals("11111", events.get(0).get("zipcode"));
      assertEquals("doctor", events.get(1).get("resourceTypeCode"));
      assertEquals(2000L, events.get(1).get("myResourceCategory"));
    }

    // reload and retry
    {
      final ExtractRequest request = new ExtractRequest();
      request.setStartTime(resp1.getTimeStamp());
      request.setEndTime(resp2.getTimeStamp() + 1);

      final var adminStore2 = new AdminStoreImpl(connection);
      final var dataEngine2 = new DataEngineImpl(connection);
      final var tfosAdmin2 = new AdminImpl(adminStore2, null, dataEngine2);
      final var signal2 = tfosAdmin2.getStream(tenantName, signal2Config.getName());
      final var dataServiceHandler2 =
          new DataServiceHandler(
              dataEngine2, tfosAdmin2, BiosModules.getMetrics(), BiosModules.getSharedConfig());
      final List<Event> events = TestUtils.extract(dataServiceHandler2, signal2, request);
      assertEquals(2, events.size());
      assertEquals("staff", events.get(0).get("resourceTypeCode"));
      assertEquals(1000L, events.get(0).get("myResourceCategory"));
      assertEquals("11111", events.get(0).get("zipcode"));
      assertEquals("doctor", events.get(1).get("resourceTypeCode"));
      assertEquals(2000L, events.get(1).get("myResourceCategory"));
    }

    AdminTestUtils.deleteStream(admin, tenantName, signal2Config.getName());
  }

  @Test
  public void testContextEnrichmentMissingLookup1() throws Throwable {
    AdminTestUtils.deleteStreamIgnorePresence(admin, tenantName, signal2Config.getName());
    StreamDesc registeredSignal = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var exception =
        assertThrows(
            TfosException.class,
            () ->
                TestUtils.insert(
                    dataServiceHandler,
                    tenantName,
                    registeredSignal.getName(),
                    "555,55,resource5"));
  }

  private void verifyEnrichedContextEntries(List<Event> entries) {
    assertEquals("11111", entries.get(0).get("zipcode"));
    assertEquals(1000L, entries.get(0).get("myResourceCategory"));
    assertEquals(2000L, entries.get(1).get("myResourceCategory"));
  }

  @Test
  public void testContextEnrichmentMissingLookupInEnrichingContext() throws Throwable {
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource4,4,44444"));
    final var entries =
        TestUtils.getContextEntriesSingleKeys(
            dataServiceHandler,
            tenantName,
            enrichedContextName,
            List.of("resource1", "resource2", "resource4"));
    assertEquals(2, entries.size());
    verifyEnrichedContextEntries(entries);
  }

  @Test
  public void testSecondLevelContextEnrichment() throws Throwable {
    String secondLevelContext1Name =
        AdminTestUtils.populateStream(
            admin,
            tenantName,
            "contextEnrichment",
            "../server/bios-server/src/test/resources/SecondLevelEnrichedContextTfos.json");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource4,4,44444"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        secondLevelContext1Name,
        Arrays.asList("x,resource1", "y,resource2", "z,resource4", "u,nonexistentResource"));
    final var entries =
        TestUtils.getContextEntriesSingleKeys(
            dataServiceHandler, tenantName, secondLevelContext1Name, List.of("x", "y", "z", "u"));
    assertEquals(2, entries.size());
    verifyEnrichedContextEntries(entries);
    assertEquals("x", entries.get(0).get("secondId"));
    assertEquals(1L, entries.get(0).get("resourceTypeNum"));
    assertEquals(2L, entries.get(1).get("resourceTypeNum"));
    assertEquals(1000L, entries.get(0).get("resourceCategory"));
    assertEquals(2000L, entries.get(1).get("resourceCategory"));
    AdminTestUtils.deleteStream(admin, tenantName, secondLevelContext1Name);
  }

  @Test
  public void testContextEnrichmentWithModifiedEntries1() throws Throwable {
    AdminTestUtils.deleteStreamIgnorePresence(admin, tenantName, signal2Config.getName());
    StreamDesc registered = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var registeredSignal = registered.getName();
    var resp =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "111,11,resource1");
    ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(resp.getTimeStamp() + 1);
    List<Event> events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(1, events.size());
    assertEquals("staff", events.get(0).get("resourceTypeCode"));
    assertEquals(1000L, events.get(0).get("myResourceCategory"));
    assertEquals("11111", events.get(0).get("zipcode"));

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource1,2,10002"));
    resp = TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "555,55,resource1");
    request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(resp.getTimeStamp() + 1);
    events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(1, events.size());
    assertEquals("doctor", events.get(0).get("resourceTypeCode"));
    assertEquals(2000L, events.get(0).get("myResourceCategory"));
    assertEquals("10002", events.get(0).get("zipcode"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource1,1,11111"));

    AdminTestUtils.deleteStream(admin, tenantName, signal2Config.getName());
  }

  @Test
  public void testContextEnrichmentWithModifiedEntries2() throws Throwable {
    AdminTestUtils.deleteStreamIgnorePresence(admin, tenantName, signal2Config.getName());
    StreamDesc registered = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var registeredSignal = registered.getName();
    var resp =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "111,11,resource1");
    ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(resp.getTimeStamp() + 1);
    List<Event> events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(1, events.size());
    assertEquals("staff", events.get(0).get("resourceTypeCode"));
    assertEquals(1000L, events.get(0).get("myResourceCategory"));
    assertEquals("11111", events.get(0).get("zipcode"));

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        Arrays.asList("1,staff-new,1002"));
    resp = TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "555,55,resource1");
    request = new ExtractRequest();
    request.setStartTime(resp.getTimeStamp());
    request.setEndTime(resp.getTimeStamp() + 1);
    events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(1, events.size());
    assertEquals("staff-new", events.get(0).get("resourceTypeCode"));
    assertEquals(1002L, events.get(0).get("myResourceCategory"));
    assertEquals("11111", events.get(0).get("zipcode"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        Arrays.asList("1,staff,1000"));

    AdminTestUtils.deleteStream(admin, tenantName, signal2Config.getName());
  }

  @Test
  public void testSecondLevelContextEnrichmentWithModifiedEntries() throws Throwable {
    final var signal3Config =
        AdminTestUtils.getStreamConfig(
            "../server/bios-server/src/test/resources/SignalForSecondContextEnrichmentTfos.json");
    AdminTestUtils.deleteStreamIgnorePresence(admin, tenantName, signal3Config.getName());

    String secondLevelContext1Name =
        AdminTestUtils.populateStream(
            admin,
            tenantName,
            "../server/bios-server/src/test/resources/SecondLevelEnrichedContextTfos.json");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource4,4,44444"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        secondLevelContext1Name,
        Arrays.asList("x,resource1", "y,resource2", "z,resource4", "u,nonexistentResource"));

    final StreamDesc registered = AdminTestUtils.populateStream(admin, tenantName, signal3Config);
    final var registeredSignal = registered.getName();
    var resp1 = TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "6,x");
    var resp2 = TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "7,y");

    ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp1.getTimeStamp());
    request.setEndTime(resp2.getTimeStamp() + 1);
    List<Event> events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(2, events.size());
    assertEquals("11111", events.get(0).get("zipcode"));
    assertEquals("22222", events.get(1).get("zipcode"));
    assertEquals("staff", events.get(0).get("resourceTypeCode"));
    assertEquals("doctor", events.get(1).get("resourceTypeCode"));
    assertEquals(1000L, events.get(0).get("myResourceCategory"));
    assertEquals(2000L, events.get(1).get("myResourceCategory"));
    assertEquals(1000L, events.get(0).get("myResourceCategory2"));
    assertEquals(2000L, events.get(1).get("myResourceCategory2"));

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        Arrays.asList("1,staff-new3,1003"));
    resp1 = TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "66,x");
    request = new ExtractRequest();
    request.setStartTime(resp1.getTimeStamp());
    request.setEndTime(resp1.getTimeStamp() + 1);
    events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(1, events.size());
    assertEquals("staff-new3", events.get(0).get("resourceTypeCode"));
    assertEquals("staff-new3", events.get(0).get("resourceTypeCode2"));
    assertEquals(1003L, events.get(0).get("myResourceCategory"));
    assertEquals(1003L, events.get(0).get("myResourceCategory2"));
    assertEquals("11111", events.get(0).get("zipcode"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        Arrays.asList("1,staff,1000"));

    final var exception =
        assertThrows(
            TfosException.class,
            () ->
                TestUtils.insert(
                    dataServiceHandler, tenantName, registeredSignal, "9,nonexistent-v"));

    AdminTestUtils.deleteStream(admin, tenantName, signal3Config.getName());
    AdminTestUtils.deleteStream(admin, tenantName, secondLevelContext1Name);
  }

  private void putContextEntriesForValuePickFirst() throws Throwable {
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        "staff",
        Arrays.asList("resource1,1.1,staffAlias1"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        "doctor",
        Arrays.asList("resource2,2.1,doctorAlias1"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        "venue",
        Arrays.asList("resource3,3.1,venueAlias1"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        "workspace",
        Arrays.asList("resource6,6.1,workspaceId1"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichingContext1Name,
        Arrays.asList("3,venue,3000", "6,workspace,6000"));
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        enrichedContextName,
        Arrays.asList("resource3,3,33333", "resource6,6,66666"));
  }

  @Test
  public void testValuePickFirstBasic() throws Throwable {
    putContextEntriesForValuePickFirst();
    String signalName = "valuePickFirstSignal1";
    signal2Config.setName(signalName);
    final StreamDesc registered = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var registeredSignal = registered.getName();
    final var resp1 =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "111,11,resource1");
    TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "222,22,resource2");
    TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "333,33,resource3");
    final var resp2 =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "666,66,resource6");

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp1.getTimeStamp());
    request.setEndTime(resp2.getTimeStamp() + 1);

    final List<Event> events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(4, events.size());
    assertEquals("staff", events.get(0).get("resourceTypeCode"));
    assertEquals(1000L, events.get(0).get("myResourceCategory"));
    assertEquals("11111", events.get(0).get("zipcode"));
    assertEquals(1.1, events.get(0).get("cost"));
    assertEquals("staffAlias1", events.get(0).get("resourceValue"));
    assertEquals("doctor", events.get(1).get("resourceTypeCode"));
    assertEquals(2000L, events.get(1).get("myResourceCategory"));
    assertEquals(2.1, events.get(1).get("cost"));
    assertEquals("doctorAlias1", events.get(1).get("resourceValue"));
    assertEquals(3.1, events.get(2).get("cost"));
    assertEquals("venueAlias1", events.get(2).get("resourceValue"));
    assertEquals(6.1, events.get(3).get("cost"));
    assertEquals("workspaceId1", events.get(3).get("resourceValue"));

    AdminTestUtils.deleteStream(admin, tenantName, signal2Config.getName());
  }

  @Ignore("Enable after fixing BIOS-1901")
  @Test
  public void testValuePickFirstModify() throws Throwable {
    putContextEntriesForValuePickFirst();
    String signalName = "valuePickFirstSignal2";
    signal2Config.setName(signalName);
    final StreamDesc registered = AdminTestUtils.populateStream(admin, tenantName, signal2Config);
    final var registeredSignal = registered.getName();
    final var resp1 =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "111,11,resource1");
    TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "222,22,resource2");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        tenantName,
        "doctor",
        Arrays.asList("resource2,2.12,doctorAlias12"));
    final var resp2 =
        TestUtils.insert(dataServiceHandler, tenantName, registeredSignal, "223,23,resource2");

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(resp1.getTimeStamp());
    request.setEndTime(resp2.getTimeStamp() + 1);

    final List<Event> events = TestUtils.extract(dataServiceHandler, registered, request);
    assertEquals(3, events.size());
    assertEquals("staff", events.get(0).get("resourceTypeCode"));
    assertEquals(1000L, events.get(0).get("myResourceCategory"));
    assertEquals("11111", events.get(0).get("zipcode"));
    assertEquals(1.1, events.get(0).get("cost"));
    assertEquals("staffAlias1", events.get(0).get("resourceValue"));
    assertEquals("doctor", events.get(1).get("resourceTypeCode"));
    assertEquals(2000L, events.get(1).get("myResourceCategory"));
    assertEquals(2.1, events.get(1).get("cost"));
    assertEquals("doctorAlias1", events.get(1).get("resourceValue"));
    assertEquals("doctor", events.get(2).get("resourceTypeCode"));
    assertEquals(2000L, events.get(2).get("myResourceCategory"));
    assertEquals(2.12, events.get(2).get("cost"));
    assertEquals("doctorAlias12", events.get(2).get("resourceValue"));

    AdminTestUtils.deleteStream(admin, tenantName, signal2Config.getName());
  }
}
