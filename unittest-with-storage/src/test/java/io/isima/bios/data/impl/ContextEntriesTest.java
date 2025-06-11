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
package io.isima.bios.data.impl;

import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextMetricsCounter;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextEntriesTest {

  private static long timestamp;
  private static AdminInternal tfosAdmin;
  private static io.isima.bios.admin.Admin admin;
  private static DataEngineImpl dataEngine;
  private static HttpClientManager clientManager;
  private static DataServiceHandler dataServiceHandler;
  private static CassandraConnection conn;

  private String previousCacheSize;

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantDesc> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(false, ContextEntriesTest.class, Map.of());
    tfosAdmin = BiosModules.getAdminInternal();
    admin = BiosModules.getAdmin();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    clientManager = BiosModules.getHttpClientManager();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    timestamp = System.currentTimeMillis();
    conn = BiosModules.getCassandraConnection();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    previousCacheSize = System.getProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE);
    testTenants.clear();
  }

  @After
  public void tearDown() throws Exception {
    for (TenantDesc tenantDesc : testTenants) {
      final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
      try {
        tfosAdmin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantDesc.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
      try {
        tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantDesc.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
    }
    TestUtils.revertProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE, previousCacheSize);
  }

  @Test
  public void testBasic()
      throws ApplicationException,
          TfosException,
          FileReadException,
          ExecutionException,
          InterruptedException {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    // build the tenant
    String tenantName = "contextEntriesTestTenant";

    TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final String keyName = "location";
    final String valueName = "zipcode";
    StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc(valueName, InternalAttributeType.INT));
    tenantDesc.addStream(context);

    String signalName = "Signal";
    StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute(valueName);
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // Add the tenant to AdminInternal -- initial execution
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    context = tfosAdmin.getStream(tenantName, contextName);
    assertNotNull(context);

    System.out.println(LocalDateTime.now().toString() + ": ------ Initial puts");
    List<Event> entries = new ArrayList<>();
    Event entry1 = new EventJson();
    entry1.getAttributes().put(keyName, "Redwood City");
    entry1.getAttributes().put(valueName, 94061);
    entries.add(entry1);

    Event entry2 = new EventJson();
    entry2.getAttributes().put(keyName, "Fremont");
    entry2.getAttributes().put(valueName, 94555);
    entries.add(entry2);

    Event entry3 = new EventJson();
    entry3.getAttributes().put(keyName, "San Mateo");
    entry3.getAttributes().put(valueName, 94401);
    entries.add(entry3);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries);

    assertEquals(0, metrics.getLookupCount());
    assertEquals(0, metrics.getCacheHitCount());

    // test getting context entries
    System.out.println(LocalDateTime.now().toString() + ": ------ Initial gets");
    List<List<Object>> keys = new ArrayList<>();
    keys.add(List.of("Redwood City"));
    keys.add(List.of("Fremont"));
    keys.add(List.of("San Mateo"));
    List<Event> retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

    assertNotNull(retrieved);
    assertEquals(entries.size(), retrieved.size());
    for (int i = 0; i < entries.size(); ++i) {
      assertEquals(
          Integer.toString(i),
          entries.get(i).getAttributes().get(keyName),
          retrieved.get(i).getAttributes().get(keyName));
      assertEquals(
          Integer.toString(i),
          entries.get(i).getAttributes().get(valueName),
          retrieved.get(i).getAttributes().get(valueName));
    }

    assertEquals(3, metrics.getLookupCount());
    assertEquals(3, metrics.getCacheHitCount());

    // test listing
    System.out.println(LocalDateTime.now().toString() + ": ------ List keys");
    List<List<Object>> listed = listContextReferenceKeys(dataEngine, context);
    assertNotNull(listed);
    assertEquals(3, listed.size());
    assertTrue(listed.contains(List.of("Redwood City")));
    assertTrue(listed.contains(List.of("Fremont")));
    assertTrue(listed.contains(List.of("San Mateo")));

    // test updating
    System.out.println(LocalDateTime.now().toString() + ": ------ Update");
    List<Event> entries2 = new ArrayList<>();
    Event entry1a = new EventJson();
    entry1a.getAttributes().put(keyName, "Redwood City");
    entry1a.getAttributes().put(valueName, 94063);
    entries2.add(entry1a);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries2);

    System.out.println(LocalDateTime.now().toString() + ": ------ Get after update");
    List<List<Object>> keys2 = new ArrayList<>();
    keys2.add(List.of("Redwood City"));
    List<Event> updated =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys2);

    assertNotNull(updated);
    assertEquals(1, updated.size());
    assertEquals(94063, updated.get(0).getAttributes().get(valueName));

    assertEquals(4, metrics.getLookupCount());
    assertEquals(4, metrics.getCacheHitCount());

    // test reloading
    System.out.println(LocalDateTime.now().toString() + ": ------ Reload");
    final var modules2 = TestModules.reload(ContextEntriesTest.class + " rl", conn);
    AdminInternal admin2 = modules2.getTfosAdmin();
    context = admin2.getStream(tenantName, contextName);

    assertNotNull(context);

    assertEquals(4, metrics.getLookupCount());
    assertEquals(4, metrics.getCacheHitCount());

    System.out.println(LocalDateTime.now() + ": ------ Get after reload");
    final List<Event> loaded =
        TestUtils.getContextEntries(
            modules2.getDataServiceHandler(), tenantName, contextName, keys);
    assertNotNull(loaded);

    List<Event> expectedEntries = new ArrayList<>();
    expectedEntries.add(entry1a);
    expectedEntries.add(entry2);
    expectedEntries.add(entry3);

    assertEquals(expectedEntries.size(), loaded.size());
    for (int i = 0; i < expectedEntries.size(); ++i) {
      assertEquals(
          Integer.toString(i),
          expectedEntries.get(i).getAttributes().get(keyName),
          loaded.get(i).getAttributes().get(keyName));
      assertEquals(
          Integer.toString(i),
          expectedEntries.get(i).getAttributes().get(valueName),
          loaded.get(i).getAttributes().get(valueName));
    }

    // test deleting
    System.out.println(LocalDateTime.now().toString() + ": ------ Delete");
    List<List<Object>> keys3 = new ArrayList<>();
    keys3.add(List.of("Redwood City"));
    keys3.add(List.of("Fremont"));

    TestUtils.deleteContextEntries(
        dataServiceHandler, clientManager, tenantName, contextName, keys3);

    System.out.println(LocalDateTime.now() + ": ------ Get after delete");
    List<List<Object>> remainingKeys = listContextReferenceKeys(dataEngine, context);
    assertNotNull(remainingKeys);
    assertEquals(1, remainingKeys.size());
    assertEquals(List.of("San Mateo"), remainingKeys.get(0));

    List<Event> remainingEntries =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);
    assertEquals(1, remainingEntries.size());
    assertEquals("San Mateo", remainingEntries.get(0).get("location"));

    // reload
    System.out.println(LocalDateTime.now().toString() + ": ------ Reload again");
    final var modules3 = TestModules.reload(ContextEntriesTest.class + " r2", conn);
    DataEngineImpl ingest3 = modules3.getDataEngine();
    AdminInternal admin3 = modules3.getTfosAdmin();
    context = admin3.getStream(tenantName, contextName);

    assertNotNull(context);

    System.out.println(LocalDateTime.now().toString() + ": ------ List after second reload");
    remainingKeys = listContextReferenceKeys(ingest3, context);
    assertNotNull(remainingKeys);
    assertEquals(1, remainingKeys.size());
    assertEquals(List.of("San Mateo"), remainingKeys.get(0));

    System.out.println(LocalDateTime.now().toString() + ": ------ Get after second reload");
    remainingEntries =
        TestUtils.getContextEntries(
            modules3.getDataServiceHandler(), tenantName, contextName, keys);
    assertEquals(1, remainingEntries.size());
    assertEquals("San Mateo", remainingEntries.get(0).get("location"));
  }

  @Test
  public void testCacheOverwriting()
      throws ApplicationException, TfosException, ExecutionException, InterruptedException {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    // build the tenant
    String tenantName = "contextEntriesTestCacheOverwriting";

    try {
      admin.deleteTenant(tenantName, RequestPhase.INITIAL, ++timestamp);
      admin.deleteTenant(tenantName, RequestPhase.FINAL, timestamp);
    } catch (TfosException e) {
      // ok
    }
    final var tenantConfig = new io.isima.bios.models.TenantConfig(tenantName);
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);

    final String contextName = "Context";
    final String primaryKeyName = "location";
    final String zipCode = "zipcode";
    final String county = "county";
    final var contextConfig = new ContextConfig(contextName, timestamp);
    contextConfig.setAttributes(
        List.of(
            new AttributeConfig(primaryKeyName, io.isima.bios.models.AttributeType.STRING),
            new AttributeConfig(zipCode, io.isima.bios.models.AttributeType.INTEGER),
            new AttributeConfig(county, io.isima.bios.models.AttributeType.STRING)));
    contextConfig.setPrimaryKey(List.of(primaryKeyName));
    contextConfig.setMissingAttributePolicy(io.isima.bios.models.MissingAttributePolicy.REJECT);
    admin.createContext(tenantName, contextConfig, RequestPhase.INITIAL, ++timestamp);
    admin.createContext(tenantName, contextConfig, RequestPhase.FINAL, timestamp);

    StreamDesc context = tfosAdmin.getStream(tenantName, contextName);
    // assertNotNull(context);

    Event entry10 = new EventJson();
    entry10.set(primaryKeyName, "Redwood City");
    entry10.set(zipCode, 94061L);
    entry10.set(county, "Santa Clara");
    Event entry11 = new EventJson();
    entry11.set(primaryKeyName, "Point Reyes Station");
    entry11.set(zipCode, 94950L);
    entry11.set(county, "Marin");
    List<Event> initialEntries = List.of(entry10, entry11);

    {
      final var state =
          TestUtils.makeUpsertState(
              tfosAdmin, tenantName, contextName, initialEntries, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
    }

    assertEquals(0, metrics.getLookupCount());
    assertEquals(0, metrics.getCacheHitCount());

    // overwrite the context entry with the same value
    Event entry20 = new EventJson();
    entry20.set(primaryKeyName, "Redwood City");
    entry20.set(zipCode, 94061L);
    entry20.set(county, "Santa Clara");
    Event entry21 = new EventJson();
    entry21.set(primaryKeyName, "Point Reyes Station");
    entry21.set(zipCode, 94956L);
    entry21.set(county, "Marin");
    List<Event> secondEntries = List.of(entry20, entry21);

    {
      final var state =
          TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, secondEntries, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
    }

    // test getting context entries
    List<List<Object>> keys = List.of(List.of("Redwood City"), List.of("Point Reyes Station"));
    List<Event> retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

    assertNotNull(retrieved);
    assertEquals(initialEntries.size(), retrieved.size());
    for (int i = 0; i < initialEntries.size(); ++i) {
      assertEquals(secondEntries.get(i).get(primaryKeyName), retrieved.get(i).get(primaryKeyName));
      assertEquals(secondEntries.get(i).get(zipCode), retrieved.get(i).get(zipCode));
      assertEquals(secondEntries.get(i).get(county), retrieved.get(i).get(county));
    }

    // test reloading
    final var modules2 = TestModules.reload(ContextEntriesTest.class + " r3", conn);
    AdminInternal admin2 = modules2.getTfosAdmin();
    context = admin2.getStream(tenantName, contextName);

    assertNotNull(context);

    List<Event> loaded =
        TestUtils.getContextEntries(
            modules2.getDataServiceHandler(),
            tenantName,
            contextName,
            List.of(List.of("Redwood City"), List.of("Point Reyes Station")));
    assertNotNull(loaded);
    assertEquals(2, loaded.size());
    assertEquals(secondEntries.get(0).getAttributes(), loaded.get(0).getAttributes());
    assertEquals(secondEntries.get(1).getAttributes(), loaded.get(1).getAttributes());

    // overwrite by different values
    final Event entry30 = new EventJson();
    entry30.set(primaryKeyName, "Redwood City");
    entry30.set(zipCode, 94061L);
    entry30.set(county, "San Mateo");
    Event entry31 = new EventJson();
    entry31.set(primaryKeyName, "Point Reyes Station");
    entry31.set(zipCode, 94951L);
    entry31.set(county, "Marin");
    List<Event> thirdEntries = List.of(entry30, entry31);
    {
      final var state =
          TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, thirdEntries, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
    }

    final var retrieved2 =
        TestUtils.getContextEntries(
            dataServiceHandler,
            tenantName,
            contextName,
            List.of(List.of("Redwood City"), List.of("Point Reyes Station")));
    assertNotNull(retrieved2);
    assertEquals(2, retrieved2.size());
    assertEquals(entry30.getAttributes(), retrieved2.get(0).getAttributes());
    assertEquals(entry31.getAttributes(), retrieved2.get(1).getAttributes());

    // reload
    final var modules3 = TestModules.reload(ContextEntriesTest.class + " r4", conn);
    AdminInternal admin3 = modules3.getTfosAdmin();
    context = admin3.getStream(tenantName, contextName);

    assertNotNull(context);

    final var retrieved3 =
        TestUtils.getContextEntries(
            modules3.getDataServiceHandler(),
            tenantName,
            contextName,
            List.of(List.of("Redwood City")));
    assertNotNull(retrieved3);
    assertEquals(1, retrieved3.size());
    assertEquals(entry30.getAttributes(), retrieved3.get(0).getAttributes());
  }

  @Test
  public void testWebService() throws Throwable {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.reset();

    // build the tenant
    String tenantName = "testPutContext";

    TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final String keyName = "ip";
    final String valueName = "location";
    StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc(valueName, InternalAttributeType.STRING));
    tenantDesc.addStream(context);

    String signalName = "Signal";
    StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("ip", InternalAttributeType.STRING));
    PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("ip");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute(valueName);
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // Add the tenant to AdminInternal -- initial execution
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // put entries via AdminServiceHandler
    List<String> entries = new ArrayList<>();
    entries.add("203.22.43.152,Redwood City");
    entries.add("203.22.43.153,Fremont");
    entries.add("203.22.43.154,San Mateo");
    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tenantName, contextName, entries);

    List<List<Object>> keys = new ArrayList<>();
    keys.add(List.of("203.22.43.153"));
    keys.add(List.of("203.22.43.152"));
    keys.add(List.of("203.22.43.155"));
    keys.add(List.of("203.22.43.154"));
    final var retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);
    assertEquals(3, retrieved.size());
    assertEquals("Fremont", retrieved.get(0).getAttributes().get(valueName));
    assertEquals("Redwood City", retrieved.get(1).getAttributes().get(valueName));
    assertEquals("San Mateo", retrieved.get(2).getAttributes().get(valueName));

    assertEquals(4, metrics.getLookupCount());
    assertEquals(3, metrics.getCacheHitCount());

    // try join insertion
    final var response =
        TestUtils.insert(dataServiceHandler, tenantName, signalName, "203.22.43.154");
    final var ingestTimestamp = response.getTimeStamp();

    assertEquals(5, metrics.getLookupCount());
    assertEquals(4, metrics.getCacheHitCount());

    // verify ingested event
    ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(ingestTimestamp);
    extractRequest.setEndTime(ingestTimestamp + 1000);
    final var events =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
    assertThat(events.size(), is(1));
    Event event = events.get(0);
    assertEquals("San Mateo", event.getAttributes().get(valueName));

    assertEquals(5, metrics.getLookupCount());
    assertEquals(4, metrics.getCacheHitCount());
  }

  @Test
  public void testIntegerContextKey()
      throws IOException,
          ApplicationException,
          TfosException,
          ExecutionException,
          InterruptedException {
    final String src =
        "{"
            + "  'name': 'integerContext',"
            + "  'streams': [{"
            + "    'name': 'integerContext',"
            + "    'type': 'context',"
            + "    'attributes': ["
            + "      {'name': 'integerKey', 'type': 'int'},"
            + "      {'name': 'stringAttr', 'type': 'string'}"
            + "    ]"
            + "  }]"
            + "}";
    final TenantConfig tenantConfig =
        TfosObjectMapperProvider.get().readValue(src.replaceAll("'", "\""), TenantConfig.class);
    tenantConfig.setVersion(timestamp);
    try {
      tfosAdmin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      tfosAdmin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // This is ok
    }
    final TenantDesc tenantDesc = new TenantDesc(tenantConfig);
    testTenants.add(tenantDesc);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    final String tenantName = tenantConfig.getName();
    final String contextName = tenantConfig.getStreams().get(0).getName();

    // put valid context entries
    {
      final var entries = List.of("1,American", "2,Chinese", "3,Japanese", "4,Chinese");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, contextName, entries);
    }

    // try putting invalid context entries
    {
      final var entries = List.of("tentacle,Atlantic");

      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, tenantName, contextName, entries));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid value syntax: Input must be a numeric string;"
                  + " attribute=integerKey, type=Int, value=tentacle"));
    }

    // test getting entries
    {
      final List<List<Object>> keys =
          List.of(List.of(1L), List.of(2L), List.of(3L), List.of(4L), List.of(5L));
      final var entries =
          TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);
      assertEquals(4, entries.size());
      assertEquals(1, entries.get(0).getAttributes().get("integerKey"));
      assertEquals("American", entries.get(0).getAttributes().get("stringAttr"));
      assertEquals(2, entries.get(1).getAttributes().get("integerKey"));
      assertEquals("Chinese", entries.get(1).getAttributes().get("stringAttr"));
      assertEquals(3, entries.get(2).getAttributes().get("integerKey"));
      assertEquals("Japanese", entries.get(2).getAttributes().get("stringAttr"));
      assertEquals(4, entries.get(3).getAttributes().get("integerKey"));
      assertEquals("Chinese", entries.get(3).getAttributes().get("stringAttr"));
    }

    // try getting with invalid key
    {
      final List<Object> keys = List.of("octopus");
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.getContextEntriesSingleKeys(
                      dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid value syntax: Input must be a numeric string;"
              + " attribute=integerKey, type=Int, value=octopus",
          exception.getMessage());
    }
  }

  @Test
  public void loadManyEntries()
      throws ApplicationException, TfosException, ExecutionException, InterruptedException {
    // build the tenant
    String tenantName = "manyContextEntriesTestTenant";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final String keyName = "index";
    final String valueName = "value";
    StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc(keyName, InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc(valueName, InternalAttributeType.INT));
    tenantDesc.addStream(context);

    // Add the tenant to AdminInternal
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    tfosAdmin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    tfosAdmin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    context = tfosAdmin.getStream(tenantName, contextName);
    assertNotNull(context);

    final int numEntries = 70000;
    final int breakPoint = 5000;

    // add initial entries
    final List<Event> entries = new ArrayList<>();
    for (int i = 0; i < breakPoint; ++i) {
      final Event entry = new EventJson();
      entry.getAttributes().put(keyName, Integer.toString(i));
      entry.getAttributes().put(valueName, i);
      entries.add(entry);
      if ((i + 1) % 1000 == 0) {
        System.out.println("writing 1000 entries (" + (i / 1000 * 1000) + "-" + i + ")");
        final var state =
            TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries, ++timestamp);
        dataEngine
            .putContextEntriesAsync(state)
            .thenCompose(
                (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
            .toCompletableFuture()
            .get();
        entries.clear();
      }
    }

    final List<List<Object>> keysInitial = listContextReferenceKeys(dataEngine, context);
    assertEquals(breakPoint, keysInitial.size());

    for (int i = 0; i < 3500; ++i) {
      entries.clear();
      final int key = 21;
      final Event entry = new EventJson();
      entry.getAttributes().put(keyName, Integer.toString(key));
      entry.getAttributes().put(valueName, i);
      entries.add(entry);
      final var state =
          TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
      if ((i + 1) % 1000 == 0) {
        System.out.println("overwrote " + (i + 1) + " entries to key " + key);
      }
    }

    final List<List<Object>> keysSecondary = listContextReferenceKeys(dataEngine, context);
    assertEquals(breakPoint, keysSecondary.size());

    entries.clear();
    for (int i = breakPoint; i < numEntries; ++i) {
      final Event entry = new EventJson();
      entry.getAttributes().put(keyName, Integer.toString(i));
      entry.getAttributes().put(valueName, i);
      entries.add(entry);
      if ((i + 1) % 1000 == 0) {
        System.out.println("writing 1000 entries (" + (i / 1000 * 1000) + "-" + i + ")");
        final var state =
            TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries, ++timestamp);
        dataEngine
            .putContextEntriesAsync(state)
            .thenCompose(
                (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
            .toCompletableFuture()
            .get();
        entries.clear();
      }
    }

    entries.clear();
    for (int i = 0; i < numEntries; i += 100) {
      final Event entry = new EventJson();
      entry.getAttributes().put(keyName, Integer.toString(i));
      entry.getAttributes().put(valueName, i + 10);
      entries.add(entry);
    }

    final var state =
        TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries, ++timestamp);
    dataEngine
        .putContextEntriesAsync(state)
        .thenCompose(
            (none) -> dataEngine.putContextEntriesAsync(state.setPhase(RequestPhase.FINAL)))
        .toCompletableFuture()
        .get();

    // test loading
    System.setProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE, "40000");
    final long start = System.currentTimeMillis();
    DataEngineImpl ingest2 = new DataEngineImpl(conn);
    AdminStore adminStore2 = new AdminStoreImpl(conn);
    AdminInternal admin2 = new AdminImpl(adminStore2, new MetricsStreamProvider(), ingest2);
    final long end = System.currentTimeMillis();
    System.out.println("Loading elapsed time: " + (end - start) + " ms");
    StreamDesc context2 = admin2.getStream(tenantName, contextName);

    final List<List<Object>> keys = listContextReferenceKeys(ingest2, context2);
    assertEquals(numEntries, keys.size());
    Set<Integer> keysSet = new HashSet<>();
    for (List<Object> key : keys) {
      keysSet.add(Integer.valueOf((String) key.get(0)));
    }
    assertEquals(numEntries, keysSet.size());
    for (int i = 0; i < numEntries; ++i) {
      assertTrue("key=" + i, keysSet.contains(i));
    }
  }

  @Test
  public void testContextMod() throws Exception {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    // build the tenant
    String tenantName = this.getClass().getSimpleName() + "_contextMod";
    TenantDesc temp = new TenantDesc(tenantName, ++timestamp, Boolean.FALSE);
    tfosAdmin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    tfosAdmin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());
    testTenants.add(temp);

    final StreamConfig oldConf =
        AdminConfigCreator.makeStreamConfig(TestUtils.CONTEXT_GEO_LOCATION_SRC, ++timestamp);

    AdminTestUtils.populateStream(
        tfosAdmin, tenantName, TestUtils.CONTEXT_GEO_LOCATION_SRC, timestamp);

    final String contextName = oldConf.getName();
    StreamDesc context = tfosAdmin.getStream(tenantName, contextName);
    assertNotNull(context);

    final String ip = "ip";
    final String country = "country";
    final String state = "state";
    final String city = "city";

    final List<Event> entries = new ArrayList<>();
    Event entry1 = new EventJson();
    entry1.getAttributes().put(ip, "192.168.0.11");
    entry1.getAttributes().put(country, "USA");
    entry1.getAttributes().put(state, "CA");
    entries.add(entry1);

    Event entry2 = new EventJson();
    entry2.getAttributes().put(ip, "192.168.0.12");
    entry2.getAttributes().put(country, "Australia");
    entry2.getAttributes().put(state, "Tasmania");
    entries.add(entry2);

    Event entry3 = new EventJson();
    entry3.getAttributes().put(ip, "192.168.0.13");
    entry3.getAttributes().put(country, "Canada");
    entry3.getAttributes().put(state, "British Columbia");
    entries.add(entry3);

    {
      final var state2 =
          TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state2)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state2.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
    }

    // test listing primary keys
    List<List<Object>> listed = listContextReferenceKeys(dataEngine, context);
    assertNotNull(listed);
    assertEquals(3, listed.size());
    assertTrue(listed.contains(List.of("192.168.0.11")));
    assertTrue(listed.contains(List.of("192.168.0.12")));
    assertTrue(listed.contains(List.of("192.168.0.13")));

    // test getting context entries
    List<List<Object>> keys = new ArrayList<>();
    keys.add(List.of("192.168.0.11"));
    keys.add(List.of("192.168.0.12"));
    keys.add(List.of("192.168.0.13"));
    final var retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

    assertNotNull(retrieved);
    assertEquals(entries.size(), retrieved.size());
    for (int i = 0; i < entries.size(); ++i) {
      assertEquals(
          Integer.toString(i),
          entries.get(i).getAttributes().get(country),
          retrieved.get(i).getAttributes().get(country));
      assertEquals(
          Integer.toString(i),
          entries.get(i).getAttributes().get(state),
          retrieved.get(i).getAttributes().get(state));
    }

    StreamConfig newConf = oldConf.duplicate();
    newConf
        .getAttributes()
        .add(new AttributeDesc(city, InternalAttributeType.STRING).setDefaultValue("n/a"));
    newConf.setVersion(++timestamp);

    tfosAdmin.modifyStream(tenantName, contextName, newConf, RequestPhase.INITIAL, FORCE, Set.of());
    tfosAdmin.modifyStream(tenantName, contextName, newConf, RequestPhase.FINAL, FORCE, Set.of());
    StreamDesc context2 = tfosAdmin.getStream(tenantName, contextName);

    Event entry4 = new EventJson();
    entry4.getAttributes().put(ip, "192.168.0.11");
    entry4.getAttributes().put(country, "USA");
    entry4.getAttributes().put(state, "CA");
    entry4.getAttributes().put(city, "Redwood City");
    List<Event> entries2 = Arrays.asList(entry4);

    {
      final var state2 =
          TestUtils.makeUpsertState(tfosAdmin, tenantName, contextName, entries2, ++timestamp);
      dataEngine
          .putContextEntriesAsync(state2)
          .thenCompose(
              (none) -> dataEngine.putContextEntriesAsync(state2.setPhase(RequestPhase.FINAL)))
          .toCompletableFuture()
          .get();
    }

    List<List<Object>> listed2 = listContextReferenceKeys(dataEngine, context2);
    assertNotNull(listed2);
    assertEquals(3, listed2.size());
    assertTrue(listed2.contains(List.of("192.168.0.11")));

    final var retrieved2 =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);
    assertNotNull(retrieved2);
    assertEquals(3, retrieved2.size());
    assertEquals("Redwood City", retrieved2.get(0).get(city));
    assertEquals("n/a", retrieved2.get(1).get(city));
    assertEquals("Tasmania", retrieved2.get(1).get(state));
    assertEquals("n/a", retrieved2.get(2).get(city));
    assertEquals("British Columbia", retrieved2.get(2).get(state));
  }

  private List<List<Object>> listContextReferenceKeys(
      DataEngine engine, StreamDesc context, ContextOpOption... options)
      throws ExecutionException, InterruptedException {
    final var future = new CompletableFuture<List<List<Object>>>();

    final var state = new ContextOpState("listPrimaryKeys", Executors.newSingleThreadExecutor());
    state.setContextDesc(context);

    engine.listContextPrimaryKeysAsync(
        context, state, future::complete, future::completeExceptionally);

    return future.get();
  }
}
