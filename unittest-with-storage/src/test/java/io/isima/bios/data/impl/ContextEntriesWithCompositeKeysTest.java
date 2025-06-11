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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.maintenance.TaskSlots;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.ContextMetricsCounter;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextEntriesWithCompositeKeysTest {

  private static String tenantName = "contextEntriesCompositeKeys";

  private static final String STATE = "state";
  private static final String CITY = "city";
  private static final String ZIP_CODE = "zipCode";

  private static long timestamp;
  private static Auth auth;
  private static AdminInternal tfosAdmin;
  private static io.isima.bios.admin.Admin admin;
  private static DataEngineImpl dataEngine;
  private static HttpClientManager clientManager;
  private static AdminServiceHandler adminServiceHandler;
  private static DataServiceHandler dataServiceHandler;
  private static SharedProperties sharedProperties;
  private static CassandraConnection conn;

  private String previousCacheSize;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(false, ContextEntriesWithCompositeKeysTest.class, Map.of());
    auth = BiosModules.getAuth();
    tfosAdmin = BiosModules.getAdminInternal();
    admin = BiosModules.getAdmin();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    clientManager = BiosModules.getHttpClientManager();
    adminServiceHandler = BiosModules.getAdminServiceHandler();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    sharedProperties = BiosModules.getSharedProperties();
    timestamp = System.currentTimeMillis();
    conn = BiosModules.getCassandraConnection();
    AdminTestUtils.setupTenant(admin, tenantName, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    previousCacheSize = System.getProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    TestUtils.revertProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE, previousCacheSize);
    sharedProperties.deleteProperty(DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS);
  }

  @Test
  public void testFundamental()
      throws ApplicationException,
          TfosException,
          FileReadException,
          ExecutionException,
          InterruptedException {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    final String contextName = "fundamentalTest";
    final var contextConfig = makeSimpleContext(contextName);
    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    var context = tfosAdmin.getStream(tenantName, contextName);

    System.out.println(LocalDateTime.now() + ": ------ Initial puts");
    final List<Map<String, Object>> attributes =
        List.of(
            Map.of(STATE, "California", CITY, "Redwood City", ZIP_CODE, 94061L),
            Map.of(STATE, "California", CITY, "Richmond", ZIP_CODE, 94530L),
            Map.of(STATE, "Oregon", CITY, "Portland", ZIP_CODE, 97035L),
            Map.of(STATE, "California", CITY, "Fremont", ZIP_CODE, 94061L),
            Map.of(STATE, "Virginia", CITY, "Richmond", ZIP_CODE, 23173L));
    final List<Event> entries = makeEntries(attributes);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries);

    assertEquals(0, metrics.getLookupCount());
    assertEquals(0, metrics.getCacheHitCount());

    // test getting context entries
    System.out.println(LocalDateTime.now() + ": ------ Initial gets");
    {
      List<List<Object>> keys = new ArrayList<>();
      keys.add(List.of("California", "Redwood City"));
      keys.add(List.of("Virginia", "Richmond"));
      keys.add(List.of("California", "Richmond"));
      List<Event> retrieved =
          TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

      assertNotNull(retrieved);
      assertEquals(keys.size(), retrieved.size());
      assertEquals(attributes.get(0), retrieved.get(0).getAttributes());
      assertEquals(attributes.get(4), retrieved.get(1).getAttributes());
      assertEquals(attributes.get(1), retrieved.get(2).getAttributes());

      // ensure the DataEngine used entry cache
      assertEquals(keys.size(), metrics.getLookupCount());
      assertEquals(keys.size(), metrics.getCacheHitCount());
    }

    // test listing
    System.out.println(LocalDateTime.now() + ": ------ List keys");
    List<List<Object>> listed = listContextPrimaryKeys(dataEngine, context);
    assertNotNull(listed);
    assertEquals(attributes.size(), listed.size());
    assertTrue(listed.contains(List.of("California", "Redwood City")));
    assertTrue(listed.contains(List.of("Virginia", "Richmond")));
    assertTrue(listed.contains(List.of("Oregon", "Portland")));
    assertTrue(listed.contains(List.of("California", "Fremont")));
    assertTrue(listed.contains(List.of("California", "Richmond")));

    // test updating
    System.out.println(LocalDateTime.now() + ": ------ Update");
    final List<Map<String, Object>> updatedAttributes =
        List.of(
            Map.of(STATE, "California", CITY, "Redwood City", ZIP_CODE, 94063L),
            Map.of(STATE, "Oregon", CITY, "Portland", ZIP_CODE, 97203L));
    List<Event> entries2 = makeEntries(updatedAttributes);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries2);

    System.out.println(LocalDateTime.now() + ": ------ Get after update");
    List<List<Object>> keys2 =
        List.of(List.of("California", "Redwood City"), List.of("Oregon", "Portland"));
    List<Event> updated =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys2);

    assertNotNull(updated);
    assertEquals(2, updated.size());
    assertEquals(updatedAttributes.get(0), updated.get(0).getAttributes());
    assertEquals(updatedAttributes.get(1), updated.get(1).getAttributes());

    assertEquals(5, metrics.getLookupCount());
    assertEquals(5, metrics.getCacheHitCount());

    // test reloading
    System.out.println(LocalDateTime.now().toString() + ": ------ Reload");
    final var modules2 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl1", conn);
    AdminInternal admin2 = modules2.getTfosAdmin();
    context = admin2.getStream(tenantName, contextName);
    metrics.fullReset();

    assertNotNull(context);

    System.out.println(LocalDateTime.now() + ": ------ Get after reload");
    {
      List<List<Object>> keys = new ArrayList<>();
      keys.add(List.of("California", "Redwood City"));
      keys.add(List.of("Virginia", "Richmond"));
      keys.add(List.of("Oregon", "Portland"));
      final var dataServiceHandler2 = modules2.getDataServiceHandler();
      List<Event> retrieved =
          TestUtils.getContextEntries(dataServiceHandler2, tenantName, contextName, keys);

      assertEquals(keys.size(), metrics.getLookupCount());
      assertEquals(0, metrics.getCacheHitCount());

      assertNotNull(retrieved);
      assertEquals(keys.size(), retrieved.size());
      assertEquals(updatedAttributes.get(0), retrieved.get(0).getAttributes());
      assertEquals(attributes.get(4), retrieved.get(1).getAttributes());
      assertEquals(updatedAttributes.get(1), retrieved.get(2).getAttributes());
    }

    // test deleting
    System.out.println(LocalDateTime.now() + ": ------ Delete");
    List<List<Object>> keys3 = new ArrayList<>();
    keys3.add(List.of("California", "Redwood City"));
    keys3.add(List.of("Virginia", "Richmond"));

    TestUtils.deleteContextEntries(
        dataServiceHandler, clientManager, tenantName, contextName, keys3);

    System.out.println(LocalDateTime.now() + ": ------ Get after delete");
    final var keysForAll =
        attributes.stream()
            .map((entry) -> List.of(entry.get(STATE), entry.get(CITY)))
            .collect(Collectors.toList());
    {
      List<List<Object>> remainingKeys = listContextPrimaryKeys(dataEngine, context);
      assertNotNull(remainingKeys);
      assertEquals(attributes.size() - 2, remainingKeys.size());
      assertTrue(remainingKeys.contains(List.of("California", "Richmond")));
      assertTrue(remainingKeys.contains(List.of("Oregon", "Portland")));
      assertTrue(remainingKeys.contains(List.of("California", "Fremont")));

      List<Event> remainingEntries =
          TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keysForAll);
      assertEquals(3, remainingEntries.size());
      assertEquals(attributes.get(1), remainingEntries.get(0).getAttributes());
      assertEquals(updatedAttributes.get(1), remainingEntries.get(1).getAttributes());
      assertEquals(attributes.get(3), remainingEntries.get(2).getAttributes());
    }

    // reload
    System.out.println(LocalDateTime.now() + ": ------ Reload again");
    final var modules3 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl2", conn);
    DataEngineImpl dataEngine3 = modules3.getDataEngine();
    AdminInternal admin3 = modules3.getTfosAdmin();
    context = admin3.getStream(tenantName, contextName);

    assertNotNull(context);

    System.out.println(LocalDateTime.now() + ": ------ List after second reload");
    {
      List<List<Object>> remainingKeys = listContextPrimaryKeys(dataEngine3, context);
      assertNotNull(remainingKeys);
      assertEquals(attributes.size() - 2, remainingKeys.size());
      assertTrue(remainingKeys.contains(List.of("California", "Richmond")));
      assertTrue(remainingKeys.contains(List.of("Oregon", "Portland")));
      assertTrue(remainingKeys.contains(List.of("California", "Fremont")));

      final var handler3 = modules3.getDataServiceHandler();
      List<Event> remainingEntries =
          TestUtils.getContextEntries(handler3, tenantName, contextName, keysForAll);
      assertEquals(3, remainingEntries.size());
      assertEquals(attributes.get(1), remainingEntries.get(0).getAttributes());
      assertEquals(updatedAttributes.get(1), remainingEntries.get(1).getAttributes());
      assertEquals(attributes.get(3), remainingEntries.get(2).getAttributes());
    }
  }

  @Test
  public void testCacheOverwriting()
      throws ApplicationException, TfosException, ExecutionException, InterruptedException {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    final String contextName = "Context";
    final String areaName = "areaName";
    final String areaCode = "areaCode";
    final String zipCode = "zipcode";
    final String county = "county";
    final var contextConfig = new ContextConfig(contextName, timestamp);
    contextConfig.setAttributes(
        List.of(
            new AttributeConfig(areaName, AttributeType.STRING),
            new AttributeConfig(areaCode, AttributeType.INTEGER),
            new AttributeConfig(zipCode, AttributeType.INTEGER),
            new AttributeConfig(county, AttributeType.STRING)));
    contextConfig.setPrimaryKey(List.of(areaName, areaCode));
    contextConfig.setMissingAttributePolicy(io.isima.bios.models.MissingAttributePolicy.REJECT);
    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    final List<Map<String, Object>> initialAttributes =
        List.of(
            Map.of(
                areaName, "Redwood City", areaCode, 650L, zipCode, 94061L, county, "Santa Clara"),
            Map.of(
                areaName, "Point Reyes Station", areaCode, 415L, zipCode, 94956L, county, "Marin"));
    List<Event> initialEntries = makeEntries(initialAttributes);

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
    final List<Map<String, Object>> secondAttributes =
        List.of(
            Map.of(
                areaName, "Redwood City", areaCode, 650L, zipCode, 94061L, county, "Santa Clara"),
            Map.of(
                areaName, "Point Reyes Station", areaCode, 415L, zipCode, 94956L, county, "Marin"));
    List<Event> secondEntries = makeEntries(secondAttributes);

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
    List<List<Object>> keys =
        List.of(List.of("Redwood City", 650L), List.of("Point Reyes Station", 415L));
    List<Event> retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

    assertNotNull(retrieved);
    assertEquals(initialEntries.size(), retrieved.size());
    for (int i = 0; i < initialEntries.size(); ++i) {
      assertEquals(secondEntries.get(i).get(areaName), retrieved.get(i).get(areaName));
      assertEquals(secondEntries.get(i).get(areaCode), retrieved.get(i).get(areaCode));
      assertEquals(secondEntries.get(i).get(zipCode), retrieved.get(i).get(zipCode));
      assertEquals(secondEntries.get(i).get(county), retrieved.get(i).get(county));
    }

    // test reloading
    final var modules2 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl3", conn);
    AdminInternal admin2 = modules2.getTfosAdmin();
    var context = admin2.getStream(tenantName, contextName);

    assertNotNull(context);

    List<Event> loaded =
        TestUtils.getContextEntries(
            modules2.getDataServiceHandler(),
            tenantName,
            contextName,
            List.of(List.of("Redwood City", 650L), List.of("Point Reyes Station", 415L)));
    assertNotNull(loaded);
    assertEquals(2, loaded.size());
    assertEquals(secondEntries.get(0).getAttributes(), loaded.get(0).getAttributes());
    assertEquals(secondEntries.get(1).getAttributes(), loaded.get(1).getAttributes());

    // overwrite by different values
    final List<Map<String, Object>> thirdAttributes =
        List.of(
            Map.of(areaName, "Redwood City", areaCode, 650L, zipCode, 94061L, county, "San Mateo"),
            Map.of(
                areaName, "Point Reyes Station", areaCode, 415L, zipCode, 94951L, county, "Marin"));
    List<Event> thirdEntries = makeEntries(thirdAttributes);
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
            List.of(List.of("Redwood City", 650L), List.of("Point Reyes Station", 415L)));
    assertNotNull(retrieved2);
    assertEquals(2, retrieved2.size());
    assertEquals(thirdEntries.get(0).getAttributes(), retrieved2.get(0).getAttributes());
    assertEquals(thirdEntries.get(1).getAttributes(), retrieved2.get(1).getAttributes());

    // reload
    final var modules3 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl4", conn);
    AdminInternal admin3 = modules3.getTfosAdmin();
    context = admin3.getStream(tenantName, contextName);

    assertNotNull(context);

    final var retrieved3 =
        TestUtils.getContextEntries(
            modules3.getDataServiceHandler(),
            tenantName,
            contextName,
            List.of(List.of("Redwood City", 650L)));
    assertNotNull(retrieved3);
    assertEquals(1, retrieved3.size());
    assertEquals(thirdEntries.get(0).getAttributes(), retrieved3.get(0).getAttributes());
  }

  @Test
  public void testIntegerContextKeys()
      throws IOException,
          ApplicationException,
          TfosException,
          ExecutionException,
          InterruptedException {
    final String src =
        "{"
            + "  'contextName': 'integerContext',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'stringAttr', 'type': 'string'},"
            + "    {'attributeName': 'integerKeyTwo', 'type': 'integer'},"
            + "    {'attributeName': 'integerKeyOne', 'type': 'integer'}"
            + "  ],"
            + "  'primaryKey': ['integerKeyOne', 'integerKeyTwo']"
            + "}";
    final var contextConfig =
        BiosObjectMapperProvider.get().readValue(src.replaceAll("'", "\""), ContextConfig.class);
    final String contextName = contextConfig.getName();
    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    // put valid context entries
    {
      final var entries =
          List.of("American,100,1", "Chinese,200,2", "Japanese,300,3", "Chinese,400,4");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, contextName, entries);
    }

    // try putting invalid context entries
    {
      final var keys = List.of("Atlantic,tentacle,animal");
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, tenantName, contextName, keys));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid value syntax: Input must be a numeric string;"
                  + " attribute=integerKeyTwo, type=Integer, value=tentacle"));
    }
    {
      final var keys = List.of("Atlantic,2,animal");
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, tenantName, contextName, keys));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid value syntax: Input must be a numeric string;"
                  + " attribute=integerKeyOne, type=Integer, value=animal"));
    }

    // test getting entries
    {
      final List<List<Object>> keys =
          List.of(
              List.of(1L, 100L),
              List.of(2L, 200L),
              List.of(3L, 300L),
              List.of(4L, 400L),
              List.of(5L, 500L));
      final var entries =
          TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);
      assertEquals(4, entries.size());
      assertEquals(1L, entries.get(0).getAttributes().get("integerKeyOne"));
      assertEquals(100L, entries.get(0).getAttributes().get("integerKeyTwo"));
      assertEquals("American", entries.get(0).getAttributes().get("stringAttr"));
      assertEquals(2L, entries.get(1).getAttributes().get("integerKeyOne"));
      assertEquals(200L, entries.get(1).getAttributes().get("integerKeyTwo"));
      assertEquals("Chinese", entries.get(1).getAttributes().get("stringAttr"));
      assertEquals(3L, entries.get(2).getAttributes().get("integerKeyOne"));
      assertEquals(300L, entries.get(2).getAttributes().get("integerKeyTwo"));
      assertEquals("Japanese", entries.get(2).getAttributes().get("stringAttr"));
      assertEquals(4L, entries.get(3).getAttributes().get("integerKeyOne"));
      assertEquals(400L, entries.get(3).getAttributes().get("integerKeyTwo"));
      assertEquals("Chinese", entries.get(3).getAttributes().get("stringAttr"));
    }

    // try getting with invalid key
    {
      final List<List<Object>> keys = List.of(List.of("octopus", "squid"));
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () -> TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid value syntax: Input must be a numeric string;"
              + " attribute=integerKeyOne, type=Integer, value=octopus",
          exception.getMessage());
    }
    {
      final List<List<Object>> keys = List.of(List.of(4L, "squid"));
      final var exception =
          assertThrows(
              InvalidValueSyntaxException.class,
              () -> TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid value syntax: Input must be a numeric string;"
              + " attribute=integerKeyTwo, type=Integer, value=squid",
          exception.getMessage());
    }
    {
      final List<List<Object>> keys = List.of(List.of(4L));
      final var exception =
          assertThrows(
              InvalidRequestException.class,
              () -> TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid request: Size of primaryKeys[0] is incorrect (should be 2 but 1)",
          exception.getMessage());
    }
    {
      final List<List<Object>> keys = List.of(List.of(4L, 5L, 6L));
      final var exception =
          assertThrows(
              InvalidRequestException.class,
              () -> TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys));
      assertEquals(
          "Invalid request: Size of primaryKeys[0] is incorrect (should be 2 but 3)",
          exception.getMessage());
    }
  }

  @Test
  public void loadManyEntries() throws Exception {
    loadManyEntriesTestCore(
        (contextConfig) -> {
          contextConfig.setName("loadManyEntries");
        });
  }

  @Test
  public void loadManyEntriesReversedPrimaryKey() throws Exception {
    loadManyEntriesTestCore(
        (contextConfig) -> {
          contextConfig.setName("loadManyEntriesRevPKey");
          contextConfig.setAttributes(
              List.of(
                  contextConfig.getAttributes().get(1),
                  contextConfig.getAttributes().get(0),
                  contextConfig.getAttributes().get(2)));
        });
  }

  @Test
  public void loadManyEntriesValueComesFirst() throws Exception {
    loadManyEntriesTestCore(
        (contextConfig) -> {
          contextConfig.setName("loadManyEntriesValueFirst");
          contextConfig.setAttributes(
              List.of(
                  contextConfig.getAttributes().get(2),
                  contextConfig.getAttributes().get(1),
                  contextConfig.getAttributes().get(0)));
        });
  }

  private void loadManyEntriesTestCore(Consumer<ContextConfig> configModifier) throws Exception {
    final String keyNameOne = "indexOne";
    final String keyNameTwo = "indexTwo";
    final String valueName = "value";
    final var contextConfig = new ContextConfig();
    contextConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    contextConfig.setAttributes(
        List.of(
            new AttributeConfig(keyNameOne, AttributeType.STRING),
            new AttributeConfig(keyNameTwo, AttributeType.STRING),
            new AttributeConfig(valueName, AttributeType.INTEGER)));
    contextConfig.setPrimaryKey(List.of(keyNameOne, keyNameTwo));
    configModifier.accept(contextConfig);
    final String contextName = contextConfig.getName();

    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    final int numEntries = 70000;
    final int breakPoint = 5000;
    final int keyTwoMultiplier = 1000;
    final int overwriteInterval = 100;
    final long overwriteOffset = 10;
    final int deepOverwriteKey = 21;
    final int deepOverwriteDepth = 3500;

    // add initial entries
    final List<Event> entries = new ArrayList<>();
    for (int i = 0; i < breakPoint; ++i) {
      final Event entry = new EventJson();
      entry.set(keyNameOne, Integer.toString(i));
      entry.set(keyNameTwo, Integer.toString(i * keyTwoMultiplier));
      entry.set(valueName, Long.valueOf(i));
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

    final List<List<Object>> keysInitial =
        TestUtils.listContextPrimaryKeys(tfosAdmin, dataEngine, tenantName, contextName);
    assertEquals(breakPoint, keysInitial.size());

    for (int i = 0; i < deepOverwriteDepth; ++i) {
      entries.clear();
      final int key = deepOverwriteKey;
      final Event entry = new EventJson();
      entry.set(keyNameOne, Integer.toString(key));
      entry.set(keyNameTwo, Integer.toString(key * keyTwoMultiplier));
      entry.set(valueName, Long.valueOf(i));
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
        System.out.println("overwrote " + (i + 1) + " times to key " + key);
      }
    }

    final List<List<Object>> keysSecondary =
        TestUtils.listContextPrimaryKeys(tfosAdmin, dataEngine, tenantName, contextName);
    assertEquals(breakPoint, keysSecondary.size());

    entries.clear();
    for (int i = breakPoint; i < numEntries; ++i) {
      final Event entry = new EventJson();
      entry.set(keyNameOne, Integer.toString(i));
      entry.set(keyNameTwo, Integer.toString(i * keyTwoMultiplier));
      entry.set(valueName, Long.valueOf(i));
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

    // overwrite every overwriteInterval entry
    entries.clear();
    for (int i = 0; i < numEntries; i += overwriteInterval) {
      final Event entry = new EventJson();
      entry.set(keyNameOne, Integer.toString(i));
      entry.set(keyNameTwo, Integer.toString(i * keyTwoMultiplier));
      entry.set(valueName, i + overwriteOffset);
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
    final var modules2 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl5", conn);
    DataEngineImpl dataEngine2 = modules2.getDataEngine();
    AdminInternal admin2 = modules2.getTfosAdmin();
    final long end = System.currentTimeMillis();
    System.out.println("Loading elapsed time: " + (end - start) + " ms");

    final List<List<Object>> keys =
        TestUtils.listContextPrimaryKeys(admin2, dataEngine2, tenantName, contextName);
    assertEquals(numEntries, keys.size());
    final Set<List<Object>> keysSet = new HashSet<>(keys);
    for (int i = 0; i < numEntries; ++i) {
      final var key = List.of(Integer.toString(i), Integer.toString(i * keyTwoMultiplier));
      assertTrue("key=" + i, keysSet.contains(key));
    }

    //
    // test cache only

    // enable cache only mode
    System.out.println("Reloading with cache only context setup");
    System.setProperty(TfosConfig.CONTEXT_ENTRY_CACHE_MAX_SIZE, Integer.toString(numEntries + 100));
    sharedProperties.setProperty(
        DataEngineImpl.PROP_CACHE_ONLY_CONTEXTS, tenantName + "." + contextName);
    // run data maintenance in the modules set three
    final var modules3 =
        TestModules.reload(ContextEntriesWithCompositeKeysTest.class + " rl6", conn);
    final var taskSlots = new TaskSlots(2);
    modules3.getDataEngine().maintain(taskSlots);
    // wait up to 20 seconds for the context getting into cache-only mode
    final var context = tfosAdmin.getStream(tenantName, contextName);
    final var cassStream = (ContextCassStream) modules3.getDataEngine().getCassStream(context);
    ContextCassStream.CacheMode cacheMode = cassStream.getCacheMode();
    for (int i = 0; i < 30 && cacheMode != ContextCassStream.CacheMode.CACHE_ONLY; ++i) {
      Thread.sleep(1000);
      cacheMode = cassStream.getCacheMode();
    }
    // verify the context has got into cache-only mode
    assertThat(cacheMode, is(ContextCassStream.CacheMode.CACHE_ONLY));

    // verify the cache items are accessible
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    final var queryKeys = new ArrayList<List<Object>>();
    for (int i = 0; i < numEntries; ++i) {
      final var keyValues = new Object[2];
      keyValues[0] = Integer.toString(i);
      keyValues[1] = Integer.toString(i * 1000);
      queryKeys.add(Arrays.asList(keyValues));
      if (queryKeys.size() % 1000 == 0) {
        final var retrieved =
            TestUtils.getContextEntries(
                modules3.getDataServiceHandler(), tenantName, contextName, queryKeys);
        assertThat(retrieved.size(), is(1000));
        for (int j = 0; j < retrieved.size(); ++j) {
          final var entry = retrieved.get(j);
          final var keyOne = (String) entry.get(keyNameOne);
          final var keyTwo = (String) entry.get(keyNameTwo);
          final var value = (Long) entry.get(valueName);
          assertThat(keyOne, notNullValue());
          assertThat(keyTwo, notNullValue());
          assertThat(value, notNullValue());
          final var keyOneValue = Long.valueOf(keyOne.toString());
          final var keyTwoValue = Long.valueOf(keyTwo.toString());
          assertThat(keyTwoValue, equalTo(keyOneValue * keyTwoMultiplier));
          if (keyOneValue.equals(Long.valueOf(deepOverwriteKey))) {
            assertThat(value, equalTo(Long.valueOf(deepOverwriteDepth - 1)));
          } else if (keyOneValue % overwriteInterval == 0) {
            assertThat(value, equalTo(keyOneValue + overwriteOffset));
          } else {
            assertThat(value, equalTo(keyOneValue));
          }
        }
        queryKeys.clear();
      }
    }
    assertThat(metrics.getLookupCount(), is((long) numEntries));
    assertThat(metrics.getCacheHitCount(), is((long) numEntries));
  }

  @Test
  public void testContextMod() throws Exception {
    final ContextMetricsCounter metrics = ContextCassStream.getMetrics();
    metrics.fullReset();

    final var oldConf =
        AdminTestUtils.createContext(
            auth, adminServiceHandler, tenantName, TestUtils.CONTEXT_GEO_LOCATION_MD_SRC);

    final String contextName = oldConf.getName();

    final String ipVersion = "ipVersion";
    final String ip = "ip";
    final String country = "country";
    final String state = "state";
    final String city = "city";

    final List<Event> entries = new ArrayList<>();
    Event entry1 = new EventJson();
    entry1.set(ipVersion, "v4");
    entry1.set(ip, "192.168.0.11");
    entry1.set(country, "USA");
    entry1.set(state, "CA");
    entries.add(entry1);

    Event entry2 = new EventJson();
    entry2.set(ipVersion, "v4");
    entry2.set(ip, "192.168.0.12");
    entry2.set(country, "Australia");
    entry2.set(state, "Tasmania");
    entries.add(entry2);

    Event entry3 = new EventJson();
    entry3.set(ipVersion, "v6");
    entry3.set(ip, "2001:0000:130F:0000:0000:09C0:876A:130B");
    entry3.set(country, "Canada");
    entry3.set(state, "British Columbia");
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
    List<List<Object>> listed =
        TestUtils.listContextPrimaryKeys(tfosAdmin, dataEngine, tenantName, contextName);
    assertNotNull(listed);
    assertEquals(3, listed.size());
    assertTrue(listed.contains(List.of("v4", "192.168.0.11")));
    assertTrue(listed.contains(List.of("v4", "192.168.0.12")));
    assertTrue(listed.contains(List.of("v6", "2001:0000:130F:0000:0000:09C0:876A:130B")));

    // test getting context entries
    List<List<Object>> keys = new ArrayList<>();
    keys.add(List.of("v4", "192.168.0.11"));
    keys.add(List.of("v4", "192.168.0.12"));
    keys.add(List.of("v6", "2001:0000:130F:0000:0000:09C0:876A:130B"));
    final var retrieved =
        TestUtils.getContextEntries(dataServiceHandler, tenantName, contextName, keys);

    assertNotNull(retrieved);
    assertEquals(entries.size(), retrieved.size());
    for (int i = 0; i < entries.size(); ++i) {
      assertEquals(entries.get(i).get(country), retrieved.get(i).getAttributes().get(country));
      assertEquals(entries.get(i).get(state), retrieved.get(i).getAttributes().get(state));
    }

    final var newConf = new ContextConfig(oldConf);
    final var newAttributes = new ArrayList<>(newConf.getAttributes());
    final var newAttribute = new AttributeConfig(city, AttributeType.STRING);
    newAttribute.setDefaultValue(new AttributeValueGeneric("n/a", AttributeType.STRING));
    newAttributes.add(newAttribute);
    newConf.setAttributes(newAttributes);

    AdminTestUtils.updateContext(auth, adminServiceHandler, tenantName, contextName, newConf);

    Event entry4 = new EventJson();
    entry4.set(ipVersion, "v4");
    entry4.set(ip, "192.168.0.11");
    entry4.set(country, "USA");
    entry4.set(state, "CA");
    entry4.set(city, "Redwood City");
    List<Event> entries2 = List.of(entry4);

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

    List<List<Object>> listed2 =
        TestUtils.listContextPrimaryKeys(tfosAdmin, dataEngine, tenantName, contextName);
    assertNotNull(listed2);
    assertEquals(3, listed2.size());
    assertTrue(listed2.contains(List.of("v4", "192.168.0.11")));

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

  @Test
  public void testEnrichmentFundamental() throws Exception {
    final String contextName = "enrichmentContext";
    final var contextConfig = makeSimpleContext(contextName);
    // make the first attribute enum
    contextConfig
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("Oregon", AttributeType.STRING),
                new AttributeValueGeneric("Virginia", AttributeType.STRING),
                new AttributeValueGeneric("California", AttributeType.STRING)));

    testEnrichmentCore(contextConfig, "enrichment1");
  }

  @Test
  public void testEnrichmentReversedPrimaryKey() throws Exception {
    final String contextName = "enrichmentContextReversePkey";
    final var contextConfig = makeSimpleContext(contextName);
    final var attributes = contextConfig.getAttributes();
    // make the first attribute enum
    attributes
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("Oregon", AttributeType.STRING),
                new AttributeValueGeneric("Virginia", AttributeType.STRING),
                new AttributeValueGeneric("California", AttributeType.STRING)));
    // reverse the attributes
    contextConfig.setAttributes(List.of(attributes.get(1), attributes.get(0), attributes.get(2)));

    testEnrichmentCore(contextConfig, "enrichment2");
  }

  private void testEnrichmentCore(ContextConfig contextConfig, String signalName) throws Exception {
    final var contextName = contextConfig.getName();
    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    final var signalConfig = new SignalConfig(signalName);
    signalConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final String myState = "myState";
    final String myCity = "myCity";
    final String itemNumber = "itemNumber";
    signalConfig.setAttributes(
        List.of(
            new AttributeConfig(myState, AttributeType.STRING),
            new AttributeConfig(myCity, AttributeType.STRING),
            new AttributeConfig(itemNumber, AttributeType.INTEGER)));
    signalConfig
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("Oregon", AttributeType.STRING),
                new AttributeValueGeneric("Virginia", AttributeType.STRING),
                new AttributeValueGeneric("California", AttributeType.STRING)));
    final var enrichment = new EnrichmentConfigSignal("byStateCity");
    enrichment.setForeignKey(List.of("myState", "myCity"));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextName(contextName);
    enrichment.setContextAttributes(List.of(new EnrichmentAttribute(ZIP_CODE)));
    signalConfig.setEnrich(new EnrichConfig(List.of(enrichment)));
    AdminTestUtils.createSignal(auth, adminServiceHandler, tenantName, signalConfig);

    System.out.println(LocalDateTime.now() + ": ------ Populate context first");
    final List<Map<String, Object>> attributes =
        List.of(
            Map.of(STATE, "California", CITY, "Redwood City", ZIP_CODE, 94061L),
            Map.of(STATE, "California", CITY, "Richmond", ZIP_CODE, 94530L),
            Map.of(STATE, "Oregon", CITY, "Portland", ZIP_CODE, 97035L),
            Map.of(STATE, "California", CITY, "Fremont", ZIP_CODE, 94061L),
            Map.of(STATE, "Virginia", CITY, "Richmond", ZIP_CODE, 23173L));
    final List<Event> entries = makeEntries(attributes);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries);

    // test getting context entries
    System.out.println(LocalDateTime.now() + ": ------ Insert signal");
    long start;
    {
      final var csv = "California,Richmond,90";
      final var response = TestUtils.insert(dataServiceHandler, tenantName, signalName, csv);
      start = response.getTimeStamp();
    }

    // retrieve the event
    {
      final var request = new ExtractRequest();
      request.setStartTime(start);
      request.setEndTime(System.currentTimeMillis() + 1);
      final var events = TestUtils.extract(dataServiceHandler, tenantName, signalName, request);
      System.out.println(events);
      assertThat(events.size(), is(1));
      final var event = events.get(0);
      assertThat(event.get(myState), is("California"));
      assertThat(event.get(myCity), is("Richmond"));
      assertThat(event.get(itemNumber), is(90L));
      assertThat(event.get(ZIP_CODE), is(94530L));
    }
  }

  // This will be rejected at earlier stage, but should work fine internally
  @Test
  public void selectAll() throws Exception {
    final String contextName = "selectAll";
    final var contextConfig = makeSimpleContext(contextName);
    AdminTestUtils.createContext(auth, adminServiceHandler, tenantName, contextConfig);

    final List<Map<String, Object>> attributes =
        List.of(
            Map.of(STATE, "California", CITY, "Redwood City", ZIP_CODE, 94061L),
            Map.of(STATE, "California", CITY, "Richmond", ZIP_CODE, 94530L),
            Map.of(STATE, "Oregon", CITY, "Portland", ZIP_CODE, 97035L),
            Map.of(STATE, "California", CITY, "Fremont", ZIP_CODE, 94061L),
            Map.of(STATE, "Virginia", CITY, "Richmond", ZIP_CODE, 23173L));
    final List<Event> entries = makeEntries(attributes);

    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, tfosAdmin, tenantName, contextName, entries);

    final var contextDesc = tfosAdmin.getStream(tenantName, contextName);

    final var request = new SelectContextRequest();
    request.setContext(contextName);
    final var state = new ContextOpState("selectAll", TestUtils.makeGenericState());
    state.setContextDesc(contextDesc);
    state.setStreamName(contextName);
    state.setTenantName(tenantName);
    state.setOptions(Set.of());
    final var response =
        DataUtils.wait(
            dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture(),
            "selectAll");
    assertThat(response.getEntries().size(), is(5));
    assertTrue(
        response
            .getEntries()
            .contains(Map.of(STATE, "California", CITY, "Redwood City", ZIP_CODE, 94061L)));
    assertTrue(
        response
            .getEntries()
            .contains(Map.of(STATE, "California", CITY, "Richmond", ZIP_CODE, 94530L)));
    assertTrue(
        response
            .getEntries()
            .contains(Map.of(STATE, "Oregon", CITY, "Portland", ZIP_CODE, 97035L)));
    assertTrue(
        response
            .getEntries()
            .contains(Map.of(STATE, "California", CITY, "Fremont", ZIP_CODE, 94061L)));
    assertTrue(
        response
            .getEntries()
            .contains(Map.of(STATE, "Virginia", CITY, "Richmond", ZIP_CODE, 23173L)));
  }

  private ContextConfig makeSimpleContext(String contextName) {
    final var contextConfig = new ContextConfig(contextName);
    contextConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    contextConfig.setAttributes(
        List.of(
            new AttributeConfig(STATE, AttributeType.STRING),
            new AttributeConfig(CITY, AttributeType.STRING),
            new AttributeConfig(ZIP_CODE, AttributeType.INTEGER)));
    contextConfig.setPrimaryKey(List.of(STATE, CITY));
    return contextConfig;
  }

  private List<Event> makeEntries(List<Map<String, Object>> attributes) {
    List<Event> entries = new ArrayList<>();
    attributes.forEach(
        (entry) -> {
          final var event = new EventJson();
          entry.forEach((name, value) -> event.set(name, value));
          entries.add(event);
        });
    return entries;
  }

  private List<List<Object>> listContextPrimaryKeys(
      DataEngine engine, StreamDesc context, ContextOpOption... options)
      throws ApplicationException, TfosException {
    final var future = new CompletableFuture<List<List<Object>>>();

    final var state = new ContextOpState("listPrimaryKeys", Executors.newSingleThreadExecutor());
    state.setContextDesc(context);

    engine.listContextPrimaryKeysAsync(
        context, state, future::complete, future::completeExceptionally);

    return DataUtils.wait(future, "listContextPrimaryKeys");
  }
}
