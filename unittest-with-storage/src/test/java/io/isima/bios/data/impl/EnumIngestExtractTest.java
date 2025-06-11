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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditManagerImpl;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidDefaultValueException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.mapper.JsonMappingExceptionMapper;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.Count;
import io.isima.bios.models.ErrorResponsePayload;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnumIngestExtractTest {

  private static long timestamp;
  private static AdminInternal admin;
  private static DataEngine dataEngine;
  private static HttpClientManager clientManager;

  private static CassandraConnection cassandraConnection;
  private static DataServiceHandler dataServiceHandler;

  private static String src;
  private static String tenantName;
  private static TenantConfig tenantConfig;
  private static String signalName;
  private static String restaurantContextName;
  private static String managementContextName;

  // Async process executor
  ExecutorService service = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(false, EnumIngestExtractTest.class, Map.of());
    admin = BiosModules.getAdminInternal();
    dataEngine = BiosModules.getDataEngine();
    clientManager = BiosModules.getHttpClientManager();
    cassandraConnection = BiosModules.getCassandraConnection();
    dataServiceHandler = BiosModules.getDataServiceHandler();

    src =
        "{"
            + "    'name': 'enumIngestExtractTest',"
            + "    'streams': [{"
            + "        'name': 'meal_event',"
            + "        'type': 'signal',"
            + "        'missingValuePolicy': 'strict',"
            + "        'attributes': ["
            + "            {'name': 'first_name', 'type': 'string'},"
            + "            {'name': 'last_name', 'type': 'string'},"
            + "            {'name': 'title', 'type': 'enum',"
            + "             'enum': ['None', 'Mr', 'Mrs', 'Ms', 'Dr']},"
            + "            {'name': 'restaurant_id', 'type': 'long'},"
            + "            {'name': 'region', 'type': 'enum',"
            + "             'enum': ['Americas', 'Asia Pacific', 'EMEA']}"
            + "        ],"
            + "        'preprocesses': [{"
            + "            'name': 'merge_restaurant',"
            + "            'condition': 'restaurant_id',"
            + "            'missingLookupPolicy': 'use_default',"
            + "            'actions': ["
            + "                {'actionType': 'merge', 'context': 'restaurant', 'attribute': 'cousin',"
            + "                 'defaultValue': 'American'}"
            + "            ]"
            + "        }, {"
            + "            'name': 'merge_management',"
            + "            'condition': 'region',"
            + "            'missingLookupPolicy': 'strict',"
            + "            'actions': ["
            + "                {'actionType': 'merge', 'context': 'management', 'attribute': 'address'}"
            + "            ]"
            + "        }]"
            + "    }, {"
            + "        'name': 'restaurant',"
            + "        'type': 'context',"
            + "        'missingValuePolicy': 'strict',"
            + "        'attributes': ["
            + "            {'name': 'id', 'type': 'long'},"
            + "            {'name': 'cousin', 'type': 'enum', 'missingValuePolicy': 'use_default',"
            + "             'enum': ['n/a', 'American', 'Chinese', 'Indian', 'Japanese', 'Mexican'],"
            + "             'defaultValue': 'n/a'}"
            + "        ],"
            + "        'primaryKey': ['id']"
            + "    }, {"
            + "        'name': 'management',"
            + "        'type': 'context',"
            + "        'missingValuePolicy': 'strict',"
            + "        'attributes': ["
            + "            {'name': 'region', 'type': 'enum',"
            + "             'enum': ['Americas', 'Asia Pacific', 'EMEA']},"
            + "            {'name': 'address', 'type': 'string'}"
            + "        ],"
            + "        'primaryKey': ['region']"
            + "    }]"
            + "}";
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    tenantConfig =
        TfosObjectMapperProvider.get().readValue(src.replaceAll("'", "\""), TenantConfig.class);

    tenantName = tenantConfig.getName();
    signalName = tenantConfig.getStreams().get(0).getName();
    restaurantContextName = tenantConfig.getStreams().get(1).getName();
    managementContextName = tenantConfig.getStreams().get(2).getName();

    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws Exception {
    timestamp = System.currentTimeMillis();
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testBasic() throws Throwable {
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // test putting normal restaurant context entries
    {
      final var entries = List.of("1,American", "2,Chinese", "3,", "4,Chinese");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, restaurantContextName, entries);
    }

    // test getting restaurant context entries
    {
      final List<Object> keys = Arrays.asList(new String[] {"1", "2", "3"});
      final var entries =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler, tenantName, restaurantContextName, keys);
      assertEquals(3, entries.size());
      assertEquals(1L, entries.get(0).getAttributes().get("id"));
      assertEquals("American", entries.get(0).getAttributes().get("cousin"));
      assertEquals(2L, entries.get(1).getAttributes().get("id"));
      assertEquals("Chinese", entries.get(1).getAttributes().get("cousin"));
      assertEquals(3L, entries.get(2).getAttributes().get("id"));
      assertEquals("n/a", entries.get(2).getAttributes().get("cousin"));
    }

    // test putting invalid restaurant context entry
    {
      final var entries = List.of("12,Norwegian");
      final var exception =
          assertThrows(
              InvalidEnumException.class,
              () ->
                  TestUtils.upsertContextEntries(
                      dataServiceHandler,
                      clientManager,
                      tenantName,
                      restaurantContextName,
                      entries));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid enum: Specified name is not in the list of enum entries;"
                  + " attribute=cousin, type=Enum, value=Norwegian"));
    }

    // test putting normal management context entries
    {
      final var entries = List.of("Asia Pacific,\"Tokyo, Japan\"", "Americas,\"Denver, USA\"");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, managementContextName, entries);
    }

    // test getting management context entries (normal case)
    {
      final List<Object> keys = Arrays.asList(new String[] {"Asia Pacific", "Americas"});
      final var entries =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler, tenantName, managementContextName, keys);
      assertEquals(2, entries.size());
      assertEquals("Asia Pacific", entries.get(0).getAttributes().get("region"));
      assertEquals("Tokyo, Japan", entries.get(0).getAttributes().get("address"));
      assertEquals("Americas", entries.get(1).getAttributes().get("region"));
      assertEquals("Denver, USA", entries.get(1).getAttributes().get("address"));
    }

    // test listing management primary keys
    {
      final var keys =
          TestUtils.listContextPrimaryKeys(admin, dataEngine, tenantName, managementContextName);
      assertEquals(2, keys.size());
      assertEquals(List.of("Americas"), keys.get(0));
      assertEquals(List.of("Asia Pacific"), keys.get(1));
    }

    // test getting context entries (negative case)
    {
      final List<Object> keys = Arrays.asList(new String[] {"Arctic"});
      final var exception =
          assertThrows(
              InvalidEnumException.class,
              () ->
                  TestUtils.getContextEntriesSingleKeys(
                      dataServiceHandler, tenantName, managementContextName, keys));
      assertEquals(
          "Invalid enum: Specified name is not in the list of enum entries;"
              + " attribute=region, type=Enum, value=Arctic",
          exception.getMessage());
    }

    StreamConfig signal = admin.getStream(tenantName, signalName);

    long startTime = Long.MAX_VALUE;
    // Ingest events
    {
      final var response =
          TestUtils.insert(
              dataServiceHandler, tenantName, signalName, "Indiana,Jones,Dr,2,Americas");
      startTime = response.getTimeStamp();
    }
    TestUtils.insert(dataServiceHandler, tenantName, signalName, "John,Doe,Mr,14,Asia Pacific");

    // Try extracting events
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 10 * 1000);
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
      assertEquals(2, result.size());
      assertEquals("Indiana", result.get(0).getAttributes().get("first_name"));
      assertEquals("Jones", result.get(0).getAttributes().get("last_name"));
      assertEquals("Dr", result.get(0).getAttributes().get("title"));
      assertEquals(2L, result.get(0).getAttributes().get("restaurant_id"));
      assertEquals("Americas", result.get(0).getAttributes().get("region"));
      assertEquals("Chinese", result.get(0).getAttributes().get("cousin"));
      assertEquals("Denver, USA", result.get(0).getAttributes().get("address"));

      assertEquals("John", result.get(1).getAttributes().get("first_name"));
      assertEquals("Doe", result.get(1).getAttributes().get("last_name"));
      assertEquals("Mr", result.get(1).getAttributes().get("title"));
      assertEquals(14L, result.get(1).getAttributes().get("restaurant_id"));
      assertEquals("Asia Pacific", result.get(1).getAttributes().get("region"));
      assertEquals("American", result.get(1).getAttributes().get("cousin"));
      assertEquals("Tokyo, Japan", result.get(1).getAttributes().get("address"));
    }

    // Try filter
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 10 * 1000);
      extractRequest.setFilter("cousin = 'Chinese'");
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
      assertEquals(1, result.size());
      assertEquals("Indiana", result.get(0).getAttributes().get("first_name"));
      assertEquals("Jones", result.get(0).getAttributes().get("last_name"));
      assertEquals("Dr", result.get(0).getAttributes().get("title"));
      assertEquals(2L, result.get(0).getAttributes().get("restaurant_id"));
      assertEquals("Americas", result.get(0).getAttributes().get("region"));
      assertEquals("Chinese", result.get(0).getAttributes().get("cousin"));
      assertEquals("Denver, USA", result.get(0).getAttributes().get("address"));
    }

    // Try invalid filter
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 10 * 1000);
      extractRequest.setFilter("cousin = 'Turkish'");

      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
      assertThat(exception, instanceOf(InvalidFilterException.class));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid filter: Invalid enum: Specified name is not in the list"
                  + " of enum entries; attribute=cousin, type=Enum, value=Turkish"));
    }

    // Ingest more for view test
    TestUtils.insert(dataServiceHandler, tenantName, signalName, "Rick,Deckard,Mr,4,Americas");

    // Try extracting with group view
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 30 * 1000);
      final View view = new Group("cousin");
      extractRequest.setView(view);
      extractRequest.setAggregates(List.of(new Count()));
      extractRequest.setAttributes(List.of("cousin"));
      final var result =
          TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
      assertEquals(2, result.size());
      assertEquals("American", result.get(0).getAttributes().get("cousin"));
      assertEquals(1L, result.get(0).getAttributes().get("count()"));
      assertEquals("Chinese", result.get(1).getAttributes().get("cousin"));
      assertEquals(2L, result.get(1).getAttributes().get("count()"));
    }

    // sum of enum attribute values should be rejected
    {
      final ExtractRequest extractRequest = new ExtractRequest();
      extractRequest.setStartTime(startTime);
      extractRequest.setEndTime(startTime + 30 * 1000);
      extractRequest.setView(new Group("cousin"));
      extractRequest.setAggregates(List.of(new Sum("cousin")));
      extractRequest.setAttributes(List.of("cousin"));
      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
      assertThat(exception, instanceOf(ConstraintViolationException.class));
      assertThat(
          exception.getMessage(),
          is(
              "Constraint violation: Query #0: Attribute 'cousin' of type ENUM"
                  + " for SUM metric is not addable"));
    }
  }

  @Test
  public void testContextLoad() throws Throwable {
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // test putting normal restaurant context entries
    {
      final var entries = List.of("1,American", "2,Chinese", "3,", "4,Chinese");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, restaurantContextName, entries);
    }

    // test putting a normal management context entries
    {
      final var entries =
          List.of(
              "Americas,\"Denver, USA\"",
              "Asia Pacific,\"Tokyo, Japan\"",
              "EMEA,\"Geneva, Switzerland\"");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, managementContextName, entries);
    }

    // load new admin and check if the contexts are available
    OperationMetrics metrics2 = null;
    try {
      final DataEngineImpl dataEngineImpl2 = new DataEngineImpl(cassandraConnection);
      final AdminStore adminStore2 = new AdminStoreImpl(cassandraConnection);
      metrics2 = new OperationMetrics(dataEngineImpl2);
      final AdminInternal admin2 =
          new AdminImpl(adminStore2, new MetricsStreamProvider(), dataEngineImpl2, metrics2);
      final SharedProperties sharedProperties2 = new SharedPropertiesImpl(cassandraConnection);
      final SharedConfig sharedConfig2 = new SharedConfig(sharedProperties2);
      final var dataServiceHandler2 =
          new DataServiceHandler(dataEngineImpl2, admin2, metrics2, sharedConfig2);

      final List<Object> keys = Arrays.asList(new String[] {"1", "2", "3"});
      final var entries =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler2, tenantName, restaurantContextName, keys);
      assertEquals(3, entries.size());
      assertEquals(1L, entries.get(0).getAttributes().get("id"));
      assertEquals("American", entries.get(0).getAttributes().get("cousin"));
      assertEquals(2L, entries.get(1).getAttributes().get("id"));
      assertEquals("Chinese", entries.get(1).getAttributes().get("cousin"));
      assertEquals(3L, entries.get(2).getAttributes().get("id"));
      assertEquals("n/a", entries.get(2).getAttributes().get("cousin"));

      final List<Object> keys2 = Arrays.asList(new String[] {"EMEA", "Americas", "Asia Pacific"});
      final var entries2 =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler2, tenantName, managementContextName, keys2);
      assertEquals(3, entries2.size());
      assertEquals("EMEA", entries2.get(0).getAttributes().get("region"));
      assertEquals("Geneva, Switzerland", entries2.get(0).getAttributes().get("address"));
      assertEquals("Americas", entries2.get(1).getAttributes().get("region"));
      assertEquals("Denver, USA", entries2.get(1).getAttributes().get("address"));
      assertEquals("Asia Pacific", entries2.get(2).getAttributes().get("region"));
      assertEquals("Tokyo, Japan", entries2.get(2).getAttributes().get("address"));
    } finally {
      if (metrics2 != null) {
        metrics2.shutdown();
      }
    }

    // update a restaurant context entry
    {
      final var entries = List.of("3,Indian");
      TestUtils.upsertContextEntries(
          dataServiceHandler, clientManager, tenantName, restaurantContextName, entries);
    }

    Thread.sleep(2000);

    // delete a management context entry
    {
      final List<List<Object>> keys = List.of(List.of("EMEA"));
      TestUtils.deleteContextEntries(
          dataServiceHandler, clientManager, tenantName, managementContextName, keys);

      final List<Object> keys2 = Arrays.asList(new String[] {"EMEA", "Americas", "Asia Pacific"});
      final var entries2 =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler, tenantName, managementContextName, keys2);
      assertEquals(2, entries2.size());
      assertEquals("Americas", entries2.get(0).getAttributes().get("region"));
      assertEquals("Denver, USA", entries2.get(0).getAttributes().get("address"));
      assertEquals("Asia Pacific", entries2.get(1).getAttributes().get("region"));
      assertEquals("Tokyo, Japan", entries2.get(1).getAttributes().get("address"));
    }

    // try deleting a management context entry with an invalid key
    {
      final List<List<Object>> keys = List.of(List.of("Antarctic"));
      final var exception =
          assertThrows(
              InvalidEnumException.class,
              () ->
                  TestUtils.deleteContextEntries(
                      dataServiceHandler, clientManager, tenantName, managementContextName, keys));
      assertThat(
          exception.getMessage(),
          is(
              "Invalid enum: Specified name is not in the list of enum entries;"
                  + " attribute=region, type=Enum, value=Antarctic"));
    }

    // load another admin and check the modifications
    OperationMetrics metrics3 = null;
    try {
      final DataEngineImpl dataEngineImpl3 = new DataEngineImpl(cassandraConnection);
      final AuditManager auditManager3 = new AuditManagerImpl(dataEngineImpl3);
      final AdminStore adminStore3 = new AdminStoreImpl(cassandraConnection);
      metrics3 = new OperationMetrics(dataEngineImpl3);
      final AdminInternal admin3 =
          new AdminImpl(adminStore3, new MetricsStreamProvider(), dataEngineImpl3, metrics3);
      final SharedProperties sharedProperties3 = new SharedPropertiesImpl(cassandraConnection);
      final SharedConfig sharedConfig3 = new SharedConfig(sharedProperties3);
      final var dataServiceHandler3 =
          new DataServiceHandler(dataEngineImpl3, admin3, metrics3, sharedConfig3);

      final List<Object> keys = Arrays.asList(new String[] {"1", "2", "3", "6"});
      final var entries =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler3, tenantName, restaurantContextName, keys);
      assertEquals(3, entries.size());
      assertEquals(1L, entries.get(0).getAttributes().get("id"));
      assertEquals("American", entries.get(0).getAttributes().get("cousin"));
      assertEquals(2L, entries.get(1).getAttributes().get("id"));
      assertEquals("Chinese", entries.get(1).getAttributes().get("cousin"));
      assertEquals(3L, entries.get(2).getAttributes().get("id"));
      assertEquals("Indian", entries.get(2).getAttributes().get("cousin"));

      final List<Object> keys2 = Arrays.asList(new String[] {"EMEA", "Americas", "Asia Pacific"});
      final var entries2 =
          TestUtils.getContextEntriesSingleKeys(
              dataServiceHandler3, tenantName, managementContextName, keys2);
      assertEquals(2, entries2.size());
      assertEquals("Americas", entries2.get(0).getAttributes().get("region"));
      assertEquals("Denver, USA", entries2.get(0).getAttributes().get("address"));
      assertEquals("Asia Pacific", entries2.get(1).getAttributes().get("region"));
      assertEquals("Tokyo, Japan", entries2.get(1).getAttributes().get("address"));

      final var keys3 =
          TestUtils.listContextPrimaryKeys(
              admin3, dataEngineImpl3, tenantName, managementContextName);
      assertEquals(2, keys3.size());
      assertEquals(List.of("Americas"), keys3.get(0));
      assertEquals(List.of("Asia Pacific"), keys3.get(1));
    } finally {
      if (metrics3 != null) {
        metrics3.shutdown();
      }
    }
  }

  @Test
  public void testNullEnumEntryInSignal() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getAttributes().get(2).getEnum().add(null);

    final var exception =
        assertThrows(
            InvalidEnumException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid enum: Enum entry may not be null or empty;"
            + " tenant=enumIngestExtractTest, signal=meal_event, attribute=title,"
            + " enum_index=5",
        exception.getMessage());
  }

  @Test
  public void testDuplicateEnumEntriesInSignal() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getAttributes().get(2).getEnum().add("Mr");

    final var exception =
        assertThrows(
            InvalidEnumException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid enum: Enum entries may not duplicate;"
            + " tenant=enumIngestExtractTest, signal=meal_event, attribute=title, value=Mr",
        exception.getMessage());
  }

  @Test
  public void testDuplicateEnumEntriesInContext() throws Exception {
    final var signal = prepStreamCreation(1);
    signal.getAttributes().get(1).getEnum().add("Indian");

    final var exception =
        assertThrows(
            InvalidEnumException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid enum: Enum entries may not duplicate;"
            + " tenant=enumIngestExtractTest, context=restaurant, attribute=cousin, value=Indian",
        exception.getMessage());
  }

  @Test
  public void testEmptyEnumEntryInSignal() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getAttributes().get(2).getEnum().add("");

    final var exception =
        assertThrows(
            InvalidEnumException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid enum: Enum entry may not be null or empty;"
            + " tenant=enumIngestExtractTest, signal=meal_event, attribute=title,"
            + " enum_index=5",
        exception.getMessage());
  }

  @Test
  public void testBlankEnumEntryInSignal() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getAttributes().get(2).getEnum().add(" ");

    final var exception =
        assertThrows(
            InvalidEnumException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid enum: Enum entry may not be null or empty;"
            + " tenant=enumIngestExtractTest, signal=meal_event, attribute=title,"
            + " enum_index=5",
        exception.getMessage());
  }

  @Test
  public void testIntEnumEntryInSignal() throws IOException {

    final String mySrc = src.replace("'Dr'", "123");
    try {
      tenantConfig =
          TfosObjectMapperProvider.get().readValue(mySrc.replaceAll("'", "\""), TenantConfig.class);
      fail("exception must happen: " + mySrc);
    } catch (JsonMappingException e) {
      JsonMappingExceptionMapper mapper = new JsonMappingExceptionMapper();
      Response resp = mapper.toResponse(e);
      final String message = ((ErrorResponsePayload) resp.getEntity()).getMessage();
      assertEquals(
          "streams[0].attributes[2]: Invalid value syntax: Enum entry name must be a string;"
              + " attribute=title, enum_index=4, value=123",
          message);
    }
  }

  @Test
  public void testInvalidContextDefault() throws Exception {
    final var context = prepStreamCreation(1);
    context.getAttributes().get(1).setDefaultValue("UNKNOWN");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL));
    assertEquals(
        "Constraint violation: Enum default must be one of the entries;"
            + " tenant=enumIngestExtractTest, context=restaurant, attribute=cousin",
        exception.getMessage());
  }

  @Test
  public void testInvalidContextDefault2() throws Exception {
    final var context = prepStreamCreation(1);
    context.getAttributes().get(1).setDefaultValue("");

    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL));
    assertEquals(
        "Constraint violation: Enum default must be one of the entries;"
            + " tenant=enumIngestExtractTest, context=restaurant, attribute=cousin",
        exception.getMessage());
  }

  @Test
  public void testNonExistingLookupDefault() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getPreprocesses().get(0).getActions().get(0).setDefaultValue("UNKNOWN");

    final var context = tenantConfig.getStreams().get(1);
    context.setVersion(timestamp + 1);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.FINAL);

    signal.setVersion(timestamp + 2);
    final var exception =
        assertThrows(
            InvalidDefaultValueException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid default value: Invalid value syntax: Enum default value must match one of entries;"
            + " attribute=cousin, defaultValue=UNKNOWN, tenant=enumIngestExtractTest,"
            + " stream=meal_event, preprocess=merge_restaurant",
        exception.getMessage());
  }

  @Test
  public void testEmptyLookupDefault() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getPreprocesses().get(0).getActions().get(0).setDefaultValue("");

    final var context = tenantConfig.getStreams().get(1);
    context.setVersion(timestamp + 1);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.FINAL);

    signal.setVersion(timestamp + 2);
    final var exception =
        assertThrows(
            InvalidDefaultValueException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid default value: Enum default value may not be an empty string;"
            + " tenant=enumIngestExtractTest, signal=meal_event, preprocess=merge_restaurant,"
            + " attribute=cousin, value=",
        exception.getMessage());
  }

  @Test
  public void testIntegerLookupDefault() throws Exception {
    final var signal = prepStreamCreation(0);
    signal.getPreprocesses().get(0).getActions().get(0).setDefaultValue(123);

    final var context = tenantConfig.getStreams().get(1);
    context.setVersion(timestamp + 1);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.FINAL);

    signal.setVersion(timestamp + 2);
    final var exception =
        assertThrows(
            InvalidDefaultValueException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Invalid default value: Enum default value must be a string;"
            + " tenant=enumIngestExtractTest, signal=meal_event, preprocess=merge_restaurant,"
            + " attribute=cousin, value=123",
        exception.getMessage());
  }

  @Test
  public void testEnumLookupKeyNumEntriesMismatch() throws Exception {
    final var context2 = prepStreamCreation(2);
    context2.getAttributes().get(0).getEnum().add("Arctic");

    final var context = tenantConfig.getStreams().get(1);
    context.setVersion(timestamp + 1);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.FINAL);

    context2.setVersion(timestamp + 2);
    admin.addStream(tenantConfig.getName(), context2, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context2, RequestPhase.FINAL);

    final var signal = tenantConfig.getStreams().get(0).setVersion(timestamp + 2);
    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Constraint violation:"
            + " Type of foreignKey[0] must match the type of the context's primary key;"
            + " tenant=enumIngestExtractTest, signal=meal_event, enrichment=merge_management,"
            + " foreignKey=region, context=management, primaryKey=region,"
            + " mismatch=different number of enum entries",
        exception.getMessage());
  }

  @Test
  public void testEnumLookupKeyMismatch() throws Exception {
    final var context2 = prepStreamCreation(2);
    context2.getAttributes().get(0).getEnum().remove(2);
    context2.getAttributes().get(0).getEnum().add("Antarctic");

    final var context = tenantConfig.getStreams().get(1);
    context.setVersion(timestamp + 1);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context, RequestPhase.FINAL);

    context2.setVersion(timestamp + 2);
    admin.addStream(tenantConfig.getName(), context2, RequestPhase.INITIAL);
    admin.addStream(tenantConfig.getName(), context2, RequestPhase.FINAL);

    final var signal = tenantConfig.getStreams().get(0).setVersion(timestamp + 2);
    final var exception =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.addStream(tenantConfig.getName(), signal, RequestPhase.INITIAL));
    assertEquals(
        "Constraint violation:"
            + " Type of foreignKey[0] must match the type of the context's primary key;"
            + " tenant=enumIngestExtractTest, signal=meal_event, enrichment=merge_management,"
            + " foreignKey=region, context=management, primaryKey=region,"
            + " mismatch=enum entries differ at index 2",
        exception.getMessage());
  }

  private StreamConfig prepStreamCreation(int signalIndex) throws Exception {
    timestamp = System.currentTimeMillis();
    final var tenantConfig0 = new TenantConfig(tenantConfig.getName());
    admin.addTenant(tenantConfig0, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig0, RequestPhase.FINAL, timestamp);

    final var signal = tenantConfig.getStreams().get(signalIndex);
    signal.setVersion(timestamp + 1);
    return signal;
  }
}
