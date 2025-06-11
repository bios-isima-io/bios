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
package io.isima.bios.it;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.ActivityRecorder;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SelectOrder;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextFeatureConfig;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ContextIndexingTest {

  private static final String TENANT_NAME = "contextIndexingTest";

  private static Auth auth;
  private static Admin admin;
  private static AdminInternal tfosAdmin;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection connection;
  private static AdminServiceHandler adminHandler;
  private static DataServiceHandler dataServiceHandler;
  private static HttpClientManager clientManager;

  @Rule public TestName name = new TestName();

  private static void startUpBios() throws Exception {
    Bios2TestModules.startModules(
        true, ContextIndexingMdTest.class, Map.of("prop.featureWorkerInterval", "2000"));
    auth = BiosModules.getAuth();
    admin = BiosModules.getAdmin();
    connection = BiosModules.getCassandraConnection();
    tfosAdmin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    adminHandler = BiosModules.getAdminServiceHandler();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    clientManager = BiosModules.getHttpClientManager();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    startUpBios();

    final var tenantConfig = new TenantConfig(TENANT_NAME);
    long timestamp = System.currentTimeMillis();
    try {
      admin.deleteTenant(TENANT_NAME, RequestPhase.INITIAL, timestamp);
      admin.deleteTenant(TENANT_NAME, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ignore
    }
    timestamp = System.currentTimeMillis();
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @After
  public void tearDown() {
    ((DataEngineImpl) BiosModules.getDataEngine()).getMaintenance().setActivityRecorder(null);
  }

  @Test
  public void testReloading() throws Exception {
    final var contextName = "reloadingTest";
    final var context = makeTestContextSdEnumKey(contextName);
    executeReloadingTest(contextName, context);
  }

  @Test
  public void testReloadingEnumKey() throws Exception {
    final var contextName = "reloadingTestEnum";
    final var context = makeTestContextSdEnumKey(contextName);
    executeReloadingTest(contextName, context);
  }

  private void executeReloadingTest(String contextName, ContextConfig context) throws Exception {
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    System.out.println("... Upserting initial entries");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of(
            "one,1000,a____",
            "two,2000,b____",
            "three,3000,c____",
            "four,4000,d____",
            "five,5000,e____"));

    sleep(20);

    System.out.println("... Verifying the index #1");
    final var contextDesc = tfosAdmin.getStream(TENANT_NAME, contextName);
    {
      final var request = new SelectContextRequest();
      request.setContext(contextName);
      request.setWhere("intValue > 2000");
      request.setOrderBy(new SelectOrder("intValue", false, false));
      final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
      state.setContextDesc(contextDesc);
      final var response =
          dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
      assertThat(response.getEntries().size(), is(3));
      assertThat(response.getEntries().get(0).get("stringValue"), is("c____"));
      assertThat(response.getEntries().get(1).get("stringValue"), is("d____"));
      assertThat(response.getEntries().get(2).get("stringValue"), is("e____"));
    }

    System.out.println("... updating, and deleting entries");
    TestUtils.upsertContextEntries(
        dataServiceHandler, clientManager, TENANT_NAME, contextName, List.of("one,1000,aaaaa"));
    TestUtils.deleteContextEntries(
        dataServiceHandler, clientManager, TENANT_NAME, contextName, List.of(List.of("two")));

    sleep(20);

    System.out.println("... Verifying the index #2");
    {
      final var request = new SelectContextRequest();
      request.setContext(contextName);
      request.setWhere("intValue <= 3000");
      request.setOrderBy(new SelectOrder("intValue", false, false));
      final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
      state.setContextDesc(contextDesc);
      final var response =
          dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
      assertThat(response.getEntries().size(), is(2));
      assertThat(response.getEntries().get(0).get("stringValue"), is("aaaaa"));
      assertThat(response.getEntries().get(1).get("stringValue"), is("c____"));
    }

    System.out.println("... Reloading Admin");
    Bios2TestModules.shutdown();
    startUpBios();
    final var activityRecorder = new ActivityRecorder();
    ((DataEngineImpl) BiosModules.getDataEngine())
        .getMaintenance()
        .setActivityRecorder(activityRecorder);
    final var startTime = System.currentTimeMillis();

    System.out.println("... Upserting and deleting entries");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of("four,4000,d____", "eight,8000,hhhhh"),
        activityRecorder);
    TestUtils.deleteContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of(List.of("five")),
        activityRecorder);

    sleep(30);

    System.out.println("... Verifying the index #3");
    try {
      final var request = new SelectContextRequest();
      request.setContext(contextName);
      request.setWhere("intValue >= 3000");
      request.setOrderBy(new SelectOrder("intValue", false, false));
      final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
      state.setContextDesc(contextDesc);
      final var response =
          dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
      System.out.println("select   >= 3000 : " + response.getEntries());
      assertThat(response.getEntries().size(), is(3));
      assertThat(response.getEntries().get(0).get("key"), is("three"));
      assertThat(response.getEntries().get(0).get("stringValue"), is("c____"));
      assertThat(response.getEntries().get(1).get("key"), is("four"));
      assertThat(response.getEntries().get(1).get("stringValue"), is("d____"));
      assertThat(response.getEntries().get(2).get("key"), is("eight"));
      assertThat(response.getEntries().get(2).get("stringValue"), is("hhhhh"));
    } catch (Throwable t) {
      dumpDebugData(name.getMethodName(), contextDesc, activityRecorder, startTime);
      throw t;
    }
  }

  @Test
  public void testComplexMods() throws Exception {
    final var activityRecorder = new ActivityRecorder();
    ((DataEngineImpl) BiosModules.getDataEngine())
        .getMaintenance()
        .setActivityRecorder(activityRecorder);
    final var contextName = "modifyIndexKeys";
    final var context = makeTestContext(contextName);

    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    final var startTime = System.currentTimeMillis();

    System.out.println("... Upserting initial entries");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of(
            "one,1000,aaaaa",
            "two,2000,bbbbb",
            "three,3000,ccccc",
            "four,4000,ddddd",
            "five,5000,eeeee"),
        activityRecorder);

    sleep(20);

    System.out.println("... Verifying the index #1");
    final var contextDesc = tfosAdmin.getStream(TENANT_NAME, contextName);
    {
      final var request = new SelectContextRequest();
      request.setContext(contextName);
      request.setWhere("intValue > 2000");
      request.setOrderBy(new SelectOrder("intValue", false, false));
      final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
      state.setContextDesc(contextDesc);
      final var response =
          dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
      assertThat(response.getEntries().size(), is(3));
      assertThat(response.getEntries().get(0).get("stringValue"), is("ccccc"));
      assertThat(response.getEntries().get(1).get("stringValue"), is("ddddd"));
      assertThat(response.getEntries().get(2).get("stringValue"), is("eeeee"));
    }

    System.out.println("... Overwriting, updating, and deleting entries multiple times");
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of("one,800,AAAAA", "two,2000,BBBBB", "six,600,fffff", "three,3100,ccccc"),
        activityRecorder);
    TestUtils.deleteContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of(List.of("two"), List.of("six"), List.of("three"), List.of("four")),
        activityRecorder);
    TestUtils.upsertContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of("four,3900,DDDDD"),
        activityRecorder);

    sleep(20);

    System.out.println("... Verifying the index #2");
    try {
      final var request = new SelectContextRequest();
      request.setContext(contextName);
      request.setWhere("intValue <= 4000");
      request.setOrderBy(new SelectOrder("intValue", false, false));
      final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
      state.setContextDesc(contextDesc);
      final var response =
          dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
      System.out.println("select   <= 4000 : " + response.getEntries());
      assertThat(response.getEntries().size(), is(2));
      assertThat(response.getEntries().get(0).get("key"), is("one"));
      assertThat(response.getEntries().get(0).get("intValue"), is(800L));
      assertThat(response.getEntries().get(0).get("stringValue"), is("AAAAA"));
      assertThat(response.getEntries().get(1).get("key"), is("four"));
      assertThat(response.getEntries().get(1).get("intValue"), is(3900L));
      assertThat(response.getEntries().get(1).get("stringValue"), is("DDDDD"));

    } catch (Throwable t) {
      dumpDebugData(name.getMethodName(), contextDesc, activityRecorder, startTime);
      throw t;
    }
  }

  @Ignore("BIOS-6009, flaky")
  @Test
  public void testManyWrites() throws Exception {
    final var activityRecorder = new ActivityRecorder();
    ((DataEngineImpl) BiosModules.getDataEngine())
        .getMaintenance()
        .setActivityRecorder(activityRecorder);
    final var contextName = "manyEntryWrites";
    final var context = makeTestContext(contextName);
    final var features = context.getFeatures();
    context.setFeatures(null);

    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    System.out.println("Starting write operations");

    final Map<String, Map<String, Object>> entries = new ConcurrentHashMap<>();
    final var random = new Random();
    long base = random.nextLong();
    final var stopped = new AtomicBoolean(false);
    final long maxNumEntries = 100000;
    final var future =
        CompletableFuture.runAsync(
            () -> {
              try {
                while (!stopped.get()) {
                  final var events = new ArrayList<String>();
                  final var usedKey = new HashSet<String>();
                  for (int i = 0; i < 20; ++i) {
                    String key =
                        String.format(
                            "%d.%04d",
                            base * maxNumEntries, (long) (random.nextDouble() * maxNumEntries));
                    if (usedKey.contains(key)) {
                      continue;
                    }
                    usedKey.add(key);
                    long intValue = random.nextLong();
                    String stringValue = Long.toString(random.nextLong());
                    events.add(String.format("%s,%d,%s", key, intValue, stringValue));
                    entries.put(
                        key, Map.of("key", key, "intValue", intValue, "stringValue", stringValue));
                  }
                  TestUtils.upsertContextEntries(
                      dataServiceHandler, clientManager, TENANT_NAME, contextName, events);

                  final List<Object> allKeys = new ArrayList<>(entries.keySet());
                  Collections.shuffle(allKeys);
                  final List<List<Object>> toDelete =
                      allKeys.subList(0, 5).stream()
                          .map((key) -> List.of(key))
                          .collect(Collectors.toList());
                  TestUtils.deleteContextEntries(
                      dataServiceHandler, clientManager, TENANT_NAME, contextName, toDelete);

                  toDelete.forEach((key) -> entries.remove(key));

                  Thread.sleep(5);
                }
              } catch (TfosException | ApplicationException | InterruptedException e) {
                e.printStackTrace();
                throw new CompletionException(e);
              }
            },
            Executors.newSingleThreadExecutor());

    sleep(10);

    context.setFeatures(features);
    AdminTestUtils.updateContext(auth, adminHandler, TENANT_NAME, contextName, context);

    sleep(20);
    stopped.set(true);
    future.get();
    System.out.println("Write operations stopped");
    sleep(20);

    final var filtered =
        entries.entrySet().stream()
            .filter((entry) -> (Long) entry.getValue().get("intValue") > 0)
            .collect(Collectors.toMap((entry) -> entry.getKey(), (entry) -> entry.getValue()));

    System.out.println("entries: " + entries.size());
    System.out.println("filtered: " + filtered.size());

    final var request = new SelectContextRequest();
    request.setContext(contextName);
    request.setWhere("intValue > 0");
    request.setOrderBy(new SelectOrder("intValue", false, false));
    final var fetchState = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
    final var contextDesc = tfosAdmin.getStream(TENANT_NAME, contextName);
    fetchState.setContextDesc(contextDesc);
    final var response =
        dataEngine.selectContextEntriesAsync(request, fetchState).toCompletableFuture().get();

    System.out.println("response: " + response.getEntries().size());

    assertThat(response.getEntries().size(), is(filtered.size()));
    for (int i = 0; i < response.getEntries().size(); ++i) {
      final var retrievedEntry = response.getEntries().get(i);
      final var origEntry = filtered.get(retrievedEntry.get("key"));
      String msg = Integer.toString(i);
      assertThat(origEntry, notNullValue());
      assertThat(msg, retrievedEntry.get("intValue"), equalTo(origEntry.get("intValue")));
      assertThat(msg, retrievedEntry.get("stringValue"), equalTo(origEntry.get("stringValue")));
    }

    request.setWhere(null);
    var response2 =
        dataEngine.selectContextEntriesAsync(request, fetchState).toCompletableFuture().get();

    System.out.println("response2: " + response2.getEntries().size());

    assertThat(response2.getEntries().size(), is(entries.size()));
    for (int i = 0; i < response2.getEntries().size(); ++i) {
      final var retrievedEntry = response2.getEntries().get(i);
      final var origEntry = entries.get(retrievedEntry.get("key"));
      String msg = Integer.toString(i);
      assertThat(origEntry, notNullValue());
      assertThat(msg, retrievedEntry.get("intValue"), equalTo(origEntry.get("intValue")));
      assertThat(msg, retrievedEntry.get("stringValue"), equalTo(origEntry.get("stringValue")));
    }
  }

  // Utilities ///////////////////////////////////////////////////////////////////////

  private ContextConfig makeTestContext(String contextName) {
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("key"));
    final var featureConfig = new ContextFeatureConfig();
    featureConfig.setName("byIntValue");
    featureConfig.setDimensions(List.of("intValue"));
    featureConfig.setAttributes(List.of("stringValue"));
    featureConfig.setFeatureInterval(5000L);
    featureConfig.setIndexed(Boolean.TRUE);
    featureConfig.setIndexType(IndexType.RANGE_QUERY);
    context.setFeatures(List.of(featureConfig));
    context.setAuditEnabled(true);
    return context;
  }

  private ContextConfig makeTestContextSdEnumKey(String contextName) {
    final var context = makeTestContext(contextName);
    context
        .getAttributes()
        .get(0)
        .setAllowedValues(
            List.of(
                new AttributeValueGeneric("one", AttributeType.STRING),
                new AttributeValueGeneric("two", AttributeType.STRING),
                new AttributeValueGeneric("three", AttributeType.STRING),
                new AttributeValueGeneric("four", AttributeType.STRING),
                new AttributeValueGeneric("five", AttributeType.STRING),
                new AttributeValueGeneric("six", AttributeType.STRING),
                new AttributeValueGeneric("seven", AttributeType.STRING),
                new AttributeValueGeneric("eight", AttributeType.STRING),
                new AttributeValueGeneric("nine", AttributeType.STRING),
                new AttributeValueGeneric("ten", AttributeType.STRING)));
    return context;
  }

  private void sleep(long seconds) throws InterruptedException {
    System.out.println(
        String.format("... Sleeping for %d seconds to wait for the indexes created", seconds));
    Thread.sleep(seconds * 1000);
  }

  private void dumpDebugData(
      String testName, StreamDesc contextDesc, ActivityRecorder activityRecorder, long startTime)
      throws Exception {
    final var contextName = contextDesc.getName();
    System.out.println(String.format("== TEST FAILED: %s ===============", testName));
    final var request = new SelectContextRequest();
    request.setContext(contextName);
    request.setWhere("intValue < 1000000000");
    request.setOrderBy(new SelectOrder("intValue", false, false));
    final var state = new ContextOpState("fetchContext", Executors.newSingleThreadExecutor());
    state.setContextDesc(contextDesc);
    final var response =
        dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
    System.out.println("select all (idx) : " + response.getEntries());
    request.setWhere(null);
    final var response2 =
        dataEngine.selectContextEntriesAsync(request, state).toCompletableFuture().get();
    System.out.println("select all       : " + response2.getEntries());
    System.out.println("\nActivity dump:");
    System.out.println(activityRecorder.dump());
    System.out.println("=====================================");

    final var extractRequest =
        new ExtractRequest().setStartTime(startTime).setEndTime(System.currentTimeMillis());
    final var auditSignalName = StringUtils.prefixToCamelCase("audit", contextName);
    final var auditEvents =
        TestUtils.extract(dataServiceHandler, TENANT_NAME, auditSignalName, extractRequest);
    System.out.println(String.format("auditEvents: %s", auditEvents));
  }
}
