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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ContextIndexingMdTest {

  private static final String TENANT_NAME = "contextIndexingMdTest";

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
            "one,111,1000,a____",
            "one,222,2000,b____",
            "three,333,3000,c____",
            "five,222,4000,d____",
            "five,555,5000,e____"));

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
        dataServiceHandler, clientManager, TENANT_NAME, contextName, List.of("one,111,1500,aaaaa"));
    TestUtils.deleteContextEntries(
        dataServiceHandler, clientManager, TENANT_NAME, contextName, List.of(List.of("one", 222)));

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
        List.of("five,222,4000,d____", "eight,888,8000,hhhhh"),
        activityRecorder);
    TestUtils.deleteContextEntries(
        dataServiceHandler,
        clientManager,
        TENANT_NAME,
        contextName,
        List.of(List.of("five", 555L)),
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
      assertThat(response.getEntries().get(0).get("intKey"), is(333L));
      assertThat(response.getEntries().get(0).get("stringValue"), is("c____"));
      assertThat(response.getEntries().get(1).get("key"), is("five"));
      assertThat(response.getEntries().get(1).get("intKey"), is(222L));
      assertThat(response.getEntries().get(1).get("stringValue"), is("d____"));
      assertThat(response.getEntries().get(2).get("key"), is("eight"));
      assertThat(response.getEntries().get(2).get("intKey"), is(888L));
      assertThat(response.getEntries().get(2).get("stringValue"), is("hhhhh"));
    } catch (Throwable t) {
      dumpDebugData(name.getMethodName(), contextDesc, activityRecorder, startTime);
      throw t;
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
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("key", "intKey"));
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
