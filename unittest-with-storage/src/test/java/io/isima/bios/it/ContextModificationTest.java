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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.TenantAppendixImpl;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.audit.AuditManagerImpl;
import io.isima.bios.auth.AuthImpl;
import io.isima.bios.auth.v1.impl.AuthV1Impl;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.Event;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.auth.GroupRepository;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextModificationTest {

  private static final String TENANT_NAME = "contextModificationTest";

  private static Admin admin;
  private static CassandraConnection connection;
  private static AdminServiceHandler adminHandler;
  private static DataServiceHandler dataServiceHandler;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(ContextModificationTest.class);
    admin = BiosModules.getAdmin();
    connection = BiosModules.getCassandraConnection();
    adminHandler = BiosModules.getAdminServiceHandler();
    dataServiceHandler = BiosModules.getDataServiceHandler();

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

    // make the pseudo session token used for running API operations
    final var userContext = new UserContext();
    userContext.setTenant(TENANT_NAME);
    userContext.setScope("/tenants/" + TENANT_NAME + "/");
    userContext.addPermissions(List.of(Permission.ADMIN));
    userContext.setAppType(AppType.ADHOC);
    userContext.setAppName("contextModTest");
    final long now = System.currentTimeMillis();
    final long expiry = now + 3000000000L;
    sessionToken =
        new SessionToken(BiosModules.getAuth().createToken(now, expiry, userContext), false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Test
  public void testBasic() throws Exception {
    final var contextName = "basicTest";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("key"));
    context.setAuditEnabled(false);

    final FanRouter<ContextConfig, ContextConfig> fanRouter = makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            context,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextName),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    upsertContext(
        BiosModules.getDataServiceHandler(),
        sessionToken,
        TENANT_NAME,
        contextName,
        List.of(
            "one,1000,a____",
            "two,2000,b____",
            "three,3000,c____",
            "four,4000,d____",
            "five,5000,e____"));

    {
      final var result =
          getContextEntries(
              dataServiceHandler,
              TENANT_NAME,
              contextName,
              List.of("one", "two", "three", "four", "five"));

      assertThat(result.size(), is(5));
      assertThat(result.get(0).get("intValue"), is(1000L));
      assertThat(result.get(1).get("intValue"), is(2000L));
      assertThat(result.get(2).get("intValue"), is(3000L));
      assertThat(result.get(3).get("intValue"), is(4000L));
      assertThat(result.get(4).get("intValue"), is(5000L));
    }

    // modify context #1, add an attribute and enable audit
    final var context2 = new ContextConfig(context);
    final var newAttr2 = new AttributeConfig("boolValue", AttributeType.BOOLEAN);
    newAttr2.setDefaultValue(new AttributeValueGeneric(FALSE, AttributeType.BOOLEAN));
    final var newAttrs2 = new ArrayList<>(context2.getAttributes());
    newAttrs2.add(newAttr2);
    context2.setAttributes(newAttrs2);
    context2.setAuditEnabled(true);
    timestamp = System.currentTimeMillis();
    adminHandler
        .updateContext(
            sessionToken,
            TENANT_NAME,
            contextName,
            context2,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextName),
            Optional.of(auditFanRouter),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    {
      upsertContext(
          BiosModules.getDataServiceHandler(),
          sessionToken,
          TENANT_NAME,
          contextName,
          List.of("six,6001,ff___,true", "three,3001,cc___,true", "five,5001,ee___,false"));

      final var result =
          getContextEntries(
              dataServiceHandler, TENANT_NAME, contextName, List.of("one", "three", "five", "six"));

      assertThat(result.size(), is(4));
      assertThat(result.get(0).get("intValue"), is(1000L));
      assertThat(result.get(0).get("boolValue"), is(FALSE));
      assertThat(result.get(1).get("intValue"), is(3001L));
      assertThat(result.get(1).get("boolValue"), is(TRUE));
      assertThat(result.get(2).get("intValue"), is(5001L));
      assertThat(result.get(2).get("boolValue"), is(FALSE));
      assertThat(result.get(3).get("intValue"), is(6001L));
      assertThat(result.get(3).get("boolValue"), is(TRUE));
    }

    // modify context #2, add another attribute
    final var context3 = new ContextConfig(context2);
    final var newAttr3 = new AttributeConfig("enumValue", AttributeType.STRING);
    newAttr3.setAllowedValues(
        List.of(
            new AttributeValueGeneric("A_", AttributeType.STRING),
            new AttributeValueGeneric("AA", AttributeType.STRING),
            new AttributeValueGeneric("B_", AttributeType.STRING),
            new AttributeValueGeneric("BB", AttributeType.STRING),
            new AttributeValueGeneric("C_", AttributeType.STRING),
            new AttributeValueGeneric("CC", AttributeType.STRING),
            new AttributeValueGeneric("D_", AttributeType.STRING),
            new AttributeValueGeneric("DD", AttributeType.STRING),
            new AttributeValueGeneric("E_", AttributeType.STRING),
            new AttributeValueGeneric("EE", AttributeType.STRING),
            new AttributeValueGeneric("F_", AttributeType.STRING),
            new AttributeValueGeneric("FF", AttributeType.STRING),
            new AttributeValueGeneric("G_", AttributeType.STRING),
            new AttributeValueGeneric("GG", AttributeType.STRING),
            new AttributeValueGeneric("H_", AttributeType.STRING),
            new AttributeValueGeneric("HH", AttributeType.STRING),
            new AttributeValueGeneric("ZZZ", AttributeType.STRING)));
    newAttr3.setDefaultValue(new AttributeValueGeneric("ZZZ", AttributeType.STRING));
    final var newAttrs3 = new ArrayList<>(context2.getAttributes());
    newAttrs3.add(newAttr3);
    context3.setAttributes(newAttrs3);
    context3.setAuditEnabled(true);
    timestamp = System.currentTimeMillis();
    BiosModules.getAdminServiceHandler()
        .updateContext(
            sessionToken,
            TENANT_NAME,
            contextName,
            context3,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextName),
            Optional.of(auditFanRouter),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();
    {
      final var result =
          getContextEntries(
              dataServiceHandler, TENANT_NAME, contextName, List.of("one", "three", "five", "six"));

      assertThat(result.size(), is(4));
      assertThat(result.get(0).get("intValue"), is(1000L));
      assertThat(result.get(0).get("boolValue"), is(FALSE));
      assertThat(result.get(0).get("enumValue"), is("ZZZ"));
      assertThat(result.get(1).get("intValue"), is(3001L));
      assertThat(result.get(1).get("boolValue"), is(TRUE));
      assertThat(result.get(1).get("enumValue"), is("ZZZ"));
      assertThat(result.get(2).get("intValue"), is(5001L));
      assertThat(result.get(2).get("boolValue"), is(FALSE));
      assertThat(result.get(2).get("enumValue"), is("ZZZ"));
      assertThat(result.get(3).get("intValue"), is(6001L));
      assertThat(result.get(3).get("boolValue"), is(TRUE));
      assertThat(result.get(3).get("enumValue"), is("ZZZ"));

      final long start = System.currentTimeMillis();
      upsertContext(
          BiosModules.getDataServiceHandler(),
          sessionToken,
          TENANT_NAME,
          contextName,
          List.of(
              "one,1003,aaaa_,true,A_", "three,3002,ccc__,false,ZZZ", "seven,7003,gggg_,true,G_"));

      final var result2 =
          getContextEntries(
              dataServiceHandler, TENANT_NAME, contextName, List.of("one", "three", "seven"));
      assertThat(result2.size(), is(3));
      assertThat(result2.get(0).get("intValue"), is(1003L));
      assertThat(result2.get(0).get("boolValue"), is(TRUE));
      assertThat(result2.get(0).get("enumValue"), is("A_"));
      assertThat(result2.get(1).get("intValue"), is(3002L));
      assertThat(result2.get(1).get("boolValue"), is(FALSE));
      assertThat(result2.get(1).get("enumValue"), is("ZZZ"));
      assertThat(result2.get(2).get("intValue"), is(7003L));
      assertThat(result2.get(2).get("boolValue"), is(TRUE));
      assertThat(result2.get(2).get("enumValue"), is("G_"));
    }

    // reload and upsert
    final var sharedProperties2 = new SharedPropertiesImpl(connection);
    SharedProperties.setInstance(sharedProperties2);

    upsertContext(
        BiosModules.getDataServiceHandler(),
        sessionToken,
        TENANT_NAME,
        contextName,
        List.of("two,2004,bbbbb,true,BB", "four,4000,d____,false,ZZZ", "eight,8004,hhhhh,true,HH"));
    {
      final var result =
          getContextEntries(
              dataServiceHandler,
              TENANT_NAME,
              contextName,
              List.of("one", "two", "three", "four", "five", "six", "seven", "eight"));
      assertThat(result.size(), is(8));
      final Object[][] expectedValues =
          new Object[][] {
            {1003L, "aaaa_", TRUE, "A_"},
            {2004L, "bbbbb", TRUE, "BB"},
            {3002L, "ccc__", FALSE, "ZZZ"},
            {4000L, "d____", FALSE, "ZZZ"},
            {5001L, "ee___", FALSE, "ZZZ"},
            {6001L, "ff___", TRUE, "ZZZ"},
            {7003L, "gggg_", TRUE, "G_"},
            {8004L, "hhhhh", TRUE, "HH"},
          };
      final var columns = List.of("intValue", "stringValue", "boolValue", "enumValue");
      for (int irow = 0; irow < 8; ++irow) {
        for (int icol = 0; icol < columns.size(); ++icol) {
          final var columnName = columns.get(icol);
          assertThat(result.get(irow).get(columnName), is(expectedValues[irow][icol]));
        }
      }
    }
  }

  // BIOS-184 Regression
  @Test
  public void testChangeContextNameCases() throws Exception {
    final var contextName = "signin";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("key"));

    final FanRouter<ContextConfig, ContextConfig> fanRouter = makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            context,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextName),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    // modify context -- change cases of the context name
    final var context2 = new ContextConfig(context);
    final var anotherAttribute = new AttributeConfig("anotherString", AttributeType.STRING);
    anotherAttribute.setDefaultValue(new AttributeValueGeneric("MISSING", AttributeType.STRING));
    final var modifiedAttributes = new ArrayList<>(context2.getAttributes());
    modifiedAttributes.add(anotherAttribute);
    context2.setAttributes(modifiedAttributes);
    context2.setName("signIn");
    timestamp = System.currentTimeMillis();
    final var updatedContext =
        adminHandler
            .updateContext(
                sessionToken,
                TENANT_NAME,
                contextName,
                context2,
                RequestPhase.INITIAL,
                timestamp,
                Optional.of(fanRouter),
                StringUtils.prefixToCamelCase("audit", contextName),
                Optional.of(auditFanRouter),
                Optional.of(auditFanRouter),
                TestUtils.makeGenericState())
            .get();

    // delete context
    final FanRouter<Void, Void> deleteFanRouter = makeDummyFanRouter();
    final var state =
        new GenericExecutionState("contextModTest", Executors.newSingleThreadExecutor());
    adminHandler.deleteContext(
        sessionToken,
        TENANT_NAME,
        contextName,
        RequestPhase.INITIAL,
        System.currentTimeMillis(),
        Optional.of(deleteFanRouter),
        state);

    // reload and verify the updated context
    final var adminStore2 = new AdminStoreImpl(connection);
    final var dataEngine2 = new DataEngineImpl(connection);
    final var tfosAdmin2 = new AdminImpl(adminStore2, null, dataEngine2);
    final var admin2 = new io.isima.bios.admin.AdminImpl(tfosAdmin2, null, null);
    final var auditManager2 = new AuditManagerImpl(dataEngine2);
    final var sharedProperties2 = new SharedPropertiesImpl(connection);
    SharedProperties.setInstance(sharedProperties2);
    final var sharedConfig2 = new SharedConfig(sharedProperties2);
    final var orep2 = new OrganizationRepository(connection);
    final var urep2 = new UserRepository(connection);
    final var grep2 = new GroupRepository(connection);
    final var authV12 = new AuthV1Impl(orep2, urep2, grep2, sharedConfig2);
    final var auth2 = new AuthImpl(authV12, sharedConfig2);
    final var tenantAppendix2 = new TenantAppendixImpl(connection);
    final var metrics2 = new OperationMetrics(dataEngine2);

    final var adminHandler2 =
        new AdminServiceHandler(
            auditManager2,
            admin2,
            tfosAdmin2,
            tenantAppendix2,
            auth2,
            dataEngine2,
            metrics2,
            sharedConfig2,
            null,
            null,
            null);

    assertThrows(
        NoSuchStreamException.class,
        () ->
            DataUtils.wait(
                adminHandler2.getContexts(
                    sessionToken, TENANT_NAME, List.of(contextName), true, false, false, state),
                "getContexts"));
  }

  private void upsertContext(
      DataServiceHandler handler,
      SessionToken sessionToken,
      String tenantName,
      String contextName,
      List<String> entries)
      throws Exception {

    final var req = new PutContextEntriesRequest();
    req.setContentRepresentation(ContentRepresentation.CSV);
    req.setEntries(entries);

    final var state =
        new ContextWriteOpState<PutContextEntriesRequest, List<Event>>(
            "putContextEntries", ExecutorManager.getSidelineExecutor());
    state.setInitialParams(
        tenantName, contextName, null, req, RequestPhase.INITIAL, System.currentTimeMillis());

    final FanRouter<PutContextEntriesRequest, Void> fanRouter = makeDummyFanRouter();

    handler.upsertContextEntries(sessionToken, null, state, fanRouter).get();
  }

  private List<Event> getContextEntries(
      DataServiceHandler handler, String tenantName, String contextName, List<Object> keys)
      throws Exception {
    final var primaryKeys = keys.stream().map((key) -> List.of(key)).collect(Collectors.toList());
    return TestUtils.getContextEntries(handler, tenantName, contextName, primaryKeys);
  }

  private <RequestT, ReplyT> FanRouter<RequestT, ReplyT> makeDummyFanRouter() {
    return new FanRouter<>() {
      @Override
      public void addParam(String name, String value) {}

      @Override
      public List<ReplyT> fanRoute(RequestT request, Long timestamp) {
        return List.of();
      }

      @Override
      public CompletionStage<List<ReplyT>> fanRouteAsync(
          RequestT request, Long timestamp, ExecutionState state) {
        return CompletableFuture.completedStage(List.of());
      }
    };
  }
}
