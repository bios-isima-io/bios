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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminOpReplayer;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichedAttribute;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextAdminTest {

  private static final String TENANT_NAME = "contextAdminTest";

  private static Auth auth;
  private static Admin admin;
  private static AdminInternal tfosAdmin;
  private static CassandraConnection connection;
  private static AdminServiceHandler adminHandler;
  private static DataServiceHandler dataHandler;
  private static HttpClientManager clientManager;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(ContextAdminTest.class);
    admin = BiosModules.getAdmin();
    tfosAdmin = BiosModules.getAdminInternal();
    connection = BiosModules.getCassandraConnection();
    auth = BiosModules.getAuth();
    adminHandler = BiosModules.getAdminServiceHandler();
    dataHandler = BiosModules.getDataServiceHandler();
    clientManager = BiosModules.getHttpClientManager();

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
  public void testCreateDeleteWithCompositeKey() throws Exception {
    final var contextName = "simple";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        Arrays.asList(
            new AttributeConfig("stringKey", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("stringKey", "intKey"));

    testSimpleContextCreationDeletion(context);
  }

  @Test
  public void testCreateDeleteWithCompositeKeyNotAtBeginning() throws Exception {
    final var contextName = "reverseOrder";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        Arrays.asList(
            new AttributeConfig("stringValue", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("stringKey", AttributeType.STRING)));
    context.setPrimaryKey(List.of("stringKey", "intKey"));

    testSimpleContextCreationDeletion(context);
  }

  private void testSimpleContextCreationDeletion(ContextConfig context) throws Exception {
    final var contextName = context.getName();
    final var state = TestUtils.makeGenericState();

    // create context
    final FanRouter<ContextConfig, ContextConfig> contextFanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> signalFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            context,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(contextFanRouter),
            "auditSimple",
            Optional.of(signalFanRouter),
            state)
        .get();

    // get context
    {
      final List<ContextConfig> contexts =
          adminHandler
              .getContexts(
                  sessionToken, TENANT_NAME, List.of(contextName), true, false, false, state)
              .get();
      assertThat(contexts.size(), is(1));
      assertContextEquals(contexts.get(0), context);
    }

    // upsert, get, and delete
    {
      final List<Map<String, Object>> entries =
          List.of(
              Map.of("stringKey", "one", "intKey", 1, "stringValue", "ichi"),
              Map.of("stringKey", "two", "intKey", 2, "stringValue", "ni"));
      TestUtils.upsertContextEntries(
          dataHandler, clientManager, TENANT_NAME, contextName, makeCsv(context, entries));

      verifySimpleContextEntries(dataHandler, contextName);
    }

    // reload and check
    {
      final var modules2 = TestModules.reload(ContextAdminTest.class + "rl1", connection);
      final List<ContextConfig> contexts =
          modules2
              .getAdminServiceHandler()
              .getContexts(
                  sessionToken, TENANT_NAME, List.of(contextName), true, false, false, state)
              .get();
      assertThat(contexts.size(), is(1));
      assertContextEquals(contexts.get(0), context);

      verifySimpleContextEntries(modules2.getDataServiceHandler(), contextName);
    }
    // test context entry deletion
    {
      TestUtils.deleteContextEntries(
          dataHandler,
          clientManager,
          TENANT_NAME,
          contextName,
          List.of(List.of("one", 1), List.of("two", 2)));
      final var events3 =
          TestUtils.getContextEntries(
              dataHandler, TENANT_NAME, contextName, List.of(List.of("one", 1), List.of("two", 2)));
      assertThat(events3.size(), is(0));
    }
    // reload and check again
    {
      final var modules3 = TestModules.reload(ContextAdminTest.class + "rl2", connection);
      final var events3 =
          TestUtils.getContextEntries(
              modules3.getDataServiceHandler(),
              TENANT_NAME,
              contextName,
              List.of(List.of("one", 1), List.of("two", 2)));
      assertThat(events3.size(), is(0));
    }

    // delete context
    final FanRouter<Void, Void> deleteFanRouter = TestUtils.makeDummyFanRouter();
    adminHandler
        .deleteContext(
            sessionToken,
            TENANT_NAME,
            contextName,
            RequestPhase.INITIAL,
            System.currentTimeMillis(),
            Optional.of(deleteFanRouter),
            TestUtils.makeGenericState())
        .get();

    assertThrows(
        NoSuchStreamException.class,
        () -> {
          final var contexts =
              DataUtils.wait(
                  adminHandler.getContexts(
                      sessionToken, TENANT_NAME, List.of(contextName), true, false, false, state),
                  "getContexts");
          System.out.println(contexts);
        });

    // reload and check
    {
      final var modules4 = TestModules.reload(ContextAdminTest.class + "rl3", connection);

      assertThrows(
          NoSuchStreamException.class,
          () ->
              DataUtils.wait(
                  modules4
                      .getAdminServiceHandler()
                      .getContexts(
                          sessionToken,
                          TENANT_NAME,
                          List.of(contextName),
                          true,
                          false,
                          false,
                          state),
                  "getContexts"));
    }
  }

  private List<String> makeCsv(ContextConfig context, List<Map<String, Object>> entries) {
    final var csv = new ArrayList<String>();
    for (var entry : entries) {
      final var value = new StringJoiner(",");
      for (var attribute : context.getAttributes()) {
        value.add(entry.get(attribute.getName()).toString());
      }
      csv.add(value.toString());
    }
    return csv;
  }

  private void assertContextEquals(ContextConfig context, ContextConfig expected) {
    assertThat(context.getName(), is(expected.getName()));
    assertThat(context.getMissingAttributePolicy(), is(expected.getMissingAttributePolicy()));
    assertThat(context.getAttributes(), equalTo(expected.getAttributes()));
    assertThat(context.getPrimaryKey(), equalTo(expected.getPrimaryKey()));
    assertThat(context.getEnrichments(), equalTo(expected.getEnrichments()));
    assertThat(context.getFeatures(), equalTo(expected.getFeatures()));
  }

  private void verifySimpleContextEntries(DataServiceHandler dataHandler, String contextName)
      throws Exception {
    final var events =
        TestUtils.getContextEntries(
            dataHandler, TENANT_NAME, contextName, List.of(List.of("one", 1), List.of("two", 2)));
    assertThat(events.size(), is(2));
    final var event0 = events.get(0);
    assertThat(event0.get("stringKey"), is("one"));
    assertThat(event0.get("intKey"), is(1L));
    assertThat(event0.get("stringValue"), is("ichi"));
    final var event1 = events.get(1);
    assertThat(event1.get("stringKey"), is("two"));
    assertThat(event1.get("intKey"), is(2L));
    assertThat(event1.get("stringValue"), is("ni"));

    final var events2 =
        TestUtils.getContextEntries(
            dataHandler, TENANT_NAME, contextName, List.of(List.of("one", 2), List.of("two", 1)));
    assertThat(events2.size(), is(0));
  }

  @Test
  public void testEnrichment() throws Exception {
    final var remoteContextName = "simpleRemote";
    final var remoteContext = makeRemoteContext(remoteContextName);
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, remoteContext);

    final var contextName = "simpleEnrichment";
    final var context = makeEnrichedContext(contextName, remoteContextName);
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    verifyEnrichedContext(contextName, context);
  }

  @Test
  public void testEnrichmentWithForeignKeyInValues() throws Exception {
    final var remoteContextName = "simpleRemote2";
    final var remoteContext = makeRemoteContext(remoteContextName);
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, remoteContext);

    final var contextName = "simpleEnrichment2";
    final var context = makeEnrichedContext(contextName, remoteContextName);
    context.getEnrichments().get(0).setForeignKey(List.of("myStringValue", "myIntValue"));

    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    final var created =
        admin.getContexts(TENANT_NAME, true, true, false, List.of(contextName)).get(0);
    assertThat(created.getEnrichments(), equalTo(context.getEnrichments()));
  }

  @Test
  public void testEnrichmentCaseMismatch() throws Exception {
    final var remoteContextName = "caseMismatchRemote";
    final var remoteContext = makeRemoteContext(remoteContextName);
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, remoteContext);

    final var contextName = "caseMismatchEnrichment";
    final var origContext = makeEnrichedContext(contextName, remoteContextName);

    final var context = makeEnrichedContext(contextName, remoteContextName);
    final var enrichment = context.getEnrichments().get(0);
    enrichment.setForeignKey(List.of("MYSTRINGKEY", "MYINTKEY"));
    enrichment
        .getEnrichedAttributes()
        .get(0)
        .setValue(remoteContextName.toUpperCase() + ".stringValue");
    enrichment.getEnrichedAttributes().get(1).setValue(remoteContextName + ".INTVALUE");
    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    verifyEnrichedContext(contextName, origContext);
  }

  private void verifyEnrichedContext(String contextName, ContextConfig context) throws Exception {
    final var created =
        admin.getContexts(TENANT_NAME, true, true, false, List.of(contextName)).get(0);
    assertThat(created.getEnrichments(), equalTo(context.getEnrichments()));

    final var modules2 = TestModules.reload(ContextAdminTest.class + "rl4", connection);
    final var reloaded =
        modules2
            .getAdmin()
            .getContexts(TENANT_NAME, true, true, false, List.of(contextName))
            .get(0);
    assertThat(reloaded.getEnrichments(), equalTo(context.getEnrichments()));
  }

  private ContextConfig makeEnrichedContext(String contextName, String remoteContextName) {
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        Arrays.asList(
            new AttributeConfig("myStringKey", AttributeType.STRING),
            new AttributeConfig("myIntKey", AttributeType.INTEGER),
            new AttributeConfig("myStringValue", AttributeType.STRING),
            new AttributeConfig("myIntValue", AttributeType.INTEGER)));
    context.setPrimaryKey(List.of("myStringKey", "myIntKey"));
    final var enrichment = new EnrichmentConfigContext("enrichment");
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.FAIL_PARENT_LOOKUP);
    enrichment.setForeignKey(List.of("myStringKey", "myIntKey"));
    final var attr0 = new EnrichedAttribute();
    attr0.setValue(remoteContextName + ".stringValue");
    attr0.setAs("enrichedStringValue");
    final var attr1 = new EnrichedAttribute();
    attr1.setValue(remoteContextName + ".intValue");
    attr1.setAs("enrichedIntValue");
    enrichment.setEnrichedAttributes(List.of(attr0, attr1));
    context.setEnrichments(List.of(enrichment));

    return context;
  }

  private ContextConfig makeRemoteContext(String contextName) {
    final var remoteContext = new ContextConfig();
    remoteContext.setName(contextName);
    remoteContext.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    remoteContext.setAttributes(
        Arrays.asList(
            new AttributeConfig("stringKey", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER)));
    remoteContext.setPrimaryKey(List.of("stringKey", "intKey"));
    return remoteContext;
  }

  @Test
  public void recreateContextAsSignal() throws Exception {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    final var replayJson = classloader.getResourceAsStream("recreate-context-as-signal.json");
    final var replayer = new AdminOpReplayer(auth, tfosAdmin, adminHandler, replayJson);
    replayer.execute();

    final var modules2 = TestModules.reload(ContextAdminTest.class + "rl5", connection);

    final var tenantName = replayer.getTenantName();
    final var employee =
        modules2.getAdmin().getSignals(tenantName, true, true, false, List.of("employee"));
    assertThat(employee, notNullValue());
    assertThrows(
        NoSuchStreamException.class,
        () -> modules2.getAdmin().getContexts(tenantName, true, true, false, List.of("employee")));

    assertThrows(
        NoSuchStreamException.class,
        () -> modules2.getAdmin().getContexts(tenantName, true, true, false, List.of("test")));
    assertThrows(
        NoSuchStreamException.class,
        () -> modules2.getAdmin().getSignals(tenantName, true, true, false, List.of("test")));
  }

  @Test
  public void bios6215Regression() throws Exception {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    final var replayJson = classloader.getResourceAsStream("bios-6215-regression.json");
    final var replayer = new AdminOpReplayer(auth, tfosAdmin, adminHandler, replayJson);
    replayer.execute();

    // just check no exception is thrown while reloading
    TestModules.reload(ContextAdminTest.class + "rl6", connection);
  }
}
