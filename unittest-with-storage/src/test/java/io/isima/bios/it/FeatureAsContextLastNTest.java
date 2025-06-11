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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.ContextMaintenanceAction;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.LastN;
import io.isima.bios.models.LastNItem;
import io.isima.bios.models.MaterializedAs;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.handler.InsertServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test corner cases of LastN feature as context with cache only context turned on. */
public class FeatureAsContextLastNTest {

  private static String tenantName;
  private static String signalName;
  private static String contextName;

  private static SignalConfig signal;
  private static ContextConfig context;

  private static AdminInternal admin;
  private static io.isima.bios.admin.Admin biosAdmin;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection connection;
  private static DataServiceHandler dataServiceHandler;

  private static ObjectMapper mapper;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tenantName = "featureAsContextLastN";
    signalName = "visits";
    contextName = "lastVisits";

    Bios2TestModules.setProperty(TfosConfig.CONTEXT_MAINTENANCE_CLEANUP_MARGIN, "0");
    Bios2TestModules.startModules(
        true,
        FeatureAsContextLastNTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "1000",
            "prop.cacheOnlyContext",
            "featureAsContextLastN.lastVisits"));
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    connection = BiosModules.getCassandraConnection();
    biosAdmin = BiosModules.getAdmin();
    dataServiceHandler = BiosModules.getDataServiceHandler();

    final String contextSrc =
        "{"
            + "  'contextName': 'lastVisits',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'memberId', 'type': 'Integer'},"
            + "    {'attributeName': 'visits', 'type': 'String'}"
            + "  ],"
            + "  'primaryKey': ['memberId']"
            + "}";

    final String signalSrc =
        "{"
            + "  'signalName': 'visits',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'memberId', 'type': 'Integer'},"
            + "    {'attributeName': 'issueId', 'type': 'Integer'}"
            + "  ],"
            + "  'postStorageStage': {"
            + "    'features': ["
            + "      {"
            + "        'featureName': 'lastFiveVisits',"
            + "        'dimensions': ['memberId'],"
            + "        'attributes': ['issueId'],"
            + "        'materializedAs': 'LastN',"
            + "        'featureAsContextName': 'lastVisits',"
            + "        'items': 5,"
            + "        'featureInterval': 15000"
            + "      }"
            + "    ]"
            + "  }"
            + "}";

    mapper = BiosObjectMapperProvider.get();
    context = mapper.readValue(contextSrc.replace("'", "\""), ContextConfig.class);
    signal = mapper.readValue(signalSrc.replace("'", "\""), SignalConfig.class);

    final var tenant = new TenantConfig(tenantName);
    long timestamp = Long.valueOf(System.currentTimeMillis());
    try {
      biosAdmin.deleteTenant(tenant.getName(), RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteTenant(tenant.getName(), RequestPhase.FINAL, timestamp);
    } catch (Throwable t) {
      // ignore
    }
    ++timestamp;
    biosAdmin.createTenant(tenant, RequestPhase.INITIAL, timestamp, false);
    biosAdmin.createTenant(tenant, RequestPhase.FINAL, timestamp, false);

    ++timestamp;
    try {
      biosAdmin.deleteSignal(tenant.getName(), signal.getName(), RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteSignal(tenant.getName(), signal.getName(), RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    ++timestamp;
    try {
      biosAdmin.deleteContext(tenant.getName(), context.getName(), RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteContext(tenant.getName(), context.getName(), RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    ++timestamp;
    biosAdmin.createContext(tenant.getName(), context, RequestPhase.INITIAL, timestamp);
    biosAdmin.createContext(tenant.getName(), context, RequestPhase.FINAL, timestamp);
    ++timestamp;
    biosAdmin.createSignal(tenant.getName(), signal, RequestPhase.INITIAL, timestamp);
    biosAdmin.createSignal(tenant.getName(), signal, RequestPhase.FINAL, timestamp);

    // make the pseudo session token used for running API operations
    final var userContext = new UserContext();
    userContext.setTenant(tenantName);
    userContext.setScope("/tenants/" + tenantName + "/");
    userContext.addPermissions(List.of(Permission.ADMIN));
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
  public void testAdmin() throws Exception {
    verifyFeature0(biosAdmin);
  }

  @Test
  public void testBasic() throws Exception {
    // Check whether the TTL is set properly in the context
    var info = admin.getFeatureAsContextInfo(tenantName, contextName);
    assertNotNull(info);
    assertNull(info.getTtlInMillis());

    final var issues = new HashMap<Long, List<LastNItem>>();

    // populate the signals for the first time
    final var random = new Random();
    Long lastInsertTimestamp = null;
    for (int i = 0; i < 500; ++i) {
      final var memberId = Long.valueOf((long) (random.nextDouble() * 100));
      final var issueId = Long.valueOf(random.nextLong());
      Thread.sleep(1);
      long timestamp =
          insertSignal(
              BiosModules.getInsertServiceHandler(),
              sessionToken,
              signalName,
              memberId,
              issueId,
              issues);
      lastInsertTimestamp = timestamp;
    }

    long sleepTime = lastInsertTimestamp + 35000 - System.currentTimeMillis();
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    for (var entry : issues.entrySet()) {
      final var memberId = entry.getKey();
      final var issueIds = entry.getValue();

      verifyContextEntry(dataServiceHandler, contextName, memberId, issueIds);
      System.out.println("ok");
    }

    // reload and check the contexts again
    final var modules2 =
        TestModules.reload(FeatureAsContextLastNTest.class + " reload", connection);
    final var dataEngine2 = modules2.getDataEngine();
    final var admin2 = modules2.getTfosAdmin();
    final var insertServiceHandler2 =
        new InsertServiceHandler(null, BiosModules.getAuth(), admin2, dataEngine2, null);
    final var biosAdmin2 = new io.isima.bios.admin.AdminImpl(admin2, null, null);

    verifyFeature0(biosAdmin2);

    info = admin2.getFeatureAsContextInfo(tenantName, contextName);
    assertNotNull(info);
    assertNull(info.getTtlInMillis());

    for (var entry : issues.entrySet()) {
      final var memberId = entry.getKey();
      final var issueIds = entry.getValue();

      verifyContextEntry(modules2.getDataServiceHandler(), contextName, memberId, issueIds);
    }

    // insert additional signals to the reloaded data engine
    lastInsertTimestamp = null;
    for (int i = 0; i < 500; ++i) {
      final var memberId = Long.valueOf((long) (random.nextDouble() * 120));
      final var issueId = Long.valueOf(random.nextLong());

      Thread.sleep(1);
      long timestamp =
          insertSignal(insertServiceHandler2, sessionToken, signalName, memberId, issueId, issues);
      lastInsertTimestamp = timestamp;
    }

    // verify the context again using the original modules
    sleepTime = lastInsertTimestamp + 35000 - System.currentTimeMillis();
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    for (var entry : issues.entrySet()) {
      final var memberId = entry.getKey();
      final var issueIds = entry.getValue();

      verifyContextEntry(dataServiceHandler, contextName, memberId, issueIds);
    }
  }

  @Test
  public void testContextMaintenance() throws Exception {
    final String signalWithTtlName = signalName + "WithTtl";
    final String contextWithTtlName = contextName + "WithTtl";

    final var signalWithTtl = new SignalConfig(signal, false);
    signalWithTtl.setName(signalWithTtlName);
    final var feature = signalWithTtl.getPostStorageStage().getFeatures().get(0);
    feature.setFeatureAsContextName(contextWithTtlName);
    feature.setTtl(30000L);

    final var contextWithTtl = new ContextConfig(context);
    contextWithTtl.setName(contextWithTtlName);

    long timestamp = System.currentTimeMillis();
    try {
      biosAdmin.deleteSignal(tenantName, signalWithTtlName, RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteSignal(tenantName, signalWithTtlName, RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    ++timestamp;
    try {
      biosAdmin.deleteContext(tenantName, contextWithTtlName, RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteContext(tenantName, contextWithTtlName, RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    ++timestamp;
    biosAdmin.createContext(tenantName, contextWithTtl, RequestPhase.INITIAL, timestamp);
    biosAdmin.createContext(tenantName, contextWithTtl, RequestPhase.FINAL, timestamp);
    ++timestamp;
    biosAdmin.createSignal(tenantName, signalWithTtl, RequestPhase.INITIAL, timestamp);
    biosAdmin.createSignal(tenantName, signalWithTtl, RequestPhase.FINAL, timestamp);

    // Verify that the TTL appears in the context desc
    var info = admin.getFeatureAsContextInfo(tenantName, contextWithTtlName);
    assertNotNull(info);
    assertEquals(Long.valueOf(30000), info.getTtlInMillis());

    long now = System.currentTimeMillis();
    // Start the whole process at 7 seconds past the feature interval time.
    Thread.sleep(Utils.ceiling(now, 15000) + 7000 - now);

    final var issues = new HashMap<Long, List<LastNItem>>();

    List<Long> memberIds = List.of(10L, 20L);
    long start = System.currentTimeMillis();
    for (int i = 0; i < memberIds.size(); ++i) {
      final var memberId = memberIds.get(i);
      final var issueId = Long.valueOf(i);
      insertSignal(
          BiosModules.getInsertServiceHandler(),
          sessionToken,
          signalWithTtlName,
          memberId,
          issueId,
          issues);
    }

    Thread.sleep(start + 15000 - System.currentTimeMillis());
    insertSignal(
        BiosModules.getInsertServiceHandler(),
        sessionToken,
        signalWithTtlName,
        memberIds.get(0),
        11L,
        issues);

    Thread.sleep(start + 30000 - System.currentTimeMillis());
    insertSignal(
        BiosModules.getInsertServiceHandler(),
        sessionToken,
        signalWithTtlName,
        memberIds.get(0),
        12L,
        issues);

    // Wait until the first set of inserts get TTL'ed out. The context entry gets written
    // during feature computation, which happens about 5 seconds after the interval,
    // so the first set of context entries were written at about start + 13 seconds.
    // So wait until start + 13 + 30 + margin of 4 seconds
    // Add a margin to allow for feature
    // computation to finish.
    long sleepTime = start + 47000 - System.currentTimeMillis();
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    System.out.println("Running context maintenance");
    dataEngine.maintainContext(
        ContextMaintenanceAction.CLEANUP, tenantName, contextWithTtlName, null, null);
    System.out.println("Returned from the context maintenance");

    // Allow some time for context maintenance to finish.
    Thread.sleep(4000);

    final var issueIds = issues.get(memberIds.get(0));
    verifyContextEntry(dataServiceHandler, contextWithTtlName, memberIds.get(1), null);
    verifyContextEntry(
        dataServiceHandler,
        contextWithTtlName,
        memberIds.get(0),
        issueIds.subList(1, issueIds.size()));

    // Test modifying and deleting signal with LastN feature //////////////////
    // TODO(Naoki): hard to debug, move to an independent test case

    // changing TTL
    signalWithTtl.getPostStorageStage().getFeatures().get(0).setTtl(3600000L);
    timestamp = System.currentTimeMillis();
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.INITIAL, timestamp);
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.FINAL, timestamp);

    info = admin.getFeatureAsContextInfo(tenantName, contextWithTtlName);
    assertNotNull(info);
    assertEquals(Long.valueOf(3600000L), info.getTtlInMillis());

    // should fail: changing target context to already-referred one
    signalWithTtl.getPostStorageStage().getFeatures().get(0).setFeatureAsContextName(contextName);
    assertThrows(
        ConstraintViolationException.class,
        () -> {
          long ts = System.currentTimeMillis();
          biosAdmin.updateSignal(
              tenantName, signalWithTtlName, signalWithTtl, RequestPhase.INITIAL, ts);
          biosAdmin.updateSignal(
              tenantName, signalWithTtlName, signalWithTtl, RequestPhase.FINAL, ts);
        });
    info = admin.getFeatureAsContextInfo(tenantName, contextWithTtlName);
    assertNotNull(info);
    assertEquals(signalWithTtlName + "." + feature.getName(), info.getReferrer());
    assertEquals(Long.valueOf(3600000L), info.getTtlInMillis());

    // changing target context to another
    final var anotherContext = new ContextConfig(context);
    anotherContext.setName("anotherContext");
    timestamp = System.currentTimeMillis();
    try {
      biosAdmin.deleteContext(tenantName, "anotherContext", RequestPhase.INITIAL, timestamp);
      biosAdmin.deleteContext(tenantName, "anotherContext", RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    ++timestamp;
    biosAdmin.createContext(tenantName, anotherContext, RequestPhase.INITIAL, timestamp);
    biosAdmin.createContext(tenantName, anotherContext, RequestPhase.FINAL, timestamp);

    signalWithTtl
        .getPostStorageStage()
        .getFeatures()
        .get(0)
        .setFeatureAsContextName("anotherContext");
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.INITIAL, timestamp);
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.FINAL, timestamp);

    assertNull(admin.getFeatureAsContextInfo(tenantName, contextWithTtlName));
    info = admin.getFeatureAsContextInfo(tenantName, "anothercontext");
    assertNotNull(info);
    assertEquals(signalWithTtlName + "." + feature.getName(), info.getReferrer());
    assertEquals(Long.valueOf(3600000L), info.getTtlInMillis());

    // delete the feature with LastN
    signalWithTtl.setName(signalWithTtlName);
    signalWithTtl.getPostStorageStage().getFeatures().clear();
    timestamp = System.currentTimeMillis();
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.INITIAL, timestamp);
    biosAdmin.updateSignal(
        tenantName, signalWithTtlName, signalWithTtl, RequestPhase.FINAL, timestamp);

    info = admin.getFeatureAsContextInfo(tenantName, contextWithTtlName);
    assertNull(info);
  }

  private long insertSignal(
      InsertServiceHandler insertServiceHandler,
      SessionToken sessionToken,
      String signalToInsert,
      Long memberId,
      Long issueId,
      Map<Long, List<LastNItem>> issues)
      throws Exception {
    UUID eventId = UUIDs.timeBased();
    final var request = new IngestRequest(eventId, String.format("%d,%d", memberId, issueId), null);
    final var response =
        insertServiceHandler
            .insertEvent(sessionToken, tenantName, signalToInsert, request, null)
            .get();
    long insertTimestamp = response.getTimestamp();

    if (issues != null) {
      List<LastNItem> issueIds = issues.get(memberId);
      if (issueIds == null) {
        issueIds = new ArrayList<>();
        issues.put(memberId, issueIds);
      }
      issueIds.add(new LastNItem(insertTimestamp, Long.valueOf(issueId)));
      if (issueIds.size() > 5) {
        issues.put(memberId, issueIds.subList(issueIds.size() - 5, issueIds.size()));
      }
    }
    return insertTimestamp;
  }

  private void verifyContextEntry(
      DataServiceHandler handler, String testContextName, Long memberId, List<LastNItem> issueIds)
      throws Exception {
    final var contextEntries =
        TestUtils.getContextEntries(
            handler, tenantName, testContextName, List.of(List.of(memberId)));
    if (issueIds != null) {
      assertEquals(1, contextEntries.size());
      final var contextEntry = contextEntries.get(0);
      final var lastN =
          mapper.readValue(
              contextEntry.get(context.getAttributes().get(1).getName()).toString(), LastN.class);
      assertEquals(issueIds, lastN.getCollection());
    } else {
      assertEquals(0, contextEntries.size());
    }
  }

  private void verifyFeature0(io.isima.bios.admin.Admin biosAdminForTest) throws Exception {
    final var retrievedSignals =
        biosAdminForTest.getSignals(tenantName, true, false, false, List.of(signalName));
    assertNotNull(retrievedSignals);
    assertEquals(1, retrievedSignals.size());
    final var retrievedSignalConfig = retrievedSignals.get(0);
    final var featureConfig = retrievedSignalConfig.getPostStorageStage().getFeatures().get(0);
    assertEquals("lastFiveVisits", featureConfig.getName());
    assertEquals(List.of("memberId"), featureConfig.getDimensions());
    assertEquals(List.of("issueId"), featureConfig.getAttributes());
    assertEquals(MaterializedAs.LAST_N, featureConfig.getMaterializedAs());
    assertEquals(Long.valueOf(5), featureConfig.getItems());
    assertEquals(Long.valueOf(BiosConstants.DEFAULT_LAST_N_COLLECTION_TTL), featureConfig.getTtl());
    final var contextName = featureConfig.getFeatureAsContextName();
    assertEquals("lastVisits", contextName);

    final var retrievedContexts =
        biosAdminForTest.getContexts(tenantName, true, true, false, List.of(contextName));
    assertNotNull(retrievedContexts);
    assertEquals(1, retrievedContexts.size());
    final var facContext = retrievedContexts.get(0);
    assertTrue(facContext.getIsInternal());

    final var listedNonInternalContexts =
        biosAdminForTest.getContexts(tenantName, true, false, false, null);
    assertFalse(
        listedNonInternalContexts.stream()
            .anyMatch((ctx) -> ctx.getName().equalsIgnoreCase(contextName)));

    final var listedContexts = biosAdminForTest.getContexts(tenantName, true, true, false, null);
    final var foundContext =
        listedContexts.stream()
            .filter((ctx) -> ctx.getName().equalsIgnoreCase(contextName))
            .findFirst();
    assertTrue(foundContext.isPresent());
    assertTrue(foundContext.get().getIsInternal());
  }
}
