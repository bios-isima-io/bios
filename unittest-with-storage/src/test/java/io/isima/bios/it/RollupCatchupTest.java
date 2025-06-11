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

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.LastN;
import io.isima.bios.models.LastNItem;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.handler.InsertServiceHandler;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test corner cases of LastN feature as context with cache only context turned on. */
public class RollupCatchupTest {
  private static String tenantName;
  private static String signalName;
  private static String contextName;

  private static SignalConfig signal;
  private static ContextConfig context;

  private static AdminInternal admin;
  private static io.isima.bios.admin.Admin biosAdmin;
  private static DataEngineImpl dataEngine;
  private static DataServiceHandler dataServiceHandler;
  private static DataEngineMaintenance maintenance;

  private static ObjectMapper mapper;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tenantName = "rollupCatchupTest";
    signalName = "visits";
    contextName = "lastVisits";

    Bios2TestModules.setProperty(TfosConfig.CONTEXT_MAINTENANCE_CLEANUP_MARGIN, "0");
    Bios2TestModules.startModules(
        false,
        RollupCatchupTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "1000",
            "prop.maintenance.fastTrackSignals",
            tenantName + "." + signalName,
            "prop.rollupDebugSignal",
            signalName));
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    biosAdmin = BiosModules.getAdmin();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    maintenance = ((DataEngineImpl) BiosModules.getDataEngine()).getMaintenance();

    final String contextSrc =
        "{"
            + "  'contextName': 'lastVisits',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'memberId', 'type': 'String'},"
            + "    {'attributeName': 'visits', 'type': 'String'}"
            + "  ],"
            + "  'primaryKey': ['memberId']"
            + "}";

    final String signalSrc =
        "{"
            + "  'signalName': 'visits',"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'memberId', 'type': 'String'},"
            + "    {'attributeName': 'region', 'type': 'String'},"
            + "    {'attributeName': 'issueId', 'type': 'Integer'},"
            + "    {'attributeName': 'age', 'type': 'Integer'}"
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
            + "        'featureInterval': 5000"
            + "      },"
            + "      {"
            + "        'featureName': 'byRegion',"
            + "        'dimensions': ['region'],"
            + "        'attributes': ['age'],"
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

  /**
   * In this test, we populate events without rollup worker running. Rollups would get behind after
   * insertions. We test bundling multiple rollup intervals for quick catchup by running only one
   * rollup afterward.
   */
  @Test
  public void testBasic() throws Exception {

    final var regions = List.of("us", "asia", "canada", "australia", "europe");
    final var issues = new HashMap<String, List<LastNItem>>();

    final var visitors = new ArrayList<Triple<String, String, Long>>();
    // create 100 visitors
    final int numVisitors = 300;
    final var random = new Random();
    for (int i = 0; i < numVisitors; ++i) {
      final var memberId = UUID.randomUUID().toString();
      final var region = regions.get(random.nextInt(regions.size()));
      final var age = 20L + random.nextInt(30);
      visitors.add(Triple.of(memberId, region, age));
    }

    // Trigger a single rollup
    System.out.println("Sleeping 10 seconds, run a maintenance in the mean time");
    Thread.sleep(1000);
    maintenance.setShutdown(false);
    maintenance.runMaintenance();
    Thread.sleep(6000);
    maintenance.stop();
    Thread.sleep(3000);

    // insert visitor activities without rollup running for multiple intervals
    final var countByRegion = new HashMap<String, AtomicLong>();
    Long lastInsertTimestamp = null;
    final long time0 = System.currentTimeMillis();
    long stampTime = time0 + 5000;
    boolean timeStamped = false;
    for (int i = 0; i < 800; ++i) {
      if (i % 25 == 0) {
        System.out.println("inserting " + i);
      }
      if (System.currentTimeMillis() > stampTime) {
        System.out.println(String.format(" ==> %d seconds", (stampTime - time0) / 1000));
        stampTime += 5000;
      }
      final var visitor = visitors.get(i % 100);
      final var memberId = visitor.getLeft();
      final var region = visitor.getMiddle();
      final var age = visitor.getRight();
      final var issueId = Long.valueOf(random.nextLong());
      final var count = countByRegion.computeIfAbsent(region, (r) -> new AtomicLong(0));
      count.incrementAndGet();
      Thread.sleep(50);
      long timestamp =
          insertSignal(
              BiosModules.getInsertServiceHandler(),
              sessionToken,
              signalName,
              memberId,
              region,
              age,
              issueId,
              issues);
      lastInsertTimestamp = timestamp;
    }

    // Sleep enough time to cover the last insertion by following one-shot rollup
    long sleepUntil = (lastInsertTimestamp + 14999) / 15000 * 15000 + 5000;
    long sleepTime = sleepUntil - System.currentTimeMillis();
    System.out.println(String.format("Sleeping %d ms", sleepTime));
    Thread.sleep(sleepTime);

    System.out.println("Doing one-shot rollup");
    maintenance.setShutdown(false);
    maintenance.runMaintenance();
    Thread.sleep(5000);

    for (var entry : issues.entrySet()) {
      final var memberId = entry.getKey();
      final var issueIds = entry.getValue();
      verifyContextEntry(dataServiceHandler, contextName, memberId, issueIds);
    }

    final var request = new SummarizeRequest();
    final var interval = 15000L;
    final long startTime = time0 / interval * interval;
    request.setStartTime(startTime);
    request.setEndTime((System.currentTimeMillis() + interval - 1) / interval * interval);
    request.setInterval(request.getEndTime() - request.getStartTime());
    request.setHorizon(request.getInterval());
    request.setGroup(List.of("region"));
    request.setAggregates(List.of(new Count() /*, new Min("age"), new Max("age")*/));

    final var streamDesc = admin.getStream(tenantName, signalName);
    final Map<Long, List<Event>> result =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);
    System.out.println(result);
    System.out.println(countByRegion);
    assertEquals(1, result.size());
    final var records = result.values().iterator().next();
    assertEquals(regions.size(), records.size());
    assertEquals("asia", records.get(0).get("region"));
    assertEquals(countByRegion.get("asia").get(), records.get(0).get("count()"));

    assertEquals("australia", records.get(1).get("region"));
    assertEquals(countByRegion.get("australia").get(), records.get(1).get("count()"));

    assertEquals("canada", records.get(2).get("region"));
    assertEquals(countByRegion.get("canada").get(), records.get(2).get("count()"));

    assertEquals("europe", records.get(3).get("region"));
    assertEquals(countByRegion.get("europe").get(), records.get(3).get("count()"));

    assertEquals("us", records.get(4).get("region"));
    assertEquals(countByRegion.get("us").get(), records.get(4).get("count()"));
  }

  private long insertSignal(
      InsertServiceHandler insertServiceHandler,
      SessionToken sessionToken,
      String signalToInsert,
      String memberId,
      String region,
      Long age,
      Long issueId,
      Map<String, List<LastNItem>> issues)
      throws Exception {
    UUID eventId = UUIDs.timeBased();
    final var request =
        new IngestRequest(
            eventId, String.format("%s,%s,%d,%d", memberId, region, issueId, age), null);
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
      DataServiceHandler handler, String testContextName, String memberId, List<LastNItem> issueIds)
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
}
