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

import static io.isima.bios.models.AttributeModAllowance.CONVERTIBLES_ONLY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.TableMetadata;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.CassTenant;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
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
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminTest {
  private final Logger logger = LoggerFactory.getLogger(AdminTest.class);

  private static AdminStore adminStore;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection conn;

  private static DataServiceHandler dataServiceHandler;

  private static long timestamp;

  // Tenants used for tests; This list is used for DB cleanup after test.
  private final List<TenantDesc> testTenants = new ArrayList<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AdminTest.class);
    adminStore = BiosModules.getAdminStore();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    conn = BiosModules.getCassandraConnection();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    timestamp = System.currentTimeMillis();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    testTenants.clear();
  }

  @After
  public void tearDown() {
    for (TenantDesc tenantDesc : testTenants) {
      final AdminInternal admin = BiosModules.getAdminInternal();
      final TenantConfig tenantConfig = tenantDesc.toTenantConfig().setVersion(++timestamp);
      try {
        admin.removeTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
      try {
        admin.removeTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());
      } catch (TfosException | ApplicationException e) {
        // ignore
      }
    }
  }

  @Test
  public void test() throws ApplicationException, TfosException, FileReadException {

    // build the tenant
    final String tenantName = "adminTestTenant";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // Add the tenant to AdminInternal -- initial execution
    final AdminInternal admin = BiosModules.getAdminInternal();
    admin.addTenant(tenantDesc.toTenantConfig(), RequestPhase.INITIAL, timestamp);
    signal.setVersion(timestamp);

    // Verify the ingest engine is ready
    CassTenant cassTenant = dataEngine.getCassTenant(tenantDesc);
    assertNotNull(cassTenant);

    CassStream cassStream = cassTenant.getCassStream(signal);
    assertNotNull(cassStream);
    final String signalTableName = cassStream.getTableName();

    final String keyspace = cassTenant.getKeyspaceName();
    Cluster cluster = conn.getCluster();
    TableMetadata metadata = cluster.getMetadata().getKeyspace(keyspace).getTable(signalTableName);
    assertNotNull(metadata);

    // The tenant should not be ready yet
    try {
      final TenantDesc retrieved = admin.getTenant(tenantName);
      fail("exception is expected: " + retrieved.toString());
    } catch (NoSuchTenantException e) {
      // expected exception, do nothing
    }

    // Add the tenant to AdminInternal -- final execution
    admin.addTenant(tenantDesc.toTenantConfig(), RequestPhase.FINAL, timestamp);

    // The tenant should be available now. Check tenant existence. Also check if streams are
    // initialized propertly.
    final TenantDesc retrieved = admin.getTenant(tenantName);
    assertNotNull(retrieved);
    assertNotSame(retrieved, tenantDesc);
    assertEquals(2, retrieved.getStreams().size());
    final StreamDesc retrievedSignal = retrieved.getStream(signalName, false);
    assertNotNull(retrievedSignal);
    assertNotSame(retrievedSignal, signal);
    assertNotNull(retrievedSignal.getPreprocessStages());
    assertEquals(1, retrievedSignal.getPreprocessStages().size());
    assertNotNull(retrievedSignal.getAdditionalAttributes());
    assertEquals(1, retrievedSignal.getAdditionalAttributes().size());

    // Test AdminInternal and DataEngine bootstrap
    final DataEngineImpl newDataEngine = new DataEngineImpl(conn);
    final AdminInternal newAdmin =
        new AdminImpl(adminStore, new MetricsStreamProvider(), newDataEngine);
    final TenantDesc loaded = newAdmin.getTenant(tenantName);
    assertNotNull(loaded);
    assertEquals(2, loaded.getStreams().size());
    final StreamDesc loadedSignal = loaded.getStream(signalName, false);
    assertNotNull(loadedSignal);
    assertNotSame(loadedSignal, signal);
    assertNotNull(loadedSignal.getPreprocessStages());
    assertEquals(1, loadedSignal.getPreprocessStages().size());
    assertNotNull(loadedSignal.getAdditionalAttributes());
    assertEquals(1, loadedSignal.getAdditionalAttributes().size());

    CassTenant loadedCassTenant = newDataEngine.getCassTenant(tenantDesc);
    assertNotNull(loadedCassTenant);

    CassStream loadedCassStream = loadedCassTenant.getCassStream(loadedSignal);
    assertNotNull(loadedCassStream);

    // Test deleting the tenant
    final TenantConfig toDelete = tenantDesc.toTenantConfig();
    admin.removeTenant(toDelete, RequestPhase.INITIAL, ++timestamp);

    // finalize the deletion
    admin.removeTenant(toDelete, RequestPhase.FINAL, timestamp);
    try {
      admin.getTenant(tenantName);
      fail("Exception is expected. retrieved=" + retrieved);
    } catch (NoSuchTenantException e) {
      // expected, do nothing
    }
    // metadata for ingestion should be accessible, yet
    cassTenant = dataEngine.getCassTenant(tenantDesc);
    assertNotNull(cassTenant);

    // bootstrap another new admin and verify the deleted tenant is not reloaded
    final var modules2 = TestModules.reload(AdminTest.class + " rl1", conn);
    final var newDataEngine2 = modules2.getDataEngine();
    final AdminInternal newAdmin2 =
        new AdminImpl(adminStore, new MetricsStreamProvider(), newDataEngine2);
    try {
      newAdmin2.getTenant(tenantName);
      fail("Exception is expected. retrieved=" + retrieved);
    } catch (NoSuchTenantException e) {
      // expected
    }
    cassTenant = newDataEngine2.getCassTenant(tenantDesc);
    assertNull(cassTenant);
  }

  @Test
  public void testUpdateStream() throws Throwable {
    final String tenantName = "updateStreamTest";

    // prepare the initial tenant
    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, ++timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));

    final TenantDesc tenantDesc = new TenantDesc(tenantName, timestamp, false);
    testTenants.add(tenantDesc);
    tenantDesc.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("amount", InternalAttributeType.LONG));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    action.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    action.setDefaultValue("921");
    pp.addAction(action);
    signal.addPreprocess(pp);

    tenantDesc.addStream(signal);

    // Add initial tenant
    final AdminInternal admin1 = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin1.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin1.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    // Test constraint check violations
    final StreamConfig modifiedContext =
        context.duplicate().setName("ModifiedContext").setVersion(++timestamp);
    verifyNotImplemented(admin1, tenantName, contextName, modifiedContext, timestamp);
    // TODO(TFOS-1273): Rename stream is unsupported yet
    // verifyConstraintCheck(admin1, tenantName, contextName, modifiedContext, timestamp);

    final StreamConfig newSignal = signal.duplicate().setVersion(++timestamp);
    newSignal.getPreprocesses().get(0).setCondition("wrongName");
    verifyConstraintCheck(admin1, tenantName, signalName, newSignal, timestamp);

    // Create context to modify
    final StreamConfig newSignal2 = signal.duplicate().setVersion(++timestamp);
    newSignal2.getAttributes().get(1).setAttributeType(InternalAttributeType.STRING); // was long

    // try updating stream
    final AdminStore adminStore2 = new AdminStoreImpl(conn);
    final DataEngineImpl data2 = new DataEngineImpl(conn);
    final AdminInternal admin2 = new AdminImpl(adminStore2, new MetricsStreamProvider(), data2);

    admin1.modifyStream(
        tenantName, signalName, newSignal2, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());

    admin1.modifyStream(
        tenantName,
        signalName,
        newSignal2.duplicate(),
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());
    admin2.modifyStream(
        tenantName,
        signalName,
        newSignal2.duplicate(),
        RequestPhase.FINAL,
        CONVERTIBLES_ONLY,
        Set.of());

    // verify
    final StreamConfig retrieved1 = admin1.getStream(tenantName, signalName);
    assertEquals(
        InternalAttributeType.STRING, retrieved1.getAttributes().get(1).getAttributeType());
    assertEquals(Long.valueOf(timestamp), retrieved1.getVersion());

    final StreamConfig retrieved2 = admin2.getStream(tenantName, signalName);
    assertEquals(
        InternalAttributeType.STRING, retrieved2.getAttributes().get(1).getAttributeType());
    assertEquals(Long.valueOf(timestamp), retrieved2.getVersion());

    final AdminStore adminStore3 = new AdminStoreImpl(conn);
    final DataEngineImpl data3 = new DataEngineImpl(conn);
    final AdminInternal admin3 = new AdminImpl(adminStore3, new MetricsStreamProvider(), data3);
    final StreamConfig retrieved3 = admin3.getStream(tenantName, signalName);
    assertEquals(
        InternalAttributeType.STRING, retrieved3.getAttributes().get(1).getAttributeType());

    // Try ingesting into new stream
    final var dataServiceHandler2 =
        new DataServiceHandler(
            data2, admin2, BiosModules.getMetrics(), BiosModules.getSharedConfig());
    TestUtils.insert(dataServiceHandler2, tenantName, signalName, "hello,10.20.30.40");
  }

  private void verifyConstraintCheck(
      final AdminInternal admin,
      final String tenant,
      final String stream,
      final StreamConfig config,
      Long timestamp)
      throws TfosException, ApplicationException {
    try {
      admin.modifyStream(
          tenant,
          stream,
          config.duplicate().setVersion(timestamp),
          RequestPhase.INITIAL,
          CONVERTIBLES_ONLY,
          Set.of());
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      // expected
    }
  }

  private void verifyNotImplemented(
      final AdminInternal admin,
      final String tenant,
      final String stream,
      final StreamConfig config,
      Long timestamp)
      throws TfosException, ApplicationException {
    try {
      admin.modifyStream(
          tenant,
          stream,
          config.duplicate().setVersion(timestamp),
          RequestPhase.INITIAL,
          CONVERTIBLES_ONLY,
          Set.of());
      fail("exception is expected");
    } catch (NotImplementedException e) {
      // expected
    }
  }

  /**
   * 1. Run only iniaial part of tenant add operation. 2. Try ingest events -- should fail with
   * NO_SUCH_TENANT 3. Try extract events -- should fail with NO_SUCH_TENANT 4. Run final part of
   * tenant add operation. 5. Try ingest events -- should succeed 6. Try extract events -- should
   * succeed.
   *
   * @throws Throwable when exception happens
   */
  @Test
  public void incompleteAddTenant() throws Throwable {
    // build the tenant
    final String tenantName = "incompleteAddTenant";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // add tenant, initial phase
    final AdminInternal admin = BiosModules.getAdminInternal();
    signal.setVersion(timestamp);
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);

    // try ingestion
    final var exception =
        assertThrows(
            TfosException.class,
            () -> TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello"));
    assertThat(exception, instanceOf(NoSuchTenantException.class));

    // try extract
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(0L);
    extractRequest.setEndTime(0L);
    final var exception2 =
        assertThrows(
            TfosException.class,
            () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
    assertThat(exception2, instanceOf(NoSuchTenantException.class));

    // add tenant, final phase
    admin.addTenant(tenantDesc.toTenantConfig(), RequestPhase.FINAL, timestamp);

    // add context
    final StreamDesc context2 = admin.getStream(tenantName, contextName);

    TestUtils.upsertContextEntries(
        dataServiceHandler,
        BiosModules.getHttpClientManager(),
        tenantName,
        context2.getName(),
        List.of("hello,123"));

    // ingest again
    final var response = TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello");

    // extract again
    // final long ingestTimestamp = ((IngestResponse) reply).getIngestTimestamp().getTime();
    final long ingestTimestamp = response.getTimeStamp();
    extractRequest.setStartTime(ingestTimestamp);
    extractRequest.setEndTime(ingestTimestamp + 1000);
    final var resp = TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
    assertThat(resp.size(), is(1));
  }

  /**
   * 1. Run only iniaial part of tenant delete operation. 2. Run final part of tenant delete
   * operation. 3. Try ingest events -- should fail 4. Try extract events -- should fail
   */
  @Test
  public void verifyDeleteTenantCompletion() throws Throwable {
    // build the tenant
    final String tenantName = "incompleteDeleteTenant";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // add tenant
    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    final long tenantTimestamp = timestamp;

    // delete tenant, initial phase
    final TenantDesc toDelete = new TenantDesc(tenantDesc.getName(), ++timestamp, true);
    admin.removeTenant(toDelete.toTenantConfig(), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(toDelete.toTenantConfig(), RequestPhase.FINAL, timestamp);

    // try ingestion
    final var exception =
        assertThrows(
            TfosException.class,
            () -> TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello"));
    assertThat(exception, instanceOf(NoSuchTenantException.class));

    // try extract
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(0L);
    extractRequest.setEndTime(0L);
    final var exception2 =
        assertThrows(
            TfosException.class,
            () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
    assertThat(exception2, instanceOf(NoSuchTenantException.class));
  }

  /**
   * 1. Run only iniaial part of stream add operation. 2. Try ingest events -- should fail with
   * NO_SUCH_STREAM 3. Try extract events -- should fail with NO_SUCH_STREAM 4. Run final part of
   * stream add operation. 5. Try ingest events -- should succeed 6. Try extract events -- should
   * succeed
   *
   * @throws Throwable when exception happens
   */
  @Test
  public void incompleteAddStream() throws Throwable {
    // build the tenant
    final String tenantName = "incompleteAddStream";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc.addStream(context);

    // add base tenant
    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String signalName = "Signal";
    final StreamConfig signal = new StreamConfig(signalName);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    action.setDefaultValue("921");
    pp.addAction(action);
    signal.addPreprocess(pp);

    // add stream, initial phase
    admin.addStream(tenantName, signal, RequestPhase.INITIAL, ++timestamp);

    // try inserting
    final var exception =
        assertThrows(
            TfosException.class,
            () -> TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello"));
    assertThat(exception, instanceOf(NoSuchStreamException.class));

    // try extract
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(0L);
    extractRequest.setEndTime(0L);
    final var exception2 =
        assertThrows(
            TfosException.class,
            () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
    assertThat(exception2, instanceOf(NoSuchStreamException.class));

    // add stream, final phase
    admin.addStream(tenantName, signal, RequestPhase.FINAL, timestamp);

    // ingest again
    final var response = TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello");

    // extract again
    // long ingestTimestamp = ((IngestResponse) reply).getIngestTimestamp().getTime();
    final long ingestTimestamp = response.getTimeStamp();
    extractRequest.setStartTime(ingestTimestamp);
    extractRequest.setEndTime(ingestTimestamp + 1000);
    final var events =
        TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest);
    assertThat(events.size(), is(1));
  }

  @Test
  public void verifyDeleteStreamCompletion() throws Throwable {
    // build the tenant
    final String tenantName = "incompleteDeleteStream";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc.addStream(signal);

    // add tenant
    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    long tenantTimestamp = timestamp;

    // delete stream
    try {
      admin.removeStream(tenantName, contextName, RequestPhase.INITIAL, ++timestamp);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      // expected
    }
    admin.removeStream(tenantName, signalName, RequestPhase.INITIAL, ++timestamp);
    admin.removeStream(tenantName, signalName, RequestPhase.FINAL, timestamp);

    // try ingest
    final var exception =
        assertThrows(
            TfosException.class,
            () -> TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello"));
    assertThat(exception, instanceOf(NoSuchStreamException.class));

    // try extract
    final ExtractRequest extractRequest = new ExtractRequest();
    extractRequest.setStartTime(0L);
    extractRequest.setEndTime(0L);
    final var exception2 =
        assertThrows(
            TfosException.class,
            () -> TestUtils.extract(dataServiceHandler, tenantName, signalName, extractRequest));
    assertThat(exception2, instanceOf(NoSuchStreamException.class));

    // also try deleting context stream
    admin.removeStream(tenantName, contextName, RequestPhase.INITIAL, ++timestamp);
    admin.removeStream(tenantName, contextName, RequestPhase.FINAL, timestamp);
  }

  @Test
  public void addTenantConflictWithDifferentNames() throws Throwable {
    // build the tenant
    final String tenantName1 = "conflictTenant1";
    final String tenantName2 = "conflictTenant2";

    final TenantDesc tenantDesc1 = new TenantDesc(tenantName1, ++timestamp, false);
    testTenants.add(tenantDesc1);

    final TenantDesc tenantDesc2 = new TenantDesc(tenantName2, timestamp, false);
    testTenants.add(tenantDesc2);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc1.addStream(context);
    tenantDesc2.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    final PreprocessDesc pp = new PreprocessDesc("merge");
    pp.setCondition("lookup");
    pp.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    final ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("value");
    action.setDefaultValue(1000);
    action.setContext(contextName);
    pp.addAction(action);
    signal.addPreprocess(pp);
    tenantDesc1.addStream(signal);
    tenantDesc2.addStream(signal);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig1 = tenantDesc1.toTenantConfig();
    final TenantConfig tenantConfig2 = tenantDesc2.toTenantConfig();
    admin.addTenant(tenantConfig1, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig2, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig1, RequestPhase.FINAL, timestamp);
    admin.addTenant(tenantConfig2, RequestPhase.FINAL, timestamp);

    // try inserting
    TestUtils.insert(dataServiceHandler, tenantName1, signalName, "hello");
    TestUtils.insert(dataServiceHandler, tenantName2, signalName, "hello");
  }

  @Test
  @Ignore("BIOS-3001 known issue")
  public void addTenantConflictWithTheSameName1() throws Throwable {
    int[] order = {0, 1, 0, 1};
    addTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-3001 known issue")
  public void addTenantConflictWithTheSameName2() throws Throwable {
    int[] order = {1, 0, 0, 1};
    addTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-3001 known issue")
  public void addTenantConflictWithTheSameName3() throws Throwable {
    int[] order = {0, 1, 1, 0};
    addTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-3001 known issue")
  public void addTenantConflictWithTheSameName4() throws Throwable {
    int[] order = {1, 0, 1, 0};
    addTenantConflictWithTheSameNameSub(order);
  }

  private void addTenantConflictWithTheSameNameSub(int[] order) throws Throwable {
    // build the tenant
    final String tenantName1 = "conflictTenant";
    final String tenantName2 = "conflictTenant";

    final TenantDesc tenantDesc1 = new TenantDesc(tenantName1, ++timestamp, false);
    testTenants.add(tenantDesc1);

    final TenantDesc tenantDesc2 = new TenantDesc(tenantName2, ++timestamp, false);
    testTenants.add(tenantDesc2);

    final String contextName = "Context";
    final StreamDesc context = new StreamDesc(contextName, timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    tenantDesc1.addStream(context);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    tenantDesc2.addStream(signal);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig[] tenants = {tenantDesc1.toTenantConfig(), tenantDesc2.toTenantConfig()};
    int idx = order[0];
    admin.addTenant(tenants[idx], RequestPhase.INITIAL, tenants[idx].getVersion());
    idx = order[1];
    admin.addTenant(tenants[idx], RequestPhase.INITIAL, tenants[idx].getVersion());
    idx = order[2];
    admin.addTenant(tenants[idx], RequestPhase.FINAL, tenants[idx].getVersion());
    idx = order[3];
    admin.addTenant(tenants[idx], RequestPhase.FINAL, tenants[idx].getVersion());

    // newer tenant wins
    final TenantDesc retrieved = admin.getTenant(tenantName1);
    assertEquals(tenantDesc2.getVersion(), retrieved.getVersion());

    // try inserts
    TestUtils.insert(dataServiceHandler, tenantName1, signalName, "hello");
    TestUtils.insert(dataServiceHandler, tenantName2, signalName, "hello");
  }

  @Test
  @Ignore("BIOS-5171 known issue")
  public void deleteTenantConflictWithTheSameName1() throws Throwable {
    int[] order = {0, 1, 0, 1};
    deleteTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-5171 known issue")
  public void deleteTenantConflictWithTheSameName2() throws Throwable {
    int[] order = {1, 0, 0, 1};
    deleteTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-5171 known issue")
  public void deleteTenantConflictWithTheSameName3() throws Throwable {
    int[] order = {0, 1, 1, 0};
    deleteTenantConflictWithTheSameNameSub(order);
  }

  @Test
  @Ignore("BIOS-5171 known issue")
  public void deleteTenantConflictWithTheSameName4() throws Throwable {
    int[] order = {1, 0, 1, 0};
    deleteTenantConflictWithTheSameNameSub(order);
  }

  private void deleteTenantConflictWithTheSameNameSub(int[] order) throws Throwable {
    // build the tenant
    final String tenantName2 = "conflictTenant";

    final TenantDesc tenantDesc2 = new TenantDesc(tenantName2, ++timestamp, false);
    testTenants.add(tenantDesc2);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    tenantDesc2.addStream(signal);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig2 = tenantDesc2.toTenantConfig();
    admin.addTenant(tenantConfig2, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig2, RequestPhase.FINAL, timestamp);

    Long[] timestamps = {timestamp + 1, timestamp + 2};
    timestamp += 2;
    int idx = order[0];
    admin.removeTenant(tenantConfig2, RequestPhase.INITIAL, timestamps[idx]);
    idx = order[1];
    admin.removeTenant(tenantConfig2, RequestPhase.INITIAL, timestamps[idx]);
    idx = order[2];
    admin.removeTenant(tenantConfig2, RequestPhase.FINAL, timestamps[idx]);
    idx = order[3];
    admin.removeTenant(tenantConfig2, RequestPhase.FINAL, timestamps[idx]);

    // verify removal
    try {
      admin.getTenant(tenantName2);
      fail("exception is expected");
    } catch (NoSuchTenantException e) {
      // expected
    }
  }

  @Test
  public void addStreamConflictWithDifferentName1() throws Throwable {
    int[] order = {0, 1, 0, 1};
    addStreamConflictWithDifferentNameSub(order);
  }

  @Test
  public void addStreamConflictWithDifferentName2() throws Throwable {
    int[] order = {1, 0, 0, 1};
    addStreamConflictWithDifferentNameSub(order);
  }

  @Test
  public void addStreamConflictWithDifferentName3() throws Throwable {
    int[] order = {0, 1, 1, 0};
    addStreamConflictWithDifferentNameSub(order);
  }

  @Test
  public void addStreamConflictWithDifferentName4() throws Throwable {
    int[] order = {1, 0, 1, 0};
    addStreamConflictWithDifferentNameSub(order);
  }

  private void addStreamConflictWithDifferentNameSub(int[] order) throws Throwable {
    // build the tenant
    final String tenantName = "conflictStream";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String contextName = "Context";
    final StreamConfig context = new StreamConfig(contextName).setVersion(++timestamp);
    context.setType(StreamType.CONTEXT);
    context.addAttribute(new AttributeDesc("key", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));

    final String signalName = "Signal";
    final StreamConfig signal = new StreamConfig(signalName).setVersion(++timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));

    final StreamConfig[] streams = {signal, context};
    int idx = order[0];
    admin.addStream(tenantName, streams[idx], RequestPhase.INITIAL, streams[idx].getVersion());
    idx = order[1];
    admin.addStream(tenantName, streams[idx], RequestPhase.INITIAL, streams[idx].getVersion());
    idx = order[2];
    admin.addStream(tenantName, streams[idx], RequestPhase.FINAL, streams[idx].getVersion());
    idx = order[3];
    admin.addStream(tenantName, streams[idx], RequestPhase.FINAL, streams[idx].getVersion());

    // both streams should exist
    admin.getStream(tenantName, signalName);
    admin.getStream(tenantName, contextName);

    // try ingest
    TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello");
    TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello");
  }

  @Test
  public void addStreamConflictWithSameName1() throws Throwable {
    int[] order = {0, 1, 0, 1};
    addStreamConflictWithSameNameSub(order);
  }

  @Test
  public void addStreamConflictWithSameName2() throws Throwable {
    int[] order = {1, 0, 0, 1};
    addStreamConflictWithSameNameSub(order);
  }

  @Test
  public void addStreamConflictWithSameName3() throws Throwable {
    int[] order = {0, 1, 1, 0};
    addStreamConflictWithSameNameSub(order);
  }

  @Test
  public void addStreamConflictWithSameName4() throws Throwable {
    int[] order = {1, 0, 1, 0};
    addStreamConflictWithSameNameSub(order);
  }

  private void addStreamConflictWithSameNameSub(int[] order) throws Throwable {
    // build the tenant
    final String tenantName = "conflictStreamSameName";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    final String signalName = "Signal";
    final StreamConfig signal1 = new StreamConfig(signalName).setVersion(++timestamp);
    signal1.addAttribute(new AttributeDesc("strLookup", InternalAttributeType.STRING));

    final StreamConfig signal2 = new StreamConfig(signalName).setVersion(++timestamp);
    signal2.addAttribute(new AttributeDesc("intLookup", InternalAttributeType.LONG));

    final StreamConfig[] streams = {signal1, signal2};
    int idx = order[0];
    admin.addStream(tenantName, streams[idx], RequestPhase.INITIAL, streams[idx].getVersion());
    idx = order[1];
    admin.addStream(tenantName, streams[idx], RequestPhase.INITIAL, streams[idx].getVersion());
    idx = order[2];
    admin.addStream(tenantName, streams[idx], RequestPhase.FINAL, streams[idx].getVersion());
    idx = order[3];
    admin.addStream(tenantName, streams[idx], RequestPhase.FINAL, streams[idx].getVersion());

    // later stream wins
    final StreamConfig retrieved = admin.getStream(tenantName, signalName);
    assertEquals(InternalAttributeType.LONG, retrieved.getAttributes().get(0).getAttributeType());

    // try inserting
    final var exception =
        assertThrows(
            TfosException.class,
            () -> TestUtils.insert(dataServiceHandler, tenantName, signalName, "hello"));
    assertThat(exception, instanceOf(InvalidValueSyntaxException.class));

    TestUtils.insert(dataServiceHandler, tenantName, signalName, "12345");
  }

  @Test
  public void deleteStreamConflictWithSameName1() throws Throwable {
    int[] order = {0, 1, 0, 1};
    deleteStreamConflictWithSameNameSub(order);
  }

  @Test
  public void deleteStreamConflictWithSameName2() throws Throwable {
    int[] order = {1, 0, 0, 1};
    deleteStreamConflictWithSameNameSub(order);
  }

  @Test
  public void deleteStreamConflictWithSameName3() throws Throwable {
    int[] order = {0, 1, 1, 0};
    deleteStreamConflictWithSameNameSub(order);
  }

  @Test
  public void deleteStreamConflictWithSameName4() throws Throwable {
    int[] order = {1, 0, 1, 0};
    deleteStreamConflictWithSameNameSub(order);
  }

  private void deleteStreamConflictWithSameNameSub(int[] order) throws Throwable {
    // build the tenant
    final String tenantName = "conflictStream";

    final TenantDesc tenantDesc = new TenantDesc(tenantName, ++timestamp, false);
    testTenants.add(tenantDesc);

    final String signalName = "Signal";
    final StreamDesc signal = new StreamDesc(signalName, timestamp);
    signal.addAttribute(new AttributeDesc("lookup", InternalAttributeType.STRING));
    tenantDesc.addStream(signal);

    final AdminInternal admin = BiosModules.getAdminInternal();
    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    Long[] timestamps = {timestamp + 1, timestamp + 2};
    timestamp += 2;
    int idx = order[0];
    admin.removeStream(tenantName, signalName, RequestPhase.INITIAL, timestamps[idx]);
    idx = order[1];
    admin.removeStream(tenantName, signalName, RequestPhase.INITIAL, timestamps[idx]);
    idx = order[2];
    admin.removeStream(tenantName, signalName, RequestPhase.FINAL, timestamps[idx]);
    idx = order[3];
    admin.removeStream(tenantName, signalName, RequestPhase.FINAL, timestamps[idx]);

    // verify removal
    try {
      admin.getStream(tenantName, signalName);
      fail("exception is expected");
    } catch (NoSuchStreamException e) {
      // expected
    }
  }
}
