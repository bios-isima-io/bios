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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.data.impl.storage.RollupCassStream;
import io.isima.bios.data.impl.storage.ViewCassStream;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.MaterializedAs;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.PostStorageStageConfig;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SignalFeatureConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalAdminTest {

  private static final String TENANT_NAME = "signalAdminTest";

  private static Auth auth;
  private static Admin admin;
  private static AdminInternal tfosAdmin;
  private static DataEngineImpl dataEngine;
  private static CassandraConnection connection;
  private static AdminServiceHandler adminHandler;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(SignalAdminTest.class);
    auth = BiosModules.getAuth();
    admin = BiosModules.getAdmin();
    connection = BiosModules.getCassandraConnection();
    tfosAdmin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    adminHandler = BiosModules.getAdminServiceHandler();

    AdminTestUtils.setupTenant(admin, TENANT_NAME, true);

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
  public void testChangeSignalNameCases() throws Exception {
    final var signalName = "signin";
    final var signal = new SignalConfig();
    signal.setName(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));

    final FanRouter<SignalConfig, SignalConfig> fanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createSignal(
            sessionToken,
            TENANT_NAME,
            signal,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            TestUtils.makeGenericState())
        .get();

    // modify signal -- change cases of the signal name
    final var signal2 = new SignalConfig(signal);
    final var anotherAttribute = new AttributeConfig("anotherString", AttributeType.STRING);
    anotherAttribute.setDefaultValue(new AttributeValueGeneric("MISSING", AttributeType.STRING));
    final var modifiedAttributes = new ArrayList<>(signal2.getAttributes());
    modifiedAttributes.add(anotherAttribute);
    signal2.setAttributes(modifiedAttributes);
    signal2.setName("signIn");
    timestamp = System.currentTimeMillis();
    final var state = new GenericExecutionState("signalMod", ExecutorManager.getSidelineExecutor());
    final var updatedSignal =
        adminHandler
            .updateSignal(
                sessionToken,
                TENANT_NAME,
                signalName,
                signal2,
                RequestPhase.INITIAL,
                timestamp,
                Optional.of(fanRouter),
                false,
                state)
            .get();

    // delete signal
    final FanRouter<Void, Void> deleteFanRouter = TestUtils.makeDummyFanRouter();
    adminHandler.deleteSignal(
        sessionToken,
        TENANT_NAME,
        signalName,
        RequestPhase.INITIAL,
        System.currentTimeMillis(),
        Optional.of(deleteFanRouter),
        TestUtils.makeGenericState());

    // reload and verify the updated signal
    final var modules2 = TestModules.reload(SignalAdminTest.class + " rl1", connection);
    final var adminHandler2 = modules2.getAdminServiceHandler();

    assertThrows(
        NoSuchStreamException.class,
        () ->
            DataUtils.wait(
                adminHandler2.getSignals(
                    sessionToken,
                    TENANT_NAME,
                    List.of(signalName),
                    true,
                    false,
                    false,
                    new GenericExecutionState("getSignals", Executors.newSingleThreadExecutor())),
                "getSignals"));
  }

  @Test
  public void testFeatureWithStringAttribute() throws Exception {
    final var signalName = "featureWithStringAttribute";
    final var signal = new SignalConfig();
    signal.setName(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    final var feature = new SignalFeatureConfig();
    feature.setName("byKey");
    feature.setDimensions(List.of("key"));
    feature.setAttributes(List.of("intValue", "stringValue"));
    feature.setIndexed(true);
    feature.setFeatureInterval(300000L);
    final var postStorage = new PostStorageStageConfig();
    postStorage.setFeatures(List.of(feature));
    signal.setPostStorageStage(postStorage);

    final FanRouter<SignalConfig, SignalConfig> fanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createSignal(
            sessionToken,
            TENANT_NAME,
            signal,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            TestUtils.makeGenericState())
        .get();

    final var initialIndexTableName = validateViewByKey(tfosAdmin, dataEngine, signalName);
    final var initialRollupTableName = validateRollupByKey(tfosAdmin, dataEngine, signalName);
    validateRetrievedFeatureByKey(admin, signalName, feature);

    // reload and check the rollup table
    {
      final var adminStore2 = new AdminStoreImpl(connection);
      final var dataEngine2 = new DataEngineImpl(connection);
      final var tfosAdmin2 = new AdminImpl(adminStore2, null, dataEngine2);
      final var admin2 = new io.isima.bios.admin.AdminImpl(tfosAdmin2, null, null);

      final var reloadedIndexTableName = validateViewByKey(tfosAdmin2, dataEngine2, signalName);
      assertThat(reloadedIndexTableName, equalTo(initialIndexTableName));

      final var reloadedRollupTableName = validateRollupByKey(tfosAdmin2, dataEngine2, signalName);
      assertThat(reloadedRollupTableName, equalTo(initialRollupTableName));

      validateRetrievedFeatureByKey(admin2, signalName, feature);
    }
  }

  @Test
  public void testFeatureWithStringAttributeByMod() throws Exception {
    final var signalName = "featureWithStringAttributeByMod";
    final var signal = new SignalConfig();
    signal.setName(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("key", AttributeType.STRING),
            new AttributeConfig("intValue", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    final var feature = new SignalFeatureConfig();
    feature.setName("byKey");
    feature.setDimensions(List.of("key"));
    feature.setAttributes(List.of("intValue"));
    feature.setIndexed(true);
    feature.setFeatureInterval(300000L);
    final var postStorage = new PostStorageStageConfig();
    postStorage.setFeatures(List.of(feature));
    signal.setPostStorageStage(postStorage);

    final FanRouter<SignalConfig, SignalConfig> fanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createSignal(
            sessionToken,
            TENANT_NAME,
            signal,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            TestUtils.makeGenericState())
        .get();

    feature.setAttributes(List.of("intValue", "stringValue"));
    adminHandler
        .updateSignal(
            sessionToken,
            TENANT_NAME,
            signalName,
            signal,
            RequestPhase.INITIAL,
            ++timestamp,
            Optional.of(fanRouter),
            false,
            TestUtils.makeGenericState())
        .get();

    final var initialIndexTableName = validateViewByKey(tfosAdmin, dataEngine, signalName);
    final var initialRollupTableName = validateRollupByKey(tfosAdmin, dataEngine, signalName);
    validateRetrievedFeatureByKey(admin, signalName, feature);

    // reload and check the rollup table
    {
      final var adminStore2 = new AdminStoreImpl(connection);
      final var dataEngine2 = new DataEngineImpl(connection);
      final var tfosAdmin2 = new AdminImpl(adminStore2, null, dataEngine2);
      final var admin2 = new io.isima.bios.admin.AdminImpl(tfosAdmin2, null, null);

      final var reloadedIndexTableName = validateViewByKey(tfosAdmin2, dataEngine2, signalName);
      assertThat(reloadedIndexTableName, equalTo(initialIndexTableName));

      final var reloadedRollupTableName = validateRollupByKey(tfosAdmin2, dataEngine2, signalName);
      assertThat(reloadedRollupTableName, equalTo(initialRollupTableName));

      validateRetrievedFeatureByKey(admin2, signalName, feature);
    }
  }

  private void validateRetrievedFeatureByKey(
      Admin myAdmin, String signalName, SignalFeatureConfig feature)
      throws NoSuchTenantException, NoSuchStreamException, InvalidRequestException {
    final var retrievedSignals =
        myAdmin.getSignals(TENANT_NAME, true, false, false, List.of(signalName));
    assertThat(retrievedSignals.size(), is(1));
    final var retrievedSignal = retrievedSignals.get(0);
    assertThat(retrievedSignal.getPostStorageStage(), notNullValue());
    assertThat(retrievedSignal.getPostStorageStage().getFeatures(), notNullValue());
    assertThat(retrievedSignal.getPostStorageStage().getFeatures().size(), is(1));
    final var retrievedFeature = retrievedSignal.getPostStorageStage().getFeatures().get(0);
    assertThat(retrievedFeature, equalTo(feature));
  }

  private String validateViewByKey(
      AdminInternal myTfosAdmin, DataEngineImpl myDataEngine, String signalName)
      throws NoSuchTenantException, NoSuchStreamException {
    final var viewDesc = myTfosAdmin.getStream(TENANT_NAME, signalName + ".view.byKey");
    final var view = viewDesc.getView("byKey");
    assertThat(view, notNullValue());
    assertThat(view.getGroupBy(), equalTo(List.of("key")));
    assertThat(view.getAttributes(), equalTo(List.of("intValue", "stringValue")));

    final var viewCassStream = myDataEngine.getCassStream(viewDesc);
    assertThat(viewCassStream, notNullValue());
    assertThat(viewCassStream, instanceOf(ViewCassStream.class));
    final var tableName = viewCassStream.getTableName();
    final var tableMetadata =
        connection
            .getCluster()
            .getMetadata()
            .getKeyspace(viewCassStream.getKeyspaceName())
            .getTable(tableName);
    assertThat(tableMetadata, notNullValue());
    return tableName;
  }

  private String validateRollupByKey(
      AdminInternal myTfosAdmin, DataEngineImpl myDataEngine, String signalName)
      throws NoSuchTenantException, NoSuchStreamException {
    final var rollupDesc = myTfosAdmin.getStream(TENANT_NAME, signalName + ".rollup.byKey");
    final var rollupView = rollupDesc.getView("byKey");
    assertThat(rollupView, notNullValue());
    assertThat(rollupView.getGroupBy(), equalTo(List.of("key")));
    assertThat(rollupView.getAttributes(), equalTo(List.of("intValue")));

    final var rollupCassStream = myDataEngine.getCassStream(rollupDesc);
    assertThat(rollupCassStream, notNullValue());
    assertThat(rollupCassStream, instanceOf(RollupCassStream.class));

    final var keyAttribute = rollupCassStream.getAttributeDesc("key");
    assertThat(keyAttribute, notNullValue());
    final var countAttribute = rollupCassStream.getAttributeDesc("count");
    assertThat(countAttribute, notNullValue());
    final var intValueSum = rollupCassStream.getAttributeDesc("intValue_sum");
    assertThat(intValueSum, notNullValue());
    final var intValueMin = rollupCassStream.getAttributeDesc("intValue_min");
    assertThat(intValueMin, notNullValue());
    final var intValueMax = rollupCassStream.getAttributeDesc("intValue_max");
    assertThat(intValueMax, notNullValue());
    final var stringValueSum = rollupCassStream.getAttributeDesc("stringValue_sum");
    assertThat(stringValueSum, nullValue());
    final var tableName = rollupCassStream.getTableName();
    final var tableMetadata =
        connection
            .getCluster()
            .getMetadata()
            .getKeyspace(rollupCassStream.getKeyspaceName())
            .getTable(tableName);
    assertThat(tableMetadata, notNullValue());
    return tableName;
  }

  /** Case-mismatched reference names should be fixed on schema creation. */
  @Test
  public void reloadEnrichedSignalWithCaseMismatchedForeignKey()
      throws ApplicationException, TfosException {
    final var contextName = "simpleRemoteContext";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        Arrays.asList(
            new AttributeConfig("stringKey", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("stringValue", AttributeType.STRING)));
    context.setPrimaryKey(List.of("stringKey", "intKey"));

    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    final String signalName = "enrichedSignal";
    final var signal = new SignalConfig(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("myStringKey", AttributeType.STRING),
            new AttributeConfig("myIntKey", AttributeType.INTEGER)));
    final var enrichment = new EnrichmentConfigSignal("test");
    enrichment.setContextName(contextName.toUpperCase());
    final var orgForeignKey = List.of("myStringKey", "myIntKey");
    final var modForeignKey = List.of("MYSTRINGKEY", "MYINTKEY");
    enrichment.setForeignKey(modForeignKey);
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextAttributes(List.of(new EnrichmentAttribute("STRINGVALUE")));
    signal.setEnrich(new EnrichConfig(List.of(enrichment)));

    AdminTestUtils.createSignal(auth, adminHandler, TENANT_NAME, signal);

    final var created = tfosAdmin.getStream(TENANT_NAME, signalName);
    checkEnrichment(created, contextName, orgForeignKey, "stringValue");

    // reload and check again
    final var modules2 = TestModules.reload(SignalAdminTest.class + " rl2", connection);
    final var tfosAdmin2 = modules2.getTfosAdmin();
    final var reloaded = tfosAdmin2.getStream(TENANT_NAME, signalName);
    checkEnrichment(reloaded, contextName, orgForeignKey, "stringValue");
  }

  @Test
  public void reloadAccumulatingCount() throws Exception {
    final var signalName = "reloadAccumulatingCount";
    final var featureName = "count";
    final var contextName = signalName + "_" + featureName;
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        Arrays.asList(
            new AttributeConfig("stringKey", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            new AttributeConfig("timestamp", AttributeType.INTEGER),
            new AttributeConfig("counter1", AttributeType.DECIMAL),
            new AttributeConfig("counter2", AttributeType.DECIMAL)));
    context.setPrimaryKey(List.of("stringKey", "intKey"));

    AdminTestUtils.createContext(auth, adminHandler, TENANT_NAME, context);

    final var signal = new SignalConfig(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var op = new AttributeConfig("operation", AttributeType.STRING);
    op.setAllowedValues(
        List.of(
            new AttributeValueGeneric("set", AttributeType.STRING),
            new AttributeValueGeneric("change", AttributeType.STRING)));
    signal.setAttributes(
        List.of(
            new AttributeConfig("stringKey", AttributeType.STRING),
            new AttributeConfig("intKey", AttributeType.INTEGER),
            op,
            new AttributeConfig("counter1", AttributeType.DECIMAL),
            new AttributeConfig("counter2", AttributeType.DECIMAL)));
    final var feature = new SignalFeatureConfig();
    feature.setName(featureName);
    feature.setDimensions(List.of("stringKey", "intKey", "operation"));
    feature.setAttributes(List.of("counter1", "counter2"));
    feature.setFeatureInterval(5000L);
    feature.setMaterializedAs(MaterializedAs.ACCUMULATING_COUNT);
    feature.setIndexOnInsert(true);
    final var ppc = new PostStorageStageConfig();
    ppc.setFeatures(List.of(feature));
    signal.setPostStorageStage(ppc);

    AdminTestUtils.createSignal(auth, adminHandler, TENANT_NAME, signal);

    final var createdContext = tfosAdmin.getStream(TENANT_NAME, contextName);
    assertThat(createdContext, notNullValue());
    assertThat(createdContext.getContextAdjuster(), notNullValue());

    // reload and check again
    final var modules2 = TestModules.reload(SignalAdminTest.class + " rl3", connection);
    final var tfosAdmin2 = modules2.getTfosAdmin();

    final var reloadedContext = tfosAdmin2.getStream(TENANT_NAME, contextName);
    assertThat(reloadedContext, notNullValue());
    assertThat(reloadedContext.getContextAdjuster(), notNullValue());

    // delete the feature and reload
    final var signal2 = new SignalConfig(signal);
    signal2.setPostStorageStage(null);
    AdminTestUtils.updateSignal(
        modules2.getAuth(), modules2.getAdminServiceHandler(), TENANT_NAME, signalName, signal2);
    AdminTestUtils.deleteStream(tfosAdmin2, TENANT_NAME, contextName);

    // reload again and recreate the counter
    final var modules3 = TestModules.reload(SignalAdminTest.class + " rl4", connection);
    final var auth3 = modules3.getAuth();
    final var adminHandler3 = modules3.getAdminServiceHandler();
    final var tfosAdmin3 = modules3.getTfosAdmin();
    AdminTestUtils.createContext(auth3, adminHandler3, TENANT_NAME, context);
    final var recreatedContext = tfosAdmin3.getStream(TENANT_NAME, contextName);
    assertThat(recreatedContext, notNullValue());
    assertThat(recreatedContext.getContextAdjuster(), nullValue());

    AdminTestUtils.updateSignal(auth3, adminHandler3, TENANT_NAME, signalName, signal);
    final var recreatedSignal = tfosAdmin3.getStream(TENANT_NAME, signalName);
    assertThat(recreatedSignal, notNullValue());

    final var recreatedContext2 = tfosAdmin3.getStream(TENANT_NAME, contextName);
    assertThat(recreatedContext2, notNullValue());
    assertThat(recreatedContext2.getContextAdjuster(), notNullValue());

    // reload
    final var modules4 = TestModules.reload(SignalAdminTest.class + " rl5", connection);
    final var tfosAdmin4 = modules4.getTfosAdmin();

    final var reloadedContext2 = tfosAdmin4.getStream(TENANT_NAME, contextName);
    assertThat(reloadedContext2, notNullValue());
    assertThat(reloadedContext2.getContextAdjuster(), notNullValue());
  }

  private void checkEnrichment(
      StreamDesc signalDesc,
      String contextName,
      List<String> targetForeignKey,
      String targetContextAttribute) {
    assertThat(signalDesc.getPreprocesses(), notNullValue());
    assertThat(signalDesc.getPreprocesses().size(), is(1));
    final var pp = signalDesc.getPreprocesses().get(0);
    assertThat(pp, notNullValue());
    assertThat(pp.getForeignKey(), equalTo(targetForeignKey));
    assertThat(pp.getActions(), notNullValue());
    assertThat(pp.getActions().get(0).getContext(), is(contextName));
    assertThat(pp.getActions().get(0).getAttribute(), is(targetContextAttribute));
  }
}
