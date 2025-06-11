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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.Admin;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichedAttribute;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminDependencyCheckTest {

  private static final String TENANT_NAME = "adminDependencyCheckTest";

  private static Admin admin;
  private static CassandraConnection connection;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(AdminDependencyCheckTest.class);
    admin = BiosModules.getAdmin();
    connection = BiosModules.getCassandraConnection();

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
    final String sessionToken = BiosModules.getAuth().createToken(now, expiry, userContext);

    AdminDependencyCheckTest.sessionToken = new SessionToken(sessionToken, false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Test
  public void testContextToSignalEnrichment() throws Exception {
    // make the remote context
    final var contextName = "remoteContext";
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

    // make the signal
    final var signalName = "enrichmentTest";
    final var signal = new SignalConfig();
    signal.setName(signalName);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("foreignKey", AttributeType.STRING),
            new AttributeConfig("productId", AttributeType.INTEGER)));
    signal.setEnrich(makeEnrichmentSignal(context, "foreignKey"));

    AdminTestUtils.populateContext(admin, TENANT_NAME, context);

    long timestamp = System.currentTimeMillis();
    admin.createSignal(TENANT_NAME, signal, RequestPhase.INITIAL, timestamp);
    admin.createSignal(TENANT_NAME, signal, RequestPhase.FINAL, timestamp);

    // modification #1, remove enrichment and remote context
    final var signal2 = new SignalConfig(signal);
    signal2.setEnrich(null);
    timestamp = System.currentTimeMillis();
    admin.updateSignal(TENANT_NAME, signalName, signal2, RequestPhase.INITIAL, timestamp);
    admin.updateSignal(TENANT_NAME, signalName, signal2, RequestPhase.FINAL, timestamp);

    timestamp = System.currentTimeMillis();
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.FINAL, timestamp);

    // reload admin entities and check if the reloaded signal has proper config
    final var admin2 = reloadAdmin();

    {
      final var out = admin2.getSignals(TENANT_NAME, true, true, false, List.of(signalName)).get(0);
      assertNull(out.getEnrich());
    }

    // modification #2, enrich the signal by another context
    final var contextName2 = "remoteContext2";
    final var context2 = new ContextConfig();
    context2.setName(contextName2);
    context2.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context2.setAttributes(
        List.of(
            new AttributeConfig("productId", AttributeType.INTEGER),
            new AttributeConfig("gtin", AttributeType.STRING),
            new AttributeConfig("categoryId", AttributeType.INTEGER)));
    context2.setPrimaryKey(List.of("productId"));
    context2.setAuditEnabled(false);

    final var signal3 = new SignalConfig(signal);
    signal3.setEnrich(makeEnrichmentSignal(context2, "productId"));

    AdminTestUtils.populateContext(admin2, TENANT_NAME, context2);

    timestamp = System.currentTimeMillis();
    admin2.updateSignal(TENANT_NAME, signalName, signal3, RequestPhase.INITIAL, timestamp);
    admin2.updateSignal(TENANT_NAME, signalName, signal3, RequestPhase.FINAL, timestamp);

    // reload admin entities again and check if the reloaded signal has proper config
    final var admin3 = reloadAdmin();

    {
      final var out = admin3.getSignals(TENANT_NAME, true, true, false, List.of(signalName)).get(0);
      assertNotNull(out.getEnrich());
      assertNotNull(out.getEnrich().getEnrichments());
      assertThat(out.getEnrich().getEnrichments().size(), is(1));
      assertThat(out.getEnrich().getEnrichments().get(0).getContextName(), is(contextName2));
      assertThat(out.getEnrich().getEnrichments().get(0).getForeignKey(), is(List.of("productId")));
      assertThat(
          out.getEnrich().getEnrichments().get(0).getContextAttributes().get(0).getAttributeName(),
          is("gtin"));
    }
  }

  @Test
  public void testContextToContextEnrichment() throws Exception {
    // make the remote context
    final var contextName = "remoteContextForContext";
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

    // make the enriched context
    final var enrichedContextName = "enrichmentContextTest";
    final var enrichedContext = new ContextConfig();
    enrichedContext.setName(enrichedContextName);
    enrichedContext.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    enrichedContext.setAttributes(
        List.of(
            new AttributeConfig("primaryKey", AttributeType.STRING),
            new AttributeConfig("anotherKey", AttributeType.STRING),
            new AttributeConfig("integerKey", AttributeType.INTEGER)));
    enrichedContext.setPrimaryKey(List.of("primaryKey"));
    enrichedContext.setEnrichments(
        List.of(makeEnrichmentContext(context, "anotherKey", "intValue")));

    // long timestamp = System.currentTimeMillis();
    AdminTestUtils.populateContext(admin, TENANT_NAME, context);
    AdminTestUtils.populateContext(admin, TENANT_NAME, enrichedContext);

    // modification #1, remove enrichment and remote context
    final var enrichedContext2 = new ContextConfig(enrichedContext);
    enrichedContext2.setEnrichments(null);
    long timestamp = System.currentTimeMillis();
    admin.updateContext(
        TENANT_NAME,
        enrichedContextName,
        enrichedContext2,
        RequestPhase.INITIAL,
        timestamp,
        Set.of());
    admin.updateContext(
        TENANT_NAME,
        enrichedContextName,
        enrichedContext2,
        RequestPhase.FINAL,
        timestamp,
        Set.of());

    timestamp = System.currentTimeMillis();
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.FINAL, timestamp);

    // reload admin entities and check if the reloaded signal has proper config
    final var admin2 = reloadAdmin();

    {
      final var out =
          admin2.getContexts(TENANT_NAME, true, true, false, List.of(enrichedContextName)).get(0);
      assertNull(out.getEnrichments());
    }

    // modification #2, enrich the signal by another context
    final var contextName2 = "remoteContext3";
    final var context2 = new ContextConfig();
    context2.setName(contextName2);
    context2.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context2.setAttributes(
        List.of(
            new AttributeConfig("productId", AttributeType.INTEGER),
            new AttributeConfig("gtin", AttributeType.STRING),
            new AttributeConfig("categoryId", AttributeType.INTEGER)));
    context2.setPrimaryKey(List.of("productId"));
    context2.setAuditEnabled(false);

    final var enrichedContext3 = new ContextConfig(enrichedContext);
    enrichedContext3.setEnrichments(List.of(makeEnrichmentContext(context2, "integerKey", "gtin")));

    AdminTestUtils.populateContext(admin2, TENANT_NAME, context2);

    timestamp = System.currentTimeMillis();
    admin2.updateContext(
        TENANT_NAME,
        enrichedContextName,
        enrichedContext3,
        RequestPhase.INITIAL,
        timestamp,
        Set.of());
    admin2.updateContext(
        TENANT_NAME,
        enrichedContextName,
        enrichedContext3,
        RequestPhase.FINAL,
        timestamp,
        Set.of());

    // reload admin entities again and check if the reloaded signal has proper config
    final var admin3 = reloadAdmin();

    {
      final var out =
          admin3.getContexts(TENANT_NAME, true, true, false, List.of(enrichedContextName)).get(0);
      assertNotNull(out.getEnrichments());
      assertThat(out.getEnrichments().size(), is(1));
      assertThat(out.getEnrichments().get(0).getForeignKey(), is(List.of("integerKey")));
      assertThat(out.getEnrichments().get(0).getEnrichedAttributes().get(0).getAs(), is("gtin"));
    }
  }

  @Test
  public void testLastNFeature() throws Exception {
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

    final var mapper = BiosObjectMapperProvider.get();
    final var context = mapper.readValue(contextSrc.replace("'", "\""), ContextConfig.class);
    final var contextName = context.getName();
    final var signal = mapper.readValue(signalSrc.replace("'", "\""), SignalConfig.class);
    final var signalName = signal.getName();

    AdminTestUtils.populateContext(admin, TENANT_NAME, context);

    // The signal has a LastN feature initially
    long timestamp = System.currentTimeMillis();
    admin.createSignal(TENANT_NAME, signal, RequestPhase.INITIAL, timestamp);
    admin.createSignal(TENANT_NAME, signal, RequestPhase.FINAL, timestamp);

    // Then remove it
    final var signal2 = new SignalConfig(signal);
    signal2.setPostStorageStage(null);
    timestamp = System.currentTimeMillis();
    admin.updateSignal(TENANT_NAME, signalName, signal2, RequestPhase.INITIAL, timestamp);
    admin.updateSignal(TENANT_NAME, signalName, signal2, RequestPhase.FINAL, timestamp);

    // Delete the previously depending context
    timestamp = System.currentTimeMillis();
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.FINAL, timestamp);

    // reload admin entities and check if the reloaded signal has proper config
    final var admin2 = reloadAdmin();

    {
      final var out = admin2.getSignals(TENANT_NAME, true, true, false, List.of(signalName)).get(0);
      assertNull(out.getPostStorageStage());
    }

    // recreate the feature with the same context name
    AdminTestUtils.populateContext(admin, TENANT_NAME, context);

    timestamp = System.currentTimeMillis();
    admin.updateSignal(TENANT_NAME, signalName, signal, RequestPhase.INITIAL, timestamp);
    admin.updateSignal(TENANT_NAME, signalName, signal, RequestPhase.FINAL, timestamp);

    // reload admin entities again
    final var admin3 = reloadAdmin();
    {
      final var out = admin3.getSignals(TENANT_NAME, true, true, false, List.of(signalName)).get(0);
      assertNotNull(out.getPostStorageStage());
      assertNotNull(out.getPostStorageStage().getFeatures());
      assertThat(out.getPostStorageStage().getFeatures().size(), is(1));
      assertThat(out.getPostStorageStage().getFeatures().get(0).getName(), is("lastFiveVisits"));
      assertThat(
          out.getPostStorageStage().getFeatures().get(0).getFeatureAsContextName(),
          is("lastVisits"));
    }
  }

  @Test
  public void testContextWithAudit() throws Exception {
    // make the context
    final var contextName = "contextWithAuditTest";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    context.setAttributes(
        List.of(
            new AttributeConfig("primaryKey", AttributeType.STRING),
            new AttributeConfig("productId", AttributeType.INTEGER)));
    context.setPrimaryKey(List.of("primaryKey"));
    context.setAuditEnabled(true);

    final var auditSignalName = "auditContextWithAuditTest";

    AdminTestUtils.populateContext(admin, TENANT_NAME, context);

    final var signal =
        admin.getSignals(TENANT_NAME, true, false, false, Set.of(auditSignalName)).get(0);

    // modification #1, disable audit
    final var context2 = new ContextConfig(context);
    context2.setAuditEnabled(false);
    long timestamp = System.currentTimeMillis();
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.INITIAL, timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.FINAL, timestamp, Set.of());

    timestamp = System.currentTimeMillis();
    admin.deleteSignal(TENANT_NAME, "audit" + contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteSignal(TENANT_NAME, "audit" + contextName, RequestPhase.FINAL, timestamp);

    // reload admin entities and check if the reloaded context has proper config
    final var admin2 = reloadAdmin();

    {
      final var out =
          admin2.getContexts(TENANT_NAME, true, true, false, List.of(contextName)).get(0);
      assertFalse(out.getAuditEnabled());
    }

    // modification #2, delete the context and create a new one with the same name with audit
    timestamp = System.currentTimeMillis();
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.FINAL, timestamp);

    final var context3 = new ContextConfig(context);

    context3.setAttributes(new ArrayList<>(context3.getAttributes()));
    context3.getAttributes().add(new AttributeConfig("thirdAttribute", AttributeType.DECIMAL));

    final var signal2 = new SignalConfig(signal);
    signal2.setAttributes(new ArrayList<>(signal2.getAttributes()));
    signal2.getAttributes().add(new AttributeConfig("thirdAttribute", AttributeType.DECIMAL));
    signal2.getAttributes().add(new AttributeConfig("prevThirdAttribute", AttributeType.DECIMAL));

    timestamp = System.currentTimeMillis();
    admin.createSignalAllowingInternalAttributes(
        TENANT_NAME, signal2, RequestPhase.INITIAL, timestamp, List.of("_operation"));
    admin.createSignalAllowingInternalAttributes(
        TENANT_NAME, signal2, RequestPhase.FINAL, timestamp, List.of("_operation"));

    timestamp = System.currentTimeMillis();
    admin.createContext(TENANT_NAME, context3, RequestPhase.INITIAL, timestamp);
    admin.createContext(TENANT_NAME, context3, RequestPhase.FINAL, timestamp);

    // reload admin entities again and check if the reloaded context has proper config
    final var admin3 = reloadAdmin();

    {
      final var out =
          admin3.getContexts(TENANT_NAME, true, true, false, List.of(contextName)).get(0);
      assertThat(out.getAttributes().size(), is(3));
      assertThat(out.getVersion(), is(timestamp));
    }

    // delete and reload
    timestamp = System.currentTimeMillis();
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteContext(TENANT_NAME, contextName, RequestPhase.FINAL, timestamp);

    final var admin4 = reloadAdmin();

    {
      assertThrows(
          NoSuchStreamException.class,
          () -> admin4.getContexts(TENANT_NAME, true, true, false, List.of(contextName)));
    }
  }

  @Test
  public void testRecoverAudit() throws Exception {
    // make the context
    final var contextName = "contextAuditRecoverTest";
    final var context = new ContextConfig();
    context.setName(contextName);
    context.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    final var productType = new AttributeConfig("productType", AttributeType.STRING);
    final var allowedValues =
        List.of(
            new AttributeValueGeneric("car", AttributeType.STRING),
            new AttributeValueGeneric("bike", AttributeType.STRING));
    productType.setAllowedValues(allowedValues);
    context.setAttributes(
        List.of(new AttributeConfig("primaryKey", AttributeType.STRING), productType));
    context.setPrimaryKey(List.of("primaryKey"));
    context.setAuditEnabled(true);

    final var auditSignalName = "auditContextAuditRecoverTest";

    AdminTestUtils.populateContext(admin, TENANT_NAME, context);

    final var signal =
        admin.getSignals(TENANT_NAME, true, false, false, Set.of(auditSignalName)).get(0);

    // reload admin entities and check if the reloaded context has proper config
    final var admin2 = reloadAdmin();

    {
      final var out =
          admin2.getContexts(TENANT_NAME, true, true, false, List.of(contextName)).get(0);
      assertTrue(out.getAuditEnabled());
    }

    // run recovery procedure
    final var context2 = new ContextConfig(context);
    context2.setAuditEnabled(false);
    long timestamp = System.currentTimeMillis();
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.INITIAL, timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.FINAL, timestamp, Set.of());

    timestamp = System.currentTimeMillis();
    admin.deleteSignal(TENANT_NAME, "audit" + contextName, RequestPhase.INITIAL, timestamp);
    admin.deleteSignal(TENANT_NAME, "audit" + contextName, RequestPhase.FINAL, timestamp);

    final var signal2 = new SignalConfig(signal);
    signal2.getAttributes().get(3).setAllowedValues(allowedValues);
    signal2.getAttributes().get(4).setAllowedValues(allowedValues);
    timestamp = System.currentTimeMillis();
    admin.createSignalAllowingInternalAttributes(
        TENANT_NAME, signal2, RequestPhase.INITIAL, timestamp, List.of("_operation"));
    admin.createSignalAllowingInternalAttributes(
        TENANT_NAME, signal2, RequestPhase.FINAL, timestamp, List.of("_operation"));

    context2.setAuditEnabled(true);
    timestamp = System.currentTimeMillis();
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.INITIAL, timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, contextName, context2, RequestPhase.FINAL, timestamp, Set.of());

    // reload admin entities again and check if the reloaded context has proper config
    final var admin3 = reloadAdmin();

    {
      final var out =
          admin3.getSignals(TENANT_NAME, true, true, false, List.of(auditSignalName)).get(0);
      assertThat(out.getAttributes().size(), is(5));
      assertNotNull(out.getAttributes().get(3).getAllowedValues());
      assertThat(out.getAttributes().get(3).getAllowedValues().size(), is(allowedValues.size()));
      assertNotNull(out.getAttributes().get(4).getAllowedValues());
      assertThat(out.getAttributes().get(4).getAllowedValues().size(), is(allowedValues.size()));
    }
  }

  // regression BIOS-5751
  @Test
  public void testEnrichByModifiedEnrichedContext() throws Exception {
    final String visitSrc =
        "{'contextName': 'visitContext',"
            + " 'missingAttributePolicy': 'Reject',"
            + " 'attributes': ["
            + "  {'attributeName': 'visitId', 'type': 'Integer'},"
            + "  {'attributeName': 'deviceType', 'type': 'String',"
            + "   'allowedValues': ['desktop', 'mobile.tablet', 'unknown']},"
            + "  {'attributeName': 'touchPoint',"
            + "   'type': 'String',"
            + "   'allowedValues': ['app_android',"
            + "    'app_ios',"
            + "    'desktop',"
            + "    'mobile',"
            + "    'web',"
            + "    'unknown']},"
            + "  {'attributeName': 'isNativePlatform', 'type': 'Boolean'},"
            + "  {'attributeName': 'ipAddr', 'type': 'String'},"
            + "  {'attributeName': 'deviceId', 'type': 'String'}"
            + " ],"
            + " 'primaryKey': ['visitId'],"
            + " 'missingLookupPolicy': 'FailParentLookup',"
            + " 'enrichments': [{'enrichmentName': 'ip2GeoEnrichment',"
            + "   'foreignKey': ['ipAddr'],"
            + "   'enrichedAttributes': [{'value': '_ip2geo.city'},"
            + "    {'value': '_ip2geo.continent'},"
            + "    {'value': '_ip2geo.isInEuropeanUnion'}]}],"
            + " 'auditEnabled': true}";

    final String pageSrc =
        "{'contextName': 'pageContext',"
            + " 'missingAttributePolicy': 'Reject',"
            + " 'attributes': [{'attributeName': 'pageId', 'type': 'String'},"
            + "  {'attributeName': 'pageHostName', 'type': 'String'},"
            + "  {'attributeName': 'pagePath', 'type': 'String'},"
            + "  {'attributeName': 'pageUrl', 'type': 'String'},"
            + "  {'attributeName': 'referrer', 'type': 'String'},"
            + "  {'attributeName': 'country', 'type': 'String'},"
            + "  {'attributeName': 'language', 'type': 'String'}"
            + " ],"
            + " 'primaryKey': ['pageId'],"
            + " 'missingLookupPolicy': 'FailParentLookup',"
            + " 'auditEnabled': false}";

    final String signalSrc =
        "{'signalName': 'checkoutPayments',"
            + " 'missingAttributePolicy': 'Reject',"
            + " 'attributes': [{'attributeName': 'ecommerceCoProductsId',"
            + "   'type': 'Integer'},"
            + "  {'attributeName': 'ecommerceCoProductsName',"
            + "   'type': 'String'},"
            + "  {'attributeName': 'ecommerceCoProductsCategory',"
            + "   'type': 'String'},"
            + "  {'attributeName': 'ecommerceCoProductsBrand',"
            + "   'type': 'String'},"
            + "  {'attributeName': 'ecommerceCoProductsVariant',"
            + "   'type': 'Integer'},"
            + "  {'attributeName': 'ecommerceCoProductsPrice',"
            + "   'type': 'Decimal'},"
            + "  {'attributeName': 'ecommerceCoProductsQuantity',"
            + "   'type': 'Integer'},"
            + "  {'attributeName': 'ecommerceCoProductsCoupon', 'type': 'String'},"
            + "  {'attributeName': 'visitId',"
            + "   'type': 'Integer'},"
            + "  {'attributeName': 'pageId',"
            + "   'type': 'String'}"
            + " ],"
            + " 'enrich': {'enrichments': [{'enrichmentName': 'visitEnrichment',"
            + "    'foreignKey': ['visitId'],"
            + "    'missingLookupPolicy': 'Reject',"
            + "    'contextName': 'visitContext',"
            + "    'contextAttributes': [{'attributeName': 'touchPoint'},"
            + "     {'attributeName': 'deviceType'},"
            + "     {'attributeName': 'city'}]},"
            + "   {'enrichmentName': 'pageEnrichment',"
            + "    'foreignKey': ['pageId'],"
            + "    'missingLookupPolicy': 'Reject',"
            + "    'contextName': 'pageContext',"
            + "    'contextAttributes': [{'attributeName': 'pagePath'},"
            + "     {'attributeName': 'pageHostName'},"
            + "     {'attributeName': 'pageUrl'},"
            + "     {'attributeName': 'country'},"
            + "     {'attributeName': 'language'}]}]},"
            + " 'postStorageStage': {'features': [{'featureName': 'byProductCatBrandVar',"
            + "    'dimensions': ['ecommerceCoProductsId',"
            + "     'ecommerceCoProductsName',"
            + "     'ecommerceCoProductsCategory',"
            + "     'ecommerceCoProductsBrand',"
            + "     'ecommerceCoProductsVariant'],"
            + "    'attributes': ['ecommerceCoProductsPrice', 'ecommerceCoProductsQuantity'],"
            + "    'featureInterval': 300000}]}}";

    final var mapper = BiosObjectMapperProvider.get();

    final var auditSignalName = "auditVisitContext";
    final var auditSignal = new SignalConfig(auditSignalName);
    auditSignal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    auditSignal.setAttributes(
        List.of(
            new AttributeConfig("_operation", AttributeType.STRING),
            new AttributeConfig("visitId", AttributeType.INTEGER),
            new AttributeConfig("deviceType", AttributeType.STRING),
            new AttributeConfig("touchPoint", AttributeType.STRING),
            new AttributeConfig("isNativePlatform", AttributeType.BOOLEAN),
            new AttributeConfig("ipAddr", AttributeType.STRING),
            new AttributeConfig("deviceId", AttributeType.STRING),
            new AttributeConfig("prevVisitId", AttributeType.INTEGER),
            new AttributeConfig("prevDeviceType", AttributeType.STRING),
            new AttributeConfig("prevTouchPoint", AttributeType.STRING),
            new AttributeConfig("prevIsNativePlatform", AttributeType.BOOLEAN),
            new AttributeConfig("prevIpAddr", AttributeType.STRING),
            new AttributeConfig("prevDeviceId", AttributeType.STRING)));

    final var visit = mapper.readValue(visitSrc.replace("'", "\""), ContextConfig.class);
    AdminTestUtils.populateContext(admin, TENANT_NAME, visit);

    final var page = mapper.readValue(pageSrc.replace("'", "\""), ContextConfig.class);
    AdminTestUtils.populateContext(admin, TENANT_NAME, page);

    final var signal = mapper.readValue(signalSrc.replace("'", "\""), SignalConfig.class);
    long timestamp = System.currentTimeMillis();
    admin.createSignal(TENANT_NAME, signal, RequestPhase.INITIAL, timestamp);
    admin.createSignal(TENANT_NAME, signal, RequestPhase.FINAL, timestamp);

    // visit.setAuditEnabled(false);
    visit.setTtl(3600000L * 10 * 24);
    timestamp = System.currentTimeMillis();
    admin.updateContext(
        TENANT_NAME, visit.getName(), visit, RequestPhase.INITIAL, timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, visit.getName(), visit, RequestPhase.FINAL, timestamp, Set.of());

    // Try loading admin. If there's no exception, the test is fine.
    reloadAdmin();
  }

  /**
   * Makes an enrichment entry.
   *
   * <p>The method picks up the first non-primary key attribute and use it as an enriched attribute
   *
   * @param context the remote context
   * @param foreignKey foreign key name in the signal
   * @return Created enrichment entry wrapped by EnrichConfig
   */
  private EnrichConfig makeEnrichmentSignal(ContextConfig context, String foreignKey) {
    final var contextName = context.getName();
    final var enrich = new EnrichConfig();
    final var enrichment = new EnrichmentConfigSignal();
    enrichment.setName("enrichment");
    enrichment.setForeignKey(List.of(foreignKey));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextName(contextName);
    final var attr = new EnrichmentAttribute();
    attr.setAttributeName(context.getAttributes().get(1).getName());
    final var type = context.getAttributes().get(1).getType();
    Object defaultValue = type == AttributeType.INTEGER ? 0 : "";
    attr.setFillIn(new AttributeValueGeneric(defaultValue, type));
    enrichment.setContextAttributes(List.of(attr));
    enrich.setEnrichments(List.of(enrichment));
    return enrich;
  }

  private EnrichmentConfigContext makeEnrichmentContext(
      ContextConfig context, String foreignKey, String as) {
    final var contextName = context.getName();
    final var enrichment = new EnrichmentConfigContext();
    enrichment.setName("enrichment");
    enrichment.setForeignKey(List.of(foreignKey));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.FAIL_PARENT_LOOKUP);
    final var attr = new EnrichedAttribute();
    final var valueName =
        String.format("%s.%s", context.getName(), context.getAttributes().get(1).getName());
    attr.setValue(valueName);
    attr.setAs(as);
    final var type = context.getAttributes().get(1).getType();
    Object defaultValue = type == AttributeType.INTEGER ? 0 : "";
    attr.setFillIn(new AttributeValueGeneric(defaultValue, type));
    enrichment.setEnrichedAttributes(List.of(attr));
    return enrichment;
  }

  private Admin reloadAdmin() throws ApplicationException {
    final var modules = TestModules.reload(AdminDependencyCheckTest.class + " rl", connection);
    return modules.getAdmin();
  }
}
