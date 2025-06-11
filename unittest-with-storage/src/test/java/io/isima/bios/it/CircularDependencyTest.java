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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.Admin;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.AppType;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.EnrichConfig;
import io.isima.bios.models.EnrichmentAttribute;
import io.isima.bios.models.EnrichmentConfigSignal;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.StringUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CircularDependencyTest {

  private static final String TENANT_NAME = "crossDependencyTest";

  private static Admin admin;
  private static CassandraConnection connection;
  private static AdminServiceHandler adminHandler;

  private ContextConfig productPlain;
  private ContextConfig productWithDependency;

  private ContextConfig pdpViewFacPlain;
  private ContextConfig pdpViewFacWithDependency;

  private static SessionToken sessionToken;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(CircularDependencyTest.class);
    admin = BiosModules.getAdmin();
    connection = BiosModules.getCassandraConnection();
    adminHandler = BiosModules.getAdminServiceHandler();

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

    CircularDependencyTest.sessionToken = new SessionToken(sessionToken, false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    final var mapper = BiosObjectMapperProvider.get();
    productWithDependency =
        AdminTestUtils.loadJson("../tests/resources/products.json", ContextConfig.class);

    productPlain = AdminTestUtils.loadJson("../tests/resources/products.json", ContextConfig.class);
    productPlain.setEnrichments(null);

    pdpViewFacWithDependency =
        AdminTestUtils.loadJson("../tests/resources/pdpviews-fac.json", ContextConfig.class);

    pdpViewFacPlain =
        AdminTestUtils.loadJson("../tests/resources/pdpviews-fac.json", ContextConfig.class);
    pdpViewFacPlain.setEnrichments(null);
  }

  // Regression BIOS-4018
  @Test
  public void makeCrossDependencyOverVersions() throws Exception {
    final var contextNameProduct = productPlain.getName();
    final var contextNamePdp = pdpViewFacPlain.getName();

    final FanRouter<ContextConfig, ContextConfig> fanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            pdpViewFacPlain,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNamePdp),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            productWithDependency,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNameProduct),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    admin.updateContext(
        TENANT_NAME, contextNameProduct, productPlain, RequestPhase.INITIAL, ++timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, contextNameProduct, productPlain, RequestPhase.FINAL, timestamp, Set.of());

    admin.updateContext(
        TENANT_NAME,
        contextNamePdp,
        pdpViewFacWithDependency,
        RequestPhase.INITIAL,
        ++timestamp,
        Set.of());
    admin.updateContext(
        TENANT_NAME,
        contextNamePdp,
        pdpViewFacWithDependency,
        RequestPhase.FINAL,
        timestamp,
        Set.of());

    // reload admin entities and check if the reloaded signal has proper config
    final var admin2 = reloadAdmin();

    {
      final var out =
          admin2.getContexts(
              TENANT_NAME, true, true, false, List.of(contextNameProduct, contextNamePdp));
      assertThat(out.size(), is(2));
    }
  }

  @Test
  public void testCrossDependencyDetection() throws Exception {
    final var contextNameProduct = productPlain.getName() + "2";
    final var contextNamePdp = pdpViewFacPlain.getName() + "2";
    renameContexts(contextNameProduct, contextNamePdp);

    final FanRouter<ContextConfig, ContextConfig> fanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            pdpViewFacPlain,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNamePdp),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            productWithDependency,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNameProduct),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    // ensure circular dependency detection
    {
      final long nextTimestamp = ++timestamp;
      final var exception =
          assertThrows(
              ConstraintViolationException.class,
              () ->
                  admin.updateContext(
                      TENANT_NAME,
                      contextNamePdp,
                      pdpViewFacWithDependency,
                      RequestPhase.INITIAL,
                      nextTimestamp,
                      Set.of()));
      assertThat(exception.getMessage(), containsString("Cyclic dependencies are not allowed"));
    }

    // reload admin entities to ensure that internal dependency graph didn't break
    final var admin2 = reloadAdmin();

    {
      final var out =
          admin2.getContexts(
              TENANT_NAME, true, true, false, List.of(contextNameProduct, contextNamePdp));
      assertThat(out.size(), is(2));
    }
  }

  /*
   * 1)
   * PDP -> audit
   * Prod
   * 2)
   * PDP -> audit
   * Prod *-> PDP
   * 3)
   * PDP -> audit *-> Prod -> PDP ::SHOULD BE REJECTED
   * 4)
   * PDP -> audit
   * Prod *(-> PDP -- remove this link)
   * 5)
   * PDP -> audit *-> Prod (-> PDP : removed)
   */
  @Test
  public void testCircularDependencyDetection() throws Exception {
    final var contextNameProduct = productPlain.getName() + "3";
    final var contextNamePdp = pdpViewFacPlain.getName() + "3";
    renameContexts(contextNameProduct, contextNamePdp);

    final FanRouter<ContextConfig, ContextConfig> fanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            pdpViewFacPlain,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNamePdp),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            productWithDependency,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNameProduct),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    final String pdpAuditName = "audit" + contextNamePdp;
    final var pdpAudit =
        admin.getSignals(TENANT_NAME, true, true, false, List.of(pdpAuditName)).get(0);

    final var enrichment = new EnrichmentConfigSignal();
    enrichment.setName("byProduct");
    enrichment.setForeignKey(List.of("productId"));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextName(contextNameProduct);
    final var contextAttribute = new EnrichmentAttribute();
    contextAttribute.setAttributeName("brand");
    contextAttribute.setFillIn(new AttributeValueGeneric("MISSING", AttributeType.STRING));
    enrichment.setContextAttributes(List.of(contextAttribute));
    final var enrich = new EnrichConfig();
    enrich.setEnrichments(List.of(enrichment));
    pdpAudit.setEnrich(enrich);

    // ensure circular dependency detection
    {
      final long nextTimestamp = ++timestamp;
      final var exception =
          assertThrows(
              ConstraintViolationException.class,
              () ->
                  admin.updateSignalAllowingInternalAttributes(
                      TENANT_NAME,
                      pdpAuditName,
                      pdpAudit,
                      RequestPhase.INITIAL,
                      nextTimestamp,
                      Set.of("_operation"),
                      Set.of()));
      System.out.println(exception);
      assertThat(exception.getMessage(), containsString("Cyclic dependencies are not allowed"));
    }

    // cut a part of the circular chain
    admin.updateContext(
        TENANT_NAME, contextNameProduct, productPlain, RequestPhase.INITIAL, ++timestamp, Set.of());
    admin.updateContext(
        TENANT_NAME, contextNameProduct, productPlain, RequestPhase.FINAL, timestamp, Set.of());

    // try previous audit signal update again, this should succeed
    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.INITIAL,
        ++timestamp,
        Set.of("_operation"),
        Set.of());
    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.FINAL,
        timestamp,
        Set.of("_operation"),
        Set.of());

    // Now there's a dependency ring across versions (which is not actually a circular dependency).
    // Reload admin entities to ensure this wouldn't cause a false positive circular dependency.
    final var admin2 = reloadAdmin();
    {
      final var out =
          admin2.getContexts(
              TENANT_NAME, true, true, false, List.of(contextNameProduct, contextNamePdp));
      assertThat(out.size(), is(2));
    }
  }

  /*
   * 1)
   * Prod
   * PDP -> audit
   * 2)
   * PDP -> audit *-> Prod
   * 3)
   * PDP -> audit -> Prod *-> PDP ::SHOULD BE REJECTED
   * 4)
   * PDP -> audit *(-> Prod -- remove this link)
   * Prod
   * 5)
   * Prod *-> PDP -> audit (-> Prod : removed)
   */
  @Test
  public void testCircularDependencyDetectedInAnotherPlace() throws Exception {
    final var contextNameProduct = productPlain.getName() + "4";
    final var contextNamePdp = pdpViewFacPlain.getName() + "4";
    renameContexts(contextNameProduct, contextNamePdp);

    final FanRouter<ContextConfig, ContextConfig> fanRouter = TestUtils.makeDummyFanRouter();
    final FanRouter<SignalConfig, SignalConfig> auditFanRouter = TestUtils.makeDummyFanRouter();
    long timestamp = System.currentTimeMillis();
    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            pdpViewFacPlain,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNamePdp),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    adminHandler
        .createContext(
            sessionToken,
            TENANT_NAME,
            productPlain,
            RequestPhase.INITIAL,
            timestamp,
            Optional.of(fanRouter),
            StringUtils.prefixToCamelCase("audit", contextNameProduct),
            Optional.of(auditFanRouter),
            TestUtils.makeGenericState())
        .get();

    final String pdpAuditName = "audit" + contextNamePdp;
    final var pdpAudit =
        admin.getSignals(TENANT_NAME, true, true, false, List.of(pdpAuditName)).get(0);

    final var enrichment = new EnrichmentConfigSignal();
    enrichment.setName("byProduct");
    enrichment.setForeignKey(List.of("productId"));
    enrichment.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    enrichment.setContextName(contextNameProduct);
    final var contextAttribute = new EnrichmentAttribute();
    contextAttribute.setAttributeName("brand");
    contextAttribute.setFillIn(new AttributeValueGeneric("MISSING", AttributeType.STRING));
    enrichment.setContextAttributes(List.of(contextAttribute));
    final var enrich = new EnrichConfig();
    enrich.setEnrichments(List.of(enrichment));
    pdpAudit.setEnrich(enrich);

    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.INITIAL,
        ++timestamp,
        Set.of("_operation"),
        Set.of());
    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.FINAL,
        timestamp,
        Set.of("_operation"),
        Set.of());

    // try to create a dependency ring, should be rejected
    {
      final long nextTimestamp = ++timestamp;
      final var exception =
          assertThrows(
              ConstraintViolationException.class,
              () ->
                  admin.updateContext(
                      TENANT_NAME,
                      contextNameProduct,
                      productWithDependency,
                      RequestPhase.INITIAL,
                      nextTimestamp,
                      Set.of()));
      System.out.println(exception);
      assertThat(exception.getMessage(), containsString("Cyclic dependencies are not allowed"));
    }

    // cut a part of the circular chain by disabling enrichment in the audit signal
    pdpAudit.setEnrich(null);
    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.INITIAL,
        ++timestamp,
        Set.of("_operation"),
        Set.of());
    admin.updateSignalAllowingInternalAttributes(
        TENANT_NAME,
        pdpAuditName,
        pdpAudit,
        RequestPhase.FINAL,
        timestamp,
        Set.of("_operation"),
        Set.of());

    // try previous context update again, this should succeed
    admin.updateContext(
        TENANT_NAME,
        contextNameProduct,
        productWithDependency,
        RequestPhase.INITIAL,
        ++timestamp,
        Set.of());
    admin.updateContext(
        TENANT_NAME,
        contextNameProduct,
        productWithDependency,
        RequestPhase.FINAL,
        timestamp,
        Set.of());

    // Now there's a dependency ring across versions (which is not actually a circular dependency).
    // Reload admin entities to ensure this wouldn't cause a false positive circular dependency.
    final var admin2 = reloadAdmin();
    {
      final var out =
          admin2.getContexts(
              TENANT_NAME, true, true, false, List.of(contextNameProduct, contextNamePdp));
      assertThat(out.size(), is(2));
    }
  }

  private void renameContexts(String contextNameProduct, String contextNamePdp) {
    pdpViewFacPlain.setName(contextNamePdp);
    pdpViewFacWithDependency.setName(contextNamePdp);
    pdpViewFacWithDependency
        .getEnrichments()
        .get(0)
        .getEnrichedAttributes()
        .get(0)
        .setValue(contextNameProduct + ".productName");
    pdpViewFacWithDependency
        .getEnrichments()
        .get(0)
        .getEnrichedAttributes()
        .get(1)
        .setValue(contextNameProduct + ".brand");

    productPlain.setName(contextNameProduct);
    productWithDependency.setName(contextNameProduct);
    productWithDependency
        .getEnrichments()
        .get(0)
        .getEnrichedAttributes()
        .get(0)
        .setValue(contextNamePdp + ".cityCollection");
  }

  private Admin reloadAdmin() throws ApplicationException {
    final var modules = TestModules.reload(CircularDependencyTest.class, connection);
    return modules.getAdmin();
  }
}
