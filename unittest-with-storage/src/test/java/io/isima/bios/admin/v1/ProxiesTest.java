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

import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ProxiesTest {
  private static ObjectMapper mapper;
  private static AdminInternal admin;
  private static io.isima.bios.admin.Admin biosAdmin;
  private static final String tenantName = "proxyTests";

  private StreamConfig enrichedContextConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
    Bios2TestModules.startModulesWithoutMaintenance(ProxiesTest.class);
    admin = BiosModules.getAdminInternal();
    biosAdmin = BiosModules.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    AdminTestUtils.addTenant(admin, tenantName);
  }

  @After
  public void tearDown() throws Exception {
    AdminTestUtils.deleteTenantIgnoreError(admin, tenantName);
  }

  private static String supportingContextFile(int contextNum) {
    return "../server/bios-server/src/test/resources/ContextToEnrichContext"
        + String.format("%d", contextNum)
        + "Tfos.json";
  }

  // Using existing schema files from an unrelated feature to quickly get a lot of combinations for
  // testing proxies easily.
  public void setupForContextEnrichment() throws Throwable {
    // Set up contexts needed for context enrichment tests.
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(1));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(2));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(3));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(4));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(5));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(6));
    AdminTestUtils.populateStream(admin, tenantName, supportingContextFile(7));
    enrichedContextConfig =
        AdminTestUtils.getStreamConfig(
            "../server/bios-server/src/test/resources/EnrichedContextTfos.json");
  }

  @Test
  public void testProxyWithServerReloadAndStreamModifyDelete() throws Throwable {
    setupForContextEnrichment();
    admin.addStream(tenantName, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, enrichedContextConfig, RequestPhase.FINAL);

    var signalConfig =
        AdminTestUtils.getStreamConfig(
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);
    var tenant = admin.getTenant(tenantName);
    var c = admin.getStream(tenantName, "enrichedContext");
    var s = admin.getStream(tenantName, "patientVisits");
    // Verify, reload server, and verify again.
    for (int i = 0; i < 2; i++) {
      assertFalse(tenant.isProxyInformationDirty());
      assertEquals(14, s.getStreamNameProxy().shortValue());
      assertEquals(14, tenant.getMaxAllocatedStreamNameProxy().intValue());
      assertEquals(9, s.getMaxAttributeProxy().shortValue());
      verifyUnchangedAttributeProxies(c, s);
      assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
      assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());
      admin.reload();
      sleep(5);
    }

    // Add newAttribute1 at end.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 10,
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos-modified1.json");
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());

    // Reorder existing attributes patientId and resourceId, enriched attributes zipcode and
    // resourceTypeCode, delete attribute visitId and enriched attribute cost.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 10,
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos-modified2.json");
    assertNull(s.getAttributeProxyInfo().get("cost"));
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());

    // Add visitId and cost again.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 12,
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos-modified3.json");
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());
    assertEquals(11, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(12, s.getAttributeProxyInfo().get("cost").getProxy());

    // Delete the stream and re-create it.
    long timestamp = System.currentTimeMillis();
    admin.removeStream(tenantName, "patientVisits", RequestPhase.INITIAL, timestamp);
    admin.removeStream(tenantName, "patientVisits", RequestPhase.FINAL, timestamp);
    signalConfig =
        AdminTestUtils.getStreamConfig(
            "../server/bios-server/src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);
    tenant = admin.getTenant(tenantName);
    s = admin.getStream(tenantName, "patientVisits");
    // Verify, reload server, and verify again.
    for (int i = 0; i < 2; i++) {
      assertFalse(tenant.isProxyInformationDirty());
      assertEquals(15, s.getStreamNameProxy().shortValue());
      assertEquals(15, tenant.getMaxAllocatedStreamNameProxy().intValue());
      assertEquals(9, s.getMaxAttributeProxy().shortValue());
      verifyUnchangedAttributeProxies(c, s);
      assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
      assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());
      admin.reload();
    }
  }

  private void verifyUnchangedAttributeProxies(StreamDesc c, StreamDesc s) {
    assertEquals(13, c.getStreamNameProxy().shortValue());
    assertEquals(7, c.getMaxAttributeProxy().shortValue());
    assertEquals(1, c.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(2, c.getAttributeProxyInfo().get("resourcetypenum").getProxy());
    assertEquals(3, c.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(4, c.getAttributeProxyInfo().get("resourcetypecode").getProxy());
    assertEquals(5, c.getAttributeProxyInfo().get("myresourcecategory").getProxy());
    assertEquals(6, c.getAttributeProxyInfo().get("cost").getProxy());
    assertEquals(7, c.getAttributeProxyInfo().get("resourcevalue").getProxy());

    assertEquals(2, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(3, s.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(4, s.getAttributeProxyInfo().get("resourcetypenum").getProxy());
    assertEquals(5, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(6, s.getAttributeProxyInfo().get("resourcetypecode").getProxy());
    assertEquals(7, s.getAttributeProxyInfo().get("myresourcecategory").getProxy());
    assertEquals(9, s.getAttributeProxyInfo().get("resourcevalue").getProxy());
  }

  private StreamDesc modifySignalAndVerifyExistingProxies(short maxAttributeProxy, String jsonFile)
      throws Exception {
    sleep(10); // To ensure the new timestamp is higher than previous timestamp.
    var signalConfig = AdminTestUtils.getStreamConfig(jsonFile);
    admin.modifyStream(
        tenantName, "patientVisits", signalConfig, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(
        tenantName, "patientVisits", signalConfig, RequestPhase.FINAL, FORCE, Set.of());
    var tenant = admin.getTenant(tenantName);
    var c = admin.getStream(tenantName, "enrichedContext");
    var s = admin.getStream(tenantName, "patientVisits");
    assertFalse(tenant.isProxyInformationDirty());
    assertEquals(14, tenant.getMaxAllocatedStreamNameProxy().intValue());
    assertEquals(14, s.getStreamNameProxy().shortValue());
    assertEquals(maxAttributeProxy, s.getMaxAttributeProxy().shortValue());
    verifyUnchangedAttributeProxies(c, s);
    // System.out.println(TfosObjectMapperProvider.get().writeValueAsString(tenant));
    return s;
  }

  @Test
  @Ignore("modifyTenant is not supported anymore")
  public void testProxyWithModifyTenant() throws Throwable {
    String tenantName2 = "proxiesModifyTenantTest";
    AdminTestUtils.deleteTenantIgnoreError(admin, tenantName2);

    String tenantJson =
        new String(
            Files.readAllBytes(
                Paths.get("../unittest-with-storage/src/test/resources/tenant-proxies.json")));
    var tenantConfig = mapper.readValue(tenantJson, TenantConfig.class);
    tenantConfig.setVersion(System.currentTimeMillis());
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    var tenant = admin.getTenant(tenantName2);
    var s = admin.getStream(tenantName2, "proxySignal");
    // Verify, reload server, and verify again.
    for (int i = 0; i < 2; i++) {
      assertFalse(tenant.isProxyInformationDirty());
      assertEquals(5, s.getStreamNameProxy().shortValue());
      assertEquals(7, tenant.getMaxAllocatedStreamNameProxy().intValue());
      assertEquals(3, s.getMaxAttributeProxy().shortValue());
      assertEquals(1, s.getAttributeProxyInfo().get("first").getProxy());
      assertEquals(2, s.getAttributeProxyInfo().get("second").getProxy());
      assertEquals(3, s.getAttributeProxyInfo().get("state").getProxy());
      admin.reload();
      sleep(5);
    }

    tenantJson =
        new String(
            Files.readAllBytes(
                Paths.get(
                    "../unittest-with-storage/src/test/resources/tenant-proxies-modified1.json")));
    tenantConfig = mapper.readValue(tenantJson, TenantConfig.class);
    tenantConfig.setVersion(System.currentTimeMillis());
    admin.modifyTenant(tenantName2, tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.modifyTenant(tenantName2, tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    tenant = admin.getTenant(tenantName2);
    s = admin.getStream(tenantName2, "proxySignal");
    // Verify, reload server, and verify again.
    for (int i = 0; i < 2; i++) {
      assertFalse(tenant.isProxyInformationDirty());
      assertEquals(5, s.getStreamNameProxy().shortValue());
      assertEquals(7, tenant.getMaxAllocatedStreamNameProxy().intValue());
      assertEquals(4, s.getMaxAttributeProxy().shortValue());
      assertEquals(1, s.getAttributeProxyInfo().get("first").getProxy());
      assertEquals(3, s.getAttributeProxyInfo().get("state").getProxy());
      assertEquals(4, s.getAttributeProxyInfo().get("third").getProxy());
      admin.reload();
      sleep(5);
    }

    AdminTestUtils.deleteTenantIgnoreError(admin, tenantName2);
  }

  @Test
  public void testAttributeVersions() throws Throwable {
    final String contextName =
        AdminTestUtils.populateContext(
            biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v1.json");
    StreamDesc c = admin.getStream(tenantName, contextName);
    final long t1 = c.getVersion();

    final String signalName =
        AdminTestUtils.populateSignal(
            biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v1.json");
    StreamDesc s = admin.getStream(tenantName, signalName);
    final long t2 = s.getVersion();
    verifyUnchangedAttributeProxies2(c, s, t1, t2);
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(2, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(3, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(7, s.getMaxAttributeProxy().shortValue());
    assertEquals(t2, s.getAttributeProxyInfo().get("visitid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("patientid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("zipcode").getBaseVersion());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("visitid").getType());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("patientid").getType());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("zipcode").getType());
    System.out.println(mapper.writeValueAsString(s.getAttributeProxyInfo()));

    // Change patientId from Integer to Decimal - convertible change,
    // and zipcode from Integer to String - convertible change.
    AdminTestUtils.updateSignal(
        biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v2.json");
    s = admin.getStream(tenantName, signalName);
    final long t3 = s.getVersion();
    verifyUnchangedAttributeProxies2(c, s, t1, t2);
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(8, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(9, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(9, s.getMaxAttributeProxy().shortValue());
    // Version should change only for non-convertible datatype changes.
    assertEquals(t2, s.getAttributeProxyInfo().get("visitid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("patientid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("zipcode").getBaseVersion());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("visitid").getType());
    assertEquals(
        InternalAttributeType.DOUBLE, s.getAttributeProxyInfo().get("patientid").getType());
    assertEquals(InternalAttributeType.STRING, s.getAttributeProxyInfo().get("zipcode").getType());
    System.out.println(mapper.writeValueAsString(c.getAttributeProxyInfo()));
    System.out.println(mapper.writeValueAsString(s.getAttributeProxyInfo()));

    // Change patientId from Decimal to String - convertible change,
    AdminTestUtils.updateSignal(
        biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v3.json");
    s = admin.getStream(tenantName, signalName);
    final long t4 = s.getVersion();
    verifyUnchangedAttributeProxies2(c, s, t1, t2);
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(10, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(9, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(10, s.getMaxAttributeProxy().shortValue());
    // Version should change only for non-convertible datatype changes.
    assertEquals(t2, s.getAttributeProxyInfo().get("visitid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("patientid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("zipcode").getBaseVersion());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("visitid").getType());
    assertEquals(
        InternalAttributeType.STRING, s.getAttributeProxyInfo().get("patientid").getType());
    assertEquals(InternalAttributeType.STRING, s.getAttributeProxyInfo().get("zipcode").getType());
    System.out.println(mapper.writeValueAsString(s.getAttributeProxyInfo()));
  }

  private void verifyUnchangedAttributeProxies2(StreamDesc c, StreamDesc s, long t1, long t2) {
    assertEquals(1, c.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(2, c.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(3, c.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(4, c.getAttributeProxyInfo().get("attr3").getProxy());
    assertEquals(4, c.getMaxAttributeProxy().shortValue());
    assertEquals(t1, c.getAttributeProxyInfo().get("resourceid").getBaseVersion());
    assertEquals(t1, c.getAttributeProxyInfo().get("attr1").getBaseVersion());
    assertEquals(t1, c.getAttributeProxyInfo().get("attr2").getBaseVersion());
    assertEquals(t1, c.getAttributeProxyInfo().get("attr3").getBaseVersion());
    assertEquals(
        InternalAttributeType.STRING, c.getAttributeProxyInfo().get("resourceid").getType());
    assertEquals(InternalAttributeType.LONG, c.getAttributeProxyInfo().get("attr1").getType());
    assertEquals(InternalAttributeType.DOUBLE, c.getAttributeProxyInfo().get("attr2").getType());
    assertEquals(InternalAttributeType.STRING, c.getAttributeProxyInfo().get("attr3").getType());

    assertEquals(4, s.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(5, s.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(6, s.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(7, s.getAttributeProxyInfo().get("attr3").getProxy());
    assertEquals(t2, s.getAttributeProxyInfo().get("resourceid").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("attr1").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("attr2").getBaseVersion());
    assertEquals(t2, s.getAttributeProxyInfo().get("attr3").getBaseVersion());
    assertEquals(
        InternalAttributeType.STRING, s.getAttributeProxyInfo().get("resourceid").getType());
    assertEquals(InternalAttributeType.LONG, s.getAttributeProxyInfo().get("attr1").getType());
    assertEquals(InternalAttributeType.DOUBLE, s.getAttributeProxyInfo().get("attr2").getType());
    assertEquals(InternalAttributeType.STRING, s.getAttributeProxyInfo().get("attr3").getType());
  }

  @Ignore("BIOS-2338")
  @Test
  public void testCascadingAttributeDatatypeChange() throws Throwable {
    final String contextName =
        AdminTestUtils.populateContext(
            biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v1.json");
    StreamDesc c = admin.getStream(tenantName, contextName);
    final long t1 = c.getVersion();
    assertEquals(2, c.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(3, c.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(4, c.getMaxAttributeProxy().shortValue());

    final String signalName =
        AdminTestUtils.populateSignal(
            biosAdmin, tenantName, "../tests/resources/bios-signal-for-proxies-v1.json");
    StreamDesc s = admin.getStream(tenantName, signalName);
    final long t2 = s.getVersion();
    assertEquals(5, s.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(6, s.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(7, s.getMaxAttributeProxy().shortValue());
    verifyUnchangedAttributeProxies3(c, s);

    // Change attr1 from Integer to Decimal and attr2 from Decimal to Integer.
    // This should have a cascading effect on the signal.
    AdminTestUtils.updateContext(
        biosAdmin, tenantName, "../tests/resources/bios-context-for-proxies-v2.json");
    c = admin.getStream(tenantName, contextName);
    final long t3 = c.getVersion();
    s = admin.getStream(tenantName, signalName);
    verifyUnchangedAttributeProxies3(c, s);
    assertEquals(5, c.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(6, c.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(6, c.getMaxAttributeProxy().shortValue());

    assertEquals(8, s.getAttributeProxyInfo().get("attr1").getProxy());
    assertEquals(9, s.getAttributeProxyInfo().get("attr2").getProxy());
    assertEquals(9, s.getMaxAttributeProxy().shortValue());
    // TODO: add asserts for versions.
  }

  private void verifyUnchangedAttributeProxies3(StreamDesc c, StreamDesc s) {
    assertEquals(1, c.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(4, c.getAttributeProxyInfo().get("attr3").getProxy());

    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(2, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(3, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(4, s.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(7, s.getAttributeProxyInfo().get("attr3").getProxy());
  }
}
