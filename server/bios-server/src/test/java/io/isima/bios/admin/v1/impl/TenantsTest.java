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
package io.isima.bios.admin.v1.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamComparators;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.TimeInterval;
import io.isima.bios.models.TimeunitType;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantsTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  @Test
  public void testListTenantNames() {
    final Tenants tenants = new Tenants();
    long timestamp = System.currentTimeMillis();
    tenants.addTenant(new TenantDesc("California", timestamp, false));
    tenants.addTenant(new TenantDesc("Illinois", timestamp, false));
    tenants.addTenant(new TenantDesc("ca", timestamp, false));
    tenants.addTenant(new TenantDesc("Oregon", timestamp, false));
    tenants.addTenant(new TenantDesc("arizona", timestamp, false));
    tenants.addTenant(new TenantDesc("Utah", timestamp, false));
    tenants.addTenant(new TenantDesc("colorado", timestamp, false));
    assertEquals(
        Arrays.asList("arizona", "ca", "California", "colorado", "Illinois", "Oregon", "Utah"),
        tenants.listTenantNames());
    tenants.addTenant(new TenantDesc("colorado", ++timestamp, true));
    assertEquals(
        Arrays.asList("arizona", "ca", "California", "Illinois", "Oregon", "Utah"),
        tenants.listTenantNames());
  }

  @Test
  public void getTenanTest() {
    final Tenants tenants = new Tenants();
    final String name = "testTenant";
    long timestamp = System.currentTimeMillis();

    // add a tenant
    tenants.addTenant(new TenantDesc(name, timestamp, false));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp), retrieved.getVersion());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNotNull(config);
      assertEquals(name, config.getName());
      assertEquals(Long.valueOf(timestamp), config.getVersion());
      assertFalse(config.isDeleted());
    }
    // add another tenant with newer timestamp
    tenants.addTenant(new TenantDesc(name, timestamp + 10, false));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 10), retrieved.getVersion());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNotNull(config);
      assertEquals(name, config.getName());
      assertEquals(Long.valueOf(timestamp + 10), config.getVersion());
      assertFalse(config.isDeleted());
    }
    // add another tenant with a timestamp that is not latest
    tenants.addTenant(new TenantDesc(name, timestamp + 8, false));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 10), retrieved.getVersion());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNotNull(config);
      assertEquals(name, config.getName());
      assertEquals(Long.valueOf(timestamp + 10), config.getVersion());
      assertFalse(config.isDeleted());
    }
    // delete the tenant but the timestamp is not the latest
    tenants.addTenant(new TenantDesc(name, timestamp + 9, true));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 10), retrieved.getVersion());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNotNull(config);
      assertEquals(name, config.getName());
      assertEquals(Long.valueOf(timestamp + 10), config.getVersion());
      assertFalse(config.isDeleted());
    }
    // delete the tenant but the timestamp is not the latest
    tenants.addTenant(new TenantDesc(name, timestamp + 11, true));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNull(retrieved);
      final TenantDesc retrieved2 = tenants.getTenantDescOrNull(name, true);
      assertNotNull(retrieved2);
      assertEquals(name, retrieved2.getName());
      assertEquals(Long.valueOf(timestamp + 11), retrieved2.getVersion());
      assertTrue(retrieved2.isDeleted());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNull(config);
    }
    // add another version
    tenants.addTenant(new TenantDesc(name, timestamp + 12, false));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name.toUpperCase(), false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 12), retrieved.getVersion());
      assertFalse(retrieved.isDeleted());
      final TenantDesc retrieved2 = tenants.getTenantDescOrNull(name.toUpperCase(), true);
      assertNotNull(retrieved2);
      assertEquals(name, retrieved2.getName());
      assertEquals(Long.valueOf(timestamp + 12), retrieved2.getVersion());
      assertFalse(retrieved2.isDeleted());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNotNull(config);
      assertEquals(name, config.getName());
      assertEquals(Long.valueOf(timestamp + 12), config.getVersion());
      assertFalse(config.isDeleted());
    }
    // overwrite with the same version
    tenants.addTenant(new TenantDesc(name, timestamp + 12, true));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNull(retrieved);
      final TenantDesc retrieved2 = tenants.getTenantDescOrNull(name.toUpperCase(), true);
      assertNotNull(retrieved2);
      assertEquals(name, retrieved2.getName());
      assertEquals(Long.valueOf(timestamp + 12), retrieved2.getVersion());
      assertTrue(retrieved2.isDeleted());

      final TenantConfig config = tenants.getTenantConfigOrNull(name);
      assertNull(config);
    }

    // try getting an unregistered tenant descriptor.
    assertNull(tenants.getTenantDescOrNull("not_registered", false));
    assertNull(tenants.getTenantDescOrNull("not_registered", true));

    tenants.addTenant(new TenantDesc("one", timestamp + 20, false));
    tenants.addTenant(new TenantDesc("two", timestamp + 20, false));
    tenants.addTenant(new TenantDesc("three", timestamp + 20, false));
    tenants.addTenant(new TenantDesc("four", timestamp + 20, false));
    tenants.addTenant(new TenantDesc("five", timestamp + 20, false));

    // check the internal TenantDesc list
    {
      final List<TenantDesc> tenantList = tenants.tenants.get(name.toLowerCase());
      assertEquals(6, tenantList.size());
    }
    // try cleanup
    tenants.trimOldTenants(timestamp + 10);
    {
      final List<TenantDesc> tenantList = tenants.tenants.get(name.toLowerCase());
      assertEquals(2, tenantList.size());
    }
    // try complete clean
    tenants.trimOldTenants(timestamp + 12);
    {
      final List<TenantDesc> tenantList = tenants.tenants.get(name.toLowerCase());
      assertNull(tenantList);
    }
    // Add another tenant and try cleanup. The entry should not be removed.
    tenants.addTenant(new TenantDesc(name, timestamp + 14, false));
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 14), retrieved.getVersion());
      assertFalse(retrieved.isDeleted());
    }
    tenants.trimOldTenants(timestamp + 18);
    {
      final TenantDesc retrieved = tenants.getTenantDescOrNull(name, false);
      assertNotNull(retrieved);
      assertEquals(name, retrieved.getName());
      assertEquals(Long.valueOf(timestamp + 14), retrieved.getVersion());
      assertFalse(retrieved.isDeleted());
    }
  }

  @Test
  public void testActionDescEquals() {
    ActionDesc left = new ActionDesc();
    ActionDesc right = new ActionDesc();
    assertTrue(StreamComparators.equals(left, right));
    left.setActionType(ActionType.MERGE);
    assertFalse(StreamComparators.equals(left, right));
    right.setActionType(ActionType.MERGE);
    assertTrue(StreamComparators.equals(left, right));
    right.setContext("mystream");
    assertFalse(StreamComparators.equals(left, right));
    left.setContext("mystreamx");
    assertFalse(StreamComparators.equals(left, right));
    left.setContext("myStream");
    assertTrue(StreamComparators.equals(left, right));
    left.setAttribute("abc");
    assertFalse(StreamComparators.equals(left, right));
    right.setAttribute("ABC");
    assertTrue(StreamComparators.equals(left, right));
  }

  @Test
  public void testPreprocessEquals() {
    PreprocessDesc left = new PreprocessDesc();
    PreprocessDesc right = new PreprocessDesc();

    // empty
    assertTrue(StreamComparators.equals(left, right));

    // name
    right.setName("mypp");
    assertFalse(StreamComparators.equals(left, right));
    left.setName("MY_PP");
    assertFalse(StreamComparators.equals(left, right));
    left.setName("MyPp");
    assertTrue(StreamComparators.equals(left, right));

    // foreign key
    left.setCondition("referenceKey");
    assertFalse(StreamComparators.equals(left, right));
    right.setCondition("reference_key");
    assertFalse(StreamComparators.equals(left, right));
    right.setCondition("ReferenceKey");
    assertTrue(StreamComparators.equals(left, right));

    // new foreign key
    right.setCondition(null);
    right.setForeignKey(List.of("referenceKey"));
    assertTrue(StreamComparators.equals(left, right));
    left.setForeignKey(List.of("reference_key"));
    assertFalse(StreamComparators.equals(left, right));
    left.setForeignKey(List.of("referenceKey"));
    assertTrue(StreamComparators.equals(left, right));

    left.setCondition("key1");
    left.setForeignKey(null);
    right.setCondition(null);
    right.setForeignKey(List.of("key1", "key2"));
    assertFalse(StreamComparators.equals(left, right));
    left.setCondition(null);
    left.setForeignKey(List.of("key1"));
    assertFalse(StreamComparators.equals(left, right));
    left.setForeignKey(List.of("key2", "key1"));
    assertFalse(StreamComparators.equals(left, right));
    left.setForeignKey(List.of("key1", "key2"));
    assertTrue(StreamComparators.equals(left, right));

    // missing lookup policy
    left.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    assertFalse(StreamComparators.equals(left, right));
    right.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertFalse(StreamComparators.equals(left, right));
    left.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertTrue(StreamComparators.equals(left, right));

    // actions
    ActionDesc la = new ActionDesc();
    ActionDesc ra = new ActionDesc();
    la.setAttribute("attr");
    ra.setAttribute("attrN");
    left.addAction(la);
    assertFalse(StreamComparators.equals(left, right));
    right.addAction(ra);
    assertFalse(StreamComparators.equals(left, right));
    right.getActions().clear();
    ra.setAttribute("attr");
    right.addAction(ra);
    assertTrue(StreamComparators.equals(left, right));
  }

  @Test
  public void testAttributeDescEquals() {
    AttributeDesc left = new AttributeDesc("hello", InternalAttributeType.STRING);
    AttributeDesc right = new AttributeDesc("HELLO", InternalAttributeType.STRING);
    assertTrue(StreamComparators.equals(left, null, right, null));
    assertTrue(
        StreamComparators.equals(
            left, MissingAttributePolicyV1.STRICT, right, MissingAttributePolicyV1.STRICT));
    assertFalse(
        StreamComparators.equals(
            left, MissingAttributePolicyV1.STRICT,
            right, MissingAttributePolicyV1.USE_DEFAULT));
    assertFalse(StreamComparators.equals(left, MissingAttributePolicyV1.STRICT, right, null));

    right.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertTrue(
        StreamComparators.equals(
            left, MissingAttributePolicyV1.USE_DEFAULT, right, MissingAttributePolicyV1.STRICT));

    AttributeDesc ldup = left.duplicate();
    ldup.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertTrue(
        StreamComparators.equals(
            ldup, MissingAttributePolicyV1.STRICT, right, MissingAttributePolicyV1.USE_DEFAULT));

    right = new AttributeDesc("hellow", InternalAttributeType.STRING);
    assertFalse(StreamComparators.equals(left, null, right, null));
    right = new AttributeDesc("hello", InternalAttributeType.INT);
    assertFalse(StreamComparators.equals(left, null, right, null));
  }

  @Test
  public void testStreamConfigEquals() {
    Long timestamp = System.currentTimeMillis();
    StreamDesc left = new StreamDesc("mystream", timestamp);
    StreamDesc right = new StreamDesc("mystream", timestamp);
    assertTrue(StreamComparators.equals(left, right));
    left.addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    assertFalse(StreamComparators.equals(left, right));
    right.addAttribute(new AttributeDesc("hello", InternalAttributeType.STRING));
    assertTrue(StreamComparators.equals(left, right));
    left.addAttribute(new AttributeDesc("second", InternalAttributeType.STRING));
    right.addAttribute(new AttributeDesc("second", InternalAttributeType.INT));
    assertFalse(StreamComparators.equals(left, right));
    left.getAttributes().clear();
    right.getAttributes().clear();
    assertTrue(StreamComparators.equals(left, right));
    // additional attributes do not affect
    left.addAdditionalAttribute(new AttributeDesc("second", InternalAttributeType.STRING));
    right.addAdditionalAttribute(new AttributeDesc("second", InternalAttributeType.INT));
    assertTrue(StreamComparators.equals(left, right));

    left.addPreprocess(new PreprocessDesc("pp1"));
    assertFalse(StreamComparators.equals(left, right));
    right.addPreprocess(new PreprocessDesc("pp1"));
    assertTrue(StreamComparators.equals(left, right));

    // parent, version, and deleted flags do not affect
    left.setParent(new TenantDesc("temp1", System.currentTimeMillis(), false));
    right.setParent(new TenantDesc("temp2", System.currentTimeMillis(), false));
    left.setVersion(123L);
    right.setVersion(456L);
    left.setDeleted(true);
    assertTrue(StreamComparators.equals(left, right));

    // missingValuePolicy
    left.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    right.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertFalse(StreamComparators.equals(left, right));
    right.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);
    assertTrue(StreamComparators.equals(left, right));
    left.addAttribute(new AttributeDesc("hello", InternalAttributeType.INET));
    left.addAttribute(new AttributeDesc("world", InternalAttributeType.UUID));
    right.addAttribute(new AttributeDesc("hello", InternalAttributeType.INET));
    right.addAttribute(new AttributeDesc("world", InternalAttributeType.UUID));
    assertTrue(StreamComparators.equals(left, right));
    left.getAttributes().get(0).setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertFalse(StreamComparators.equals(left, right));
    left.getAttributes().get(1).setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    right.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    assertTrue(StreamComparators.equals(left, right));

    var actionLeft = new ActionDesc();
    actionLeft.setActionType(ActionType.MERGE);
    actionLeft.setAs("CN");
    var actionRight = actionLeft.duplicate();
    actionRight.setAs("CountryName");
    var preProcessLeft = new PreprocessDesc();
    preProcessLeft.addAction(actionLeft);
    var preProcessRight = new PreprocessDesc();
    preProcessRight.addAction(actionRight);
    left.addPreprocess(preProcessLeft);
    right.addPreprocess(preProcessRight);
    assertFalse(StreamComparators.equals(left, right));
  }

  @Test
  public void testStreamConfigEnrichContextAttributesEquals() {
    Long timestamp = System.currentTimeMillis();
    StreamDesc left = new StreamDesc("mystream", timestamp);
    StreamDesc right = new StreamDesc("mystream", timestamp);
    assertTrue(StreamComparators.equals(left, right));

    var actionLeft = new ActionDesc();
    actionLeft.setActionType(ActionType.MERGE);
    actionLeft.setDefaultValue("IN");
    var actionRight = actionLeft.duplicate();
    actionRight.setDefaultValue("CN");
    var preProcessLeft = new PreprocessDesc();
    preProcessLeft.addAction(actionLeft);
    var preProcessRight = new PreprocessDesc();
    preProcessRight.addAction(actionRight);
    left.addPreprocess(preProcessLeft);
    right.addPreprocess(preProcessRight);
    assertFalse(StreamComparators.equals(left, right));
  }

  @Test
  public void testStreamConfigEqualsView()
      throws JsonParseException, JsonMappingException, IOException {
    final String src =
        "{"
            + "  'name': 'postprocess_stream',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttribute', 'type': 'string'},"
            + "    {'name': 'int', 'type': 'int'},"
            + "    {'name': 'number', 'type': 'number'},"
            + "    {'name': 'double', 'type': 'double'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'EmptyAttributeViewGroupIntNum',"
            + "    'groupBy': ['int','number'],"
            + "    'attributes': []"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'EmptyAttributeViewGroupIntNum',"
            + "    'rollups': [{"
            + "      'name': 'EmptyAttribute_rollup',"
            + "      'interval': {'value': 5,'timeunit': 'minute'},"
            + "      'horizon': {'value': 15,'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig streamConfig1 =
        mapper.readValue(src.replaceAll("'", "\""), StreamConfig.class);
    final StreamConfig streamConfig2 = streamConfig1.duplicate();

    assertTrue(StreamComparators.equals(streamConfig1, streamConfig2));
    streamConfig2
        .getPostprocesses()
        .get(0)
        .getRollups()
        .get(0)
        .setHorizon(new TimeInterval(5, TimeunitType.MINUTE));

    assertFalse(StreamComparators.equals(streamConfig1, streamConfig2));

    {
      final StreamConfig streamConfig3 = streamConfig1.duplicate();
      streamConfig3.getViews().get(0).setGroupBy(Arrays.asList("int", "string"));
      assertFalse(StreamComparators.equals(streamConfig1, streamConfig3));
    }
    {
      final StreamConfig streamConfig3 = streamConfig1.duplicate();
      streamConfig3.getViews().get(0).setGroupBy(Arrays.asList("Int", "Number"));
      assertTrue(StreamComparators.equals(streamConfig1, streamConfig3));
    }
    {
      final StreamConfig streamConfig3 = streamConfig1.duplicate();
      final StreamConfig streamConfig4 = streamConfig1.duplicate();
      streamConfig3.getViews().get(0).setAttributes(Arrays.asList("attr1", "attr2"));
      streamConfig4.getViews().get(0).setAttributes(Arrays.asList("attr1", "attr2"));
      assertTrue(StreamComparators.equals(streamConfig3, streamConfig4));

      streamConfig4.getViews().get(0).setAttributes(Arrays.asList("attr1"));
      assertFalse(StreamComparators.equals(streamConfig3, streamConfig4));

      streamConfig4.getViews().get(0).setAttributes(Arrays.asList("Attr1", "Attr2"));
      assertTrue(StreamComparators.equals(streamConfig3, streamConfig4));
    }

    final StreamConfig streamConfig4 = streamConfig1.duplicate();
    assertTrue(StreamComparators.equals(streamConfig1, streamConfig4));
    streamConfig4
        .getPostprocesses()
        .get(0)
        .getRollups()
        .get(0)
        .setInterval(new TimeInterval(15, TimeunitType.MINUTE));
    assertFalse(StreamComparators.equals(streamConfig1, streamConfig4));

    final StreamConfig streamConfig5 = streamConfig1.duplicate();
    var alerts = new AlertConfig();
    alerts.setName("testAlerts");
    alerts.setCondition("(1 AND 1)");
    alerts.setWebhookUrl("https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422");
    var ad = new ArrayList<AlertConfig>();
    ad.add(alerts);
    streamConfig5.getPostprocesses().get(0).getRollups().get(0).setAlerts(ad);
    assertFalse(StreamComparators.equals(streamConfig1, streamConfig5));
    var anotherAlert = alerts.duplicate();
    anotherAlert.setWebhookUrl("https://webhook.site/99743393-3a47-473f-8676-319e8c5d9433");
    ad = new ArrayList<AlertConfig>();
    ad.add(anotherAlert);
    var streamConfig6 = streamConfig5.duplicate();
    streamConfig6.getPostprocesses().get(0).getRollups().get(0).setAlerts(ad);
    assertFalse(StreamComparators.equals(streamConfig5, streamConfig6));
  }

  @Test
  public void testTenantConfigEquals() {
    TenantConfig left = new TenantConfig();
    TenantConfig right = new TenantConfig();
    assertTrue(Tenants.equals(left, right));
    left.setName("tenant");
    assertFalse(Tenants.equals(left, right));
    right.setName("tenantx");
    assertFalse(Tenants.equals(left, right));
    right.setName("Tenant");
    assertTrue(Tenants.equals(left, right));

    left.addStream(new StreamConfig("mystream"));
    assertFalse(Tenants.equals(left, right));
    right.addStream(new StreamConfig("my_stream"));
    assertFalse(Tenants.equals(left, right));
    right.getStreams().clear();
    right.addStream(new StreamConfig("mystream"));
    assertTrue(Tenants.equals(left, right));

    // version and deleted flags do not affect
    left.setVersion(123L);
    right.setVersion(456L);
    left.setDeleted(true);
    assertTrue(Tenants.equals(left, right));
  }
}
