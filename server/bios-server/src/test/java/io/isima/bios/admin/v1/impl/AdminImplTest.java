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

import static java.lang.Thread.sleep;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.InetAddresses;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.errors.exception.StreamAlreadyExistsException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsConstants;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.AttributeModAllowance;
import io.isima.bios.models.EnrichedAttribute;
import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.preprocess.JoinProcessor;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class AdminImplTest {

  private static ObjectMapper objectMapper;
  private static String tenantName2 = "context_enrichment";

  private AdminImpl admin;
  private StreamConfig enrichedContextConfig;

  @BeforeClass
  public static void setUpClass() {
    objectMapper = TfosObjectMapperProvider.get();
  }

  @Before
  public void setUp() throws FileReadException {
    admin = new AdminImpl(null, new MetricsStreamProvider());
  }

  @Test
  public void processCompiler() throws TfosException, ApplicationException {

    final String tenantName = "geolocation_demo";
    final String signalStreamName = "accesses";
    final String contextStreamName = "geolocation";
    final String keyAttr = "ip";

    long timestamp = System.currentTimeMillis();
    TenantConfig tenantConfig = new TenantConfig(tenantName);
    tenantConfig.setVersion(timestamp);

    StreamConfig signalConfig = new StreamConfig(signalStreamName).setVersion(timestamp);
    signalConfig.addAttribute(new AttributeDesc(keyAttr, InternalAttributeType.INET));
    //
    PreprocessDesc join = new PreprocessDesc();
    join.setName("ip_to_geolocation");
    join.setCondition(keyAttr);
    join.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    ActionDesc action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("city");
    action.setContext(contextStreamName);
    join.addAction(action);
    signalConfig.addPreprocess(join);
    //
    tenantConfig.addStream(signalConfig);

    StreamConfig context =
        new StreamConfig(contextStreamName, StreamType.CONTEXT).setVersion(timestamp);
    context.addAttribute(new AttributeDesc("ip_address", InternalAttributeType.INET));
    context.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    context.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    context.setPrimaryKey(List.of("ip_address"));
    tenantConfig.addStream(context);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    StreamDesc registeredStream = admin.getStream(tenantName, signalStreamName);
    assertNotNull(registeredStream);
    assertNotNull(registeredStream.getPreprocessStages());
    assertEquals(1, registeredStream.getPreprocessStages().size());
    final ProcessStage stage = registeredStream.getPreprocessStages().get(0);
    assertEquals("JoinProcessor", stage.getProcess().getClass().getSimpleName());
    final JoinProcessor process = (JoinProcessor) stage.getProcess();
    assertEquals(List.of(keyAttr), process.getForeignKeyNames());
    assertEquals(1, process.getActions().size());
    final ActionDesc actionDesc = process.getActions().get(0);
    assertEquals(ActionType.MERGE, actionDesc.getActionType());
    assertEquals("city", actionDesc.getAttribute());
  }

  @Test
  public void testInitStreamConfigUseDefaultBasic()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'missingValuePolicy': 'use_default',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string', 'defaultValue': 'no value'},"
            + "    {'name': 'booleanAttr', 'type': 'boolean', 'defaultValue': true},"
            + "    {'name': 'intAttr', 'type': 'int', 'defaultValue': 10},"
            + "    {'name': 'numberAttr', 'type': 'number', 'defaultValue': 1234567890123456789012345},"
            + "    {'name': 'doubleAttr', 'type': 'double', 'defaultValue': 9.81},"
            + "    {'name': 'inetAttr', 'type': 'inet', 'defaultValue': '203.45.67.89'},"
            + "    {'name': 'dateAttr', 'type': 'date', 'defaultValue': '2018-09-15'},"
            + "    {'name': 'timestampAttr', 'type': 'timestamp',"
            + "     'defaultValue': '2018-09-10T12:34:56.789Z'},"
            + "    {'name': 'uuidAttr', 'type': 'uuid',"
            + "     'defaultValue': '1d201970-bac6-11e8-96f8-529269fb1459'}"
            + "  ]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc.replaceAll("'", "\""));
    admin.initStreamConfig(tenant, signal, Set.of());
    assertEquals("testDefault", signal.getName());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, signal.getMissingValuePolicy());
    final List<AttributeDesc> attrs = signal.getAttributes();
    assertEquals(9, attrs.size());
    for (AttributeDesc attr : attrs) {
      assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attr.getMissingValuePolicy());
    }
    int index = 0;
    assertEquals("no value", attrs.get(index++).getInternalDefaultValue());
    assertEquals(Boolean.TRUE, attrs.get(index++).getInternalDefaultValue());
    assertEquals(Integer.valueOf(10), attrs.get(index++).getInternalDefaultValue());
    assertEquals(
        new BigInteger("1234567890123456789012345"), attrs.get(index++).getInternalDefaultValue());
    assertEquals(Double.valueOf(9.81), attrs.get(index++).getInternalDefaultValue());
    assertEquals(
        InetAddresses.forString("203.45.67.89"), attrs.get(index++).getInternalDefaultValue());
    assertEquals(LocalDate.of(2018, 9, 15), attrs.get(index++).getInternalDefaultValue());
    assertEquals(new Date(1536582896789L), attrs.get(index++).getInternalDefaultValue());
    assertEquals(
        UUID.fromString("1d201970-bac6-11e8-96f8-529269fb1459"),
        attrs.get(index++).getInternalDefaultValue());
  }

  @Test
  public void testInitStreamConfigUseDefaultSetPerAttribute()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': 'no value'},"
            + "    {'name': 'booleanAttr', 'type': 'boolean', 'missingValuePolicy': 'strict'},"
            + "    {'name': 'intAttr', 'type': 'int'},"
            + "    {'name': 'numberAttr', 'type': 'number',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': 1234567890123456789012345},"
            + "    {'name': 'doubleAttr', 'type': 'double',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': 9.81},"
            + "    {'name': 'inetAttr', 'type': 'inet',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': '203.45.67.89'},"
            + "    {'name': 'dateAttr', 'type': 'date',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': '2018-09-15'},"
            + "    {'name': 'timestampAttr', 'type': 'timestamp',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': '2018-09-10T12:34:56.789Z'},"
            + "    {'name': 'uuidAttr', 'type': 'uuid', 'missingValuePolicy': 'use_default',"
            + "     'defaultValue': '1d201970-bac6-11e8-96f8-529269fb1459'}"
            + "  ]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
    assertEquals("testDefault", signal.getName());
    assertEquals(MissingAttributePolicyV1.STRICT, signal.getMissingValuePolicy());
    final List<AttributeDesc> attrs = signal.getAttributes();
    assertEquals(9, attrs.size());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(0).getMissingValuePolicy());
    assertEquals("no value", attrs.get(0).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.STRICT, attrs.get(1).getMissingValuePolicy());
    assertEquals(MissingAttributePolicyV1.STRICT, attrs.get(2).getMissingValuePolicy());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(3).getMissingValuePolicy());
    assertEquals(
        new BigInteger("1234567890123456789012345"), attrs.get(3).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(4).getMissingValuePolicy());
    assertEquals(Double.valueOf(9.81), attrs.get(4).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(5).getMissingValuePolicy());
    assertEquals(InetAddresses.forString("203.45.67.89"), attrs.get(5).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(6).getMissingValuePolicy());
    assertEquals(LocalDate.of(2018, 9, 15), attrs.get(6).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(7).getMissingValuePolicy());
    assertEquals(new Date(1536582896789L), attrs.get(7).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(8).getMissingValuePolicy());
    assertEquals(
        UUID.fromString("1d201970-bac6-11e8-96f8-529269fb1459"),
        attrs.get(8).getInternalDefaultValue());
  }

  @Test
  public void testInitStreamConfigStrictSetPerAttribute()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'missingValuePolicy': 'use_default',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string', 'defaultValue': 'no value'},"
            + "    {'name': 'booleanAttr', 'type': 'boolean', 'missingValuePolicy': 'strict'},"
            + "    {'name': 'intAttr', 'type': 'int', 'missingValuePolicy': 'strict'},"
            + "    {'name': 'numberAttr', 'type': 'number', 'defaultValue': 1234567890123456789012345},"
            + "    {'name': 'doubleAttr', 'type': 'double', 'defaultValue': 9.81},"
            + "    {'name': 'inetAttr', 'type': 'inet', 'defaultValue': '203.45.67.89'},"
            + "    {'name': 'dateAttr', 'type': 'date',"
            + "     'missingValuePolicy': 'use_default', 'defaultValue': '2018-09-15'},"
            + "    {'name': 'timestampAttr', 'type': 'timestamp',"
            + "     'defaultValue': '2018-09-10T12:34:56.789Z'},"
            + "    {'name': 'uuidAttr', 'type': 'uuid', 'missingValuePolicy': 'use_default',"
            + "     'defaultValue': '1d201970-bac6-11e8-96f8-529269fb1459'}"
            + "  ]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
    assertEquals("testDefault", signal.getName());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, signal.getMissingValuePolicy());
    final List<AttributeDesc> attrs = signal.getAttributes();
    assertEquals(9, attrs.size());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(0).getMissingValuePolicy());
    assertEquals("no value", attrs.get(0).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.STRICT, attrs.get(1).getMissingValuePolicy());
    assertEquals(MissingAttributePolicyV1.STRICT, attrs.get(2).getMissingValuePolicy());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(3).getMissingValuePolicy());
    assertEquals(
        new BigInteger("1234567890123456789012345"), attrs.get(3).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(4).getMissingValuePolicy());
    assertEquals(Double.valueOf(9.81), attrs.get(4).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(5).getMissingValuePolicy());
    assertEquals(InetAddresses.forString("203.45.67.89"), attrs.get(5).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(6).getMissingValuePolicy());
    assertEquals(LocalDate.of(2018, 9, 15), attrs.get(6).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(7).getMissingValuePolicy());
    assertEquals(new Date(1536582896789L), attrs.get(7).getInternalDefaultValue());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, attrs.get(8).getMissingValuePolicy());
    assertEquals(
        UUID.fromString("1d201970-bac6-11e8-96f8-529269fb1459"),
        attrs.get(8).getInternalDefaultValue());
  }

  @Test
  public void testInitStreamConfigUseDefaultConstraintCheck()
      throws JsonParseException, JsonMappingException, IOException, TfosException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'missingValuePolicy': 'use_default',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'}"
            + "  ]"
            + "}";
    final StreamDesc context = decodeConfig(signalSrc);
    try {
      admin.initStreamConfig(tenant, context, Set.of());
      fail("exception must happen");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation: Property 'defaultValue' must be set"
              + " when missing value policy is 'use_default';"
              + " tenant=testInitStreamConfigUsingDefault, signal=testDefault,"
              + " attribute=stringAttr",
          e.getMessage());
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInitStreamConfigValidDefaultStringContextKey()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String contextSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'use_default',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string', 'defaultValue': 'n/a'},"
            + "    {'name': 'intAttr', 'type': 'int', 'defaultValue': 0}"
            + "  ]"
            + "}";
    final StreamDesc context = decodeConfig(contextSrc);
    admin.initStreamConfig(tenant, context, Set.of());
    assertEquals("n/a", context.getAttributes().get(0).getInternalDefaultValue());
    assertEquals(0, context.getAttributes().get(1).getInternalDefaultValue());
  }

  @Test
  public void testInitStreamConfigInvalidDefaultStringContextKey()
      throws JsonParseException, JsonMappingException, IOException, TfosException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String contextSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'use_default',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string', 'defaultValue': ''},"
            + "    {'name': 'intAttr', 'type': 'int', 'defaultValue': 0}"
            + "  ]"
            + "}";
    final StreamDesc signal = decodeConfig(contextSrc);
    try {
      admin.initStreamConfig(tenant, signal, Set.of());
      fail("exception must happen");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation: Default value of primary key may not be empty;"
              + " tenant=testInitStreamConfigUsingDefault, context=testDefault",
          e.getMessage());
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInitStreamConfigValidContextLookup()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String contextSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intLookupAttr', 'type': 'int'},"
            + "    {'name': 'dateLookupAttr', 'type': 'date'}"
            + "  ],"
            + "  'primaryKey': ['stringAttr']"
            + "}";
    final StreamDesc context = decodeConfig(contextSrc);
    tenant.addStream(context);
    final String signalSrc =
        "{"
            + "  'name': 'signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intAttr', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'join',"
            + "    'condition': 'stringAttr',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': ["
            + "      {'actionType': 'merge', 'context': 'testDefault', 'attribute': 'intLookupAttr'},"
            + "      {'actionType': 'merge', 'context': 'testDefault', 'attribute': 'dateLookupAttr',"
            + "       'missingLookupPolicy': 'use_default', 'defaultValue': '2001-02-10'}"
            + "    ]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
    assertNotNull(signal.getPreprocesses());
    assertEquals(1, signal.getPreprocesses().size());
    assertEquals(2, signal.getPreprocesses().get(0).getActions().size());
    assertEquals(
        MissingAttributePolicyV1.STRICT,
        signal.getPreprocesses().get(0).getActions().get(0).getMissingLookupPolicy());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        signal.getPreprocesses().get(0).getActions().get(1).getMissingLookupPolicy());
    assertEquals(
        LocalDate.of(2001, 2, 10),
        signal.getPreprocesses().get(0).getActions().get(1).getInternalDefaultValue());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigContextLookupMissingDefault()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String contextSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intLookupAttr', 'type': 'int'},"
            + "    {'name': 'dateLookupAttr', 'type': 'date'}"
            + "  ],"
            + "  'primaryKey': ['stringAttr']"
            + "}";
    final StreamDesc context = decodeConfig(contextSrc);
    tenant.addStream(context);
    final String signalSrc =
        "{"
            + "  'name': 'signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intAttr', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'join',"
            + "    'condition': 'stringAttr',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': ["
            + "      {'actionType': 'merge', 'context': 'testDefault', 'attribute': 'intLookupAttr'},"
            + "      {'actionType': 'merge', 'context': 'testDefault', 'attribute': 'dateLookupAttr',"
            + "       'missingLookupPolicy': 'use_default'}"
            + "    ]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigContextLookupToContextKey()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigUsingDefault", System.currentTimeMillis(), false);
    final String contextSrc =
        "{"
            + "  'name': 'testDefault',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intLookupAttr', 'type': 'int'}"
            + "  ],"
            + "  'primaryKey': ['stringAttr']"
            + "}";
    final StreamDesc context = decodeConfig(contextSrc);
    tenant.addStream(context);
    final String signalSrc =
        "{"
            + "  'name': 'signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'intAttr', 'type': 'int'}"
            + "  ],"
            + "  'preprocesses': [{"
            + "    'name': 'join',"
            + "    'condition': 'stringAttr',"
            + "    'missingLookupPolicy': 'strict',"
            + "    'actions': ["
            + "      {'actionType': 'merge', 'context': 'testDefault', 'attribute': 'stringsAttr'}"
            + "    ]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test
  public void testInitStreamConfigForContextSharing()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testContextShareTenant", System.currentTimeMillis(), false);

    final long timestamp = System.currentTimeMillis();
    final StreamDesc context1 =
        new StreamDesc("geolocation", StreamType.CONTEXT).setVersion(timestamp);
    context1.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    context1.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    context1.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    context1.setPrimaryKey(List.of("ip"));
    tenant.addStream(context1);

    final StreamDesc context2 =
        new StreamDesc("identity", StreamType.CONTEXT).setVersion(timestamp);
    context2.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    context2.addAttribute(new AttributeDesc("user_name", InternalAttributeType.STRING));
    context2.addAttribute(new AttributeDesc("group_name", InternalAttributeType.STRING));
    context2.setPrimaryKey(List.of("ip"));
    tenant.addStream(context2);
    admin.addTenant(tenant.toTenantConfig(), RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant.toTenantConfig(), RequestPhase.FINAL, timestamp);

    final StreamDesc signal = new StreamDesc("testDefault", timestamp);
    signal.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    signal.addAttribute(new AttributeDesc("user_name", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("group_name", InternalAttributeType.STRING));
    signal.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);

    final AttributeDesc attr1 = new AttributeDesc("appliance_id", InternalAttributeType.STRING);
    final AttributeDesc attr2 = new AttributeDesc("source_ip", InternalAttributeType.INET);
    final AttributeDesc attr3 = new AttributeDesc("source_port", InternalAttributeType.INT);
    final AttributeDesc attr4 = new AttributeDesc("endpoint_ip", InternalAttributeType.INET);
    signal.addAttribute(attr1);
    signal.addAttribute(attr2);
    signal.addAttribute(attr3);
    signal.addAttribute(attr4);

    final ActionDesc actionDescOne = new ActionDesc();
    actionDescOne.setActionType(ActionType.MERGE);
    actionDescOne.setAttribute("user_name");
    actionDescOne.setAs("src_user_name");
    actionDescOne.setContext("identity");

    final ActionDesc actionDescTwo = new ActionDesc();
    actionDescTwo.setActionType(ActionType.MERGE);
    actionDescTwo.setAttribute("group_name");
    actionDescTwo.setContext("identity");
    actionDescTwo.setAs("src_group_name");

    final PreprocessDesc preprocess = new PreprocessDesc("add_source_ip_context");
    preprocess.addAction(actionDescOne);
    preprocess.addAction(actionDescTwo);
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocess.setCondition("source_ip");

    final ActionDesc actionDescthree = new ActionDesc();
    actionDescthree.setActionType(ActionType.MERGE);
    actionDescthree.setAttribute("user_name");
    actionDescthree.setAs("ep_user_name");
    actionDescthree.setContext("identity");

    final ActionDesc actionDescFour = new ActionDesc();
    actionDescFour.setActionType(ActionType.MERGE);
    actionDescFour.setAttribute("group_name");
    actionDescFour.setContext("identity");
    actionDescFour.setAs("ep_group_name");

    final PreprocessDesc preprocessTwo = new PreprocessDesc("add_source_ip_context");
    preprocessTwo.addAction(actionDescthree);
    preprocessTwo.addAction(actionDescFour);
    preprocessTwo.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocessTwo.setCondition("endpoint_ip");
    signal.addPreprocess(preprocessTwo);
    tenant.addStream(signal);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test
  public void testInitStreamConfigForContextSharingSameAsAttribute()
      throws JsonParseException,
          JsonMappingException,
          IOException,
          TfosException,
          ApplicationException {
    final TenantDesc tenant =
        new TenantDesc("testContextShareTenantWrongPrep", System.currentTimeMillis(), false);

    final long timestamp = System.currentTimeMillis();
    final StreamDesc context1 =
        new StreamDesc("geolocation", StreamType.CONTEXT).setVersion(timestamp);
    context1.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    context1.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    context1.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    context1.setPrimaryKey(List.of("ip"));
    tenant.addStream(context1);

    final StreamDesc context2 =
        new StreamDesc("identity", StreamType.CONTEXT).setVersion(timestamp);
    context2.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    context2.addAttribute(new AttributeDesc("user_name", InternalAttributeType.STRING));
    context2.addAttribute(new AttributeDesc("group_name", InternalAttributeType.STRING));
    context2.setPrimaryKey(List.of("ip"));
    tenant.addStream(context2);
    admin.addTenant(tenant.toTenantConfig(), RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant.toTenantConfig(), RequestPhase.FINAL, timestamp);

    final StreamDesc signal = new StreamDesc("testDefault", timestamp);
    signal.addAttribute(new AttributeDesc("ip", InternalAttributeType.INET));
    signal.addAttribute(new AttributeDesc("user_name", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("group_name", InternalAttributeType.STRING));
    signal.setMissingValuePolicy(MissingAttributePolicyV1.STRICT);

    final AttributeDesc attr1 = new AttributeDesc("appliance_id", InternalAttributeType.STRING);
    final AttributeDesc attr2 = new AttributeDesc("source_ip", InternalAttributeType.INET);
    final AttributeDesc attr3 = new AttributeDesc("source_port", InternalAttributeType.INT);
    final AttributeDesc attr4 = new AttributeDesc("endpoint_ip", InternalAttributeType.INET);
    signal.addAttribute(attr1);
    signal.addAttribute(attr2);
    signal.addAttribute(attr3);
    signal.addAttribute(attr4);

    final ActionDesc actionDescOne = new ActionDesc();
    actionDescOne.setActionType(ActionType.MERGE);
    actionDescOne.setAttribute("user_name");
    actionDescOne.setAs("src_user_name");
    actionDescOne.setContext("identity");

    final ActionDesc actionDescTwo = new ActionDesc();
    actionDescTwo.setActionType(ActionType.MERGE);
    actionDescTwo.setAttribute("group_name");
    actionDescTwo.setContext("identity");
    actionDescTwo.setAs("src_group_name");

    final PreprocessDesc preprocess = new PreprocessDesc("add_source_ip_context");
    preprocess.addAction(actionDescOne);
    preprocess.addAction(actionDescTwo);
    preprocess.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocess.setCondition("source_ip");

    final ActionDesc actionDescthree = new ActionDesc();
    actionDescthree.setActionType(ActionType.MERGE);
    actionDescthree.setAttribute("user_name");
    actionDescthree.setAs("ep_user_name");
    actionDescthree.setContext("identity");

    final ActionDesc actionDescFour = new ActionDesc();
    actionDescFour.setActionType(ActionType.MERGE);
    actionDescFour.setAttribute("group_name");
    actionDescFour.setContext("identity");
    actionDescFour.setAs("");

    final PreprocessDesc preprocessTwo = new PreprocessDesc("add_source_ip_context");
    preprocessTwo.addAction(actionDescthree);
    preprocessTwo.addAction(actionDescFour);
    preprocessTwo.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    preprocessTwo.setCondition("endpoint_ip");
    signal.addPreprocess(preprocessTwo);
    tenant.addStream(signal);
    try {
      admin.initStreamConfig(tenant, signal, Set.of());
    } catch (ConstraintViolationException cev) {
      assertEquals(
          "Constraint violation: The 'as' property may not be empty;"
              + " tenant=testContextShareTenantWrongPrep, signal=testDefault,"
              + " preprocess=add_source_ip_context, attribute=group_name",
          cev.getMessage());
    }
  }

  @Test
  public void testInitStreamConfigTryToRollupNonAddable() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigTryToRollupNonAddable", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'invalidRollup',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'skid', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_skid_state',"
            + "    'groupBy': ['skid'],"
            + "    'attributes': ['state']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_skid_state',"
            + "    'rollups': [{"
            + "      'name': 'rollup_skid_state',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigRollupNamesConflict() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigRollupNamesConflict", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }, {"
            + "    'name': 'view_by_name',"
            + "    'groupBy': ['name'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_conflicting_name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view_by_name',"
            + "    'rollups': [{"
            + "      'name': 'rollup_conflicting_name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigRollupNamesConflictCaseInsensitive() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigRollupNamesConflictCaseInsensitive",
            System.currentTimeMillis(),
            false);
    final String signalSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }, {"
            + "    'name': 'view_by_name',"
            + "    'groupBy': ['name'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_conflicting_name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view_by_name',"
            + "    'rollups': [{"
            + "      'name': 'Rollup_Conflicting_Name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigRollupAndStreamConflict() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigRollupAndStreamConflict", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }, {"
            + "    'name': 'view_by_name',"
            + "    'groupBy': ['name'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_phone',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'view_by_name',"
            + "    'rollups': [{"
            + "      'name': 'product_order',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test
  public void testAddConflictRollupsBetweenTwoStreams()
      throws TfosException,
          ApplicationException,
          JsonParseException,
          JsonMappingException,
          IOException {
    final TenantDesc tenant =
        new TenantDesc(
            "testAddConflictRollupsBetweenTwoStreams", System.currentTimeMillis(), false);
    final String initialSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_phone',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamDesc initialSignal = decodeConfig(initialSrc);

    tenant.addStream(initialSignal);

    final long timestamp = System.currentTimeMillis();

    admin.addTenant(tenant.toTenantConfig(), RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant.toTenantConfig(), RequestPhase.FINAL, timestamp);

    // verify constraint violation: new rollup conflicts with existing rollup
    final String secondSrc =
        "{"
            + "  'name': 'product_order_second',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone2',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone2',"
            + "    'rollups': [{"
            + "      'name': 'Rollup_By_Phone',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamDesc secondSignal = decodeConfig(secondSrc);
    try {
      admin.addStream(tenant.getName(), secondSignal, RequestPhase.INITIAL);
      admin.addStream(tenant.getName(), secondSignal, RequestPhase.FINAL);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      // expected
    }

    // verify constraint violation: new rollup conflicts with existing signal
    final String thirdSrc =
        "{"
            + "  'name': 'product_order_third',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'Product_Order',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamDesc thirdSignal = decodeConfig(thirdSrc);
    try {
      admin.addStream(tenant.getName(), thirdSignal, RequestPhase.INITIAL);
      admin.addStream(tenant.getName(), thirdSignal, RequestPhase.FINAL);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      // expected
    }

    // verify constraint violation: new signal conflicts with existing signal
    final String fourthSrc =
        "{"
            + "  'name': 'Product_Order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_phone_fourth',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamDesc fourthSignal = decodeConfig(fourthSrc);
    try {
      admin.addStream(tenant.getName(), fourthSignal, RequestPhase.INITIAL);
      admin.addStream(tenant.getName(), fourthSignal, RequestPhase.FINAL);
      fail("exception is expected");
    } catch (StreamAlreadyExistsException e) {
      // expected
    }

    // verify constraint violation: new signal conflicts with existing rollup
    final String fifthSrc =
        "{"
            + "  'name': 'Rollup_By_Phone',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_by_phone',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_by_phone',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_phone_fifth',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamDesc fifthSignal = decodeConfig(fifthSrc);
    try {
      admin.addStream(tenant.getName(), fifthSignal, RequestPhase.INITIAL);
      admin.addStream(tenant.getName(), fifthSignal, RequestPhase.FINAL);
      fail("exception is expected");
    } catch (StreamAlreadyExistsException e) {
      // expected
    }
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigViewNamesConflict() throws Exception {
    final TenantDesc tenant =
        new TenantDesc("testInitStreamConfigViewNamesConflict", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_conflicting_name',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }, {"
            + "    'name': 'view_conflicting_name',"
            + "    'groupBy': ['name'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_conflicting_name',"
            + "    'rollups': [{"
            + "      'name': 'rollup_conflicting_name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInitStreamConfigViewNamesConflictCaseInsensitive() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigViewNamesConflictCaseInsensitive",
            System.currentTimeMillis(),
            false);
    final String signalSrc =
        "{"
            + "  'name': 'product_order',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'phone', 'type': 'string'},"
            + "    {'name': 'name', 'type': 'string'},"
            + "    {'name': 'product', 'type': 'string'},"
            + "    {'name': 'price', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_conflicting_name',"
            + "    'groupBy': ['phone'],"
            + "    'attributes': ['price']"
            + "  }, {"
            + "    'name': 'View_Conflicting_Name',"
            + "    'groupBy': ['name'],"
            + "    'attributes': ['price']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'view_conflicting_name',"
            + "    'rollups': [{"
            + "      'name': 'rollup_conflicting_name',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  /** Indexing non-addable attribute is fine. */
  @Ignore("Bug TFOS-1578")
  @Test
  public void testInitStreamConfigIndexNonAddableAttribute() throws Exception {
    final TenantDesc tenant =
        new TenantDesc(
            "testInitStreamConfigIndexNonAddableAttribute", System.currentTimeMillis(), false);
    final String signalSrc =
        "{"
            + "  'name': 'invalidRollup',"
            + "  'type': 'signal',"
            + "  'attributes': ["
            + "    {'name': 'skid', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'view_skid_state',"
            + "    'groupBy': ['skid'],"
            + "    'attributes': ['state']"
            + "  }]"
            + "}";
    final StreamDesc signal = decodeConfig(signalSrc);
    admin.initStreamConfig(tenant, signal, Set.of());
  }

  @Test
  public void testAddMissingMetricsStream() throws TfosException, ApplicationException {
    long version = System.currentTimeMillis();

    AdminImpl adminImpl =
        partialMockBuilder(AdminImpl.class)
            .withConstructor(new MetricsStreamProvider())
            .addMockedMethods("addStream", "isTenantDescLatest")
            .addMockedMethod("addStream", String.class, StreamConfig.class, RequestPhase.class)
            .createMock();

    final TenantDesc tenant = new TenantDesc("opmAutoUpdate", version, false);

    expect(adminImpl.isTenantDescLatest(tenant)).andReturn(true).anyTimes();

    adminImpl.addStream(eq("opmAutoUpdate"), anyObject(StreamDesc.class), eq(RequestPhase.INITIAL));
    expectLastCall().andVoid().times(1);

    adminImpl.addStream(eq("opmAutoUpdate"), anyObject(StreamDesc.class), eq(RequestPhase.FINAL));
    expectLastCall().andVoid().times(1);

    replay(adminImpl);

    adminImpl.addMissingMetricsStream(tenant);
    verify(adminImpl);
  }

  @Test
  public void testUpdateMetricsStream() throws TfosException, ApplicationException {
    long version = System.currentTimeMillis();

    final StreamDesc stream =
        new StreamDesc(MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL, version);
    final TenantDesc tenant = new TenantDesc("opmAutoUpdate", version, false);
    tenant.addStream(stream);

    AdminImpl adminImpl =
        partialMockBuilder(AdminImpl.class)
            .withConstructor(new MetricsStreamProvider())
            .addMockedMethods("modifyStream", "isTenantDescLatest")
            .createMock();

    final StreamDesc newStream = adminImpl.generateOperationalMetricsStream(tenant);

    expect(adminImpl.isTenantDescLatest(tenant)).andReturn(true).anyTimes();

    adminImpl.modifyStream(
        "opmAutoUpdate",
        MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL,
        newStream,
        RequestPhase.INITIAL,
        AttributeModAllowance.FORCE,
        Set.of());
    expectLastCall().andVoid().times(1);

    adminImpl.modifyStream(
        "opmAutoUpdate",
        MetricsConstants.BIOS_TENANT_OP_METRICS_SIGNAL,
        newStream,
        RequestPhase.FINAL,
        AttributeModAllowance.FORCE,
        Set.of());
    expectLastCall().andVoid().times(1);

    replay(adminImpl);

    adminImpl.addMissingMetricsStream(tenant);
    verify(adminImpl);
  }

  @Test
  public void testContextJoinChaining()
      throws TfosException,
          ApplicationException,
          JsonParseException,
          JsonMappingException,
          IOException {

    final String tenantName = "context_join_chain_basic";
    TenantConfig tenantConfig = new TenantConfig(tenantName);
    tenantConfig.setVersion(System.currentTimeMillis());

    final String contextSrc =
        "{"
            + "  'name': 'temp_city_context',"
            + "  'type': 'context',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': [{"
            + "    'name': 'city_id',"
            + "    'type': 'int'"
            + "  }, {"
            + "    'name': 'city_name',"
            + "    'type': 'string'"
            + "  }, {"
            + "    'name': 'supplier_city_id',"
            + "    'type': 'int'"
            + "  }]"
            + "}";

    final StreamConfig contextConfig = decodeConfig(contextSrc);

    final String signalSrc =
        "{"
            + "  'name': 'temp_order_signal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {"
            + "      'name': 'order_id',"
            + "      'type': 'int'"
            + "    }, {"
            + "      'name': 'city_id',"
            + "      'type': 'int'"
            + "    }"
            + "  ],"
            + "  'preprocesses': ["
            + "    {"
            + "      'name': 'city_preprocess_1',"
            + "      'condition': 'city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'temp_city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'city_name'"
            + "        }, {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'temp_city_context',"
            + "          'attribute': 'supplier_city_id',"
            + "          'as': 'supplier_city_id'"
            + "        }"
            + "      ]"
            + "    },"
            + "    {"
            + "      'name': 'city_preprocess_2',"
            + "      'condition': 'supplier_city_id',"
            + "      'missingLookupPolicy': 'strict',"
            + "      'actions': ["
            + "        {"
            + "          'actionType': 'MERGE',"
            + "          'context': 'temp_city_context',"
            + "          'attribute': 'city_name',"
            + "          'as': 'supplier_city_name'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ],"
            + "  'views': [],"
            + "  'postprocesses': []"
            + "}";

    final StreamConfig signalConfig = decodeConfig(signalSrc);

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, tenantConfig.getVersion());
    admin.addTenant(tenantConfig, RequestPhase.FINAL, tenantConfig.getVersion());

    admin.addStream(tenantName, contextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, contextConfig, RequestPhase.FINAL);

    admin.addStream(tenantName, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, signalConfig, RequestPhase.FINAL);
  }

  private static String supportingContextFile(int contextNum) {
    return "src/test/resources/ContextToEnrichContext"
        + String.format("%d", contextNum)
        + "Tfos.json";
  }

  public void setupForContextEnrichment() throws Throwable {
    AdminTestUtils.addTenant(admin, tenantName2);

    // Set up contexts needed for context enrichment tests.
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(1));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(2));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(3));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(4));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(5));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(6));
    AdminTestUtils.populateStream(admin, tenantName2, supportingContextFile(7));
    enrichedContextConfig =
        AdminTestUtils.getStreamConfig("src/test/resources/EnrichedContextTfos.json");
  }

  @Test
  public void testContextEnrichmentBasic() throws Throwable {
    setupForContextEnrichment();
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNameAbsent() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).setName(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNameDuplicate() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(1).setName("resourceType");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentMlpAbsent() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.setMissingLookupPolicy(null);
    enrichedContextConfig.getContextEnrichments().get(0).setMissingLookupPolicy(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentBadMlp1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    enrichedContextConfig.getContextEnrichments().get(0).setMissingLookupPolicy(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentBadMlp2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(0)
        .setMissingLookupPolicy(MissingLookupPolicy.REJECT);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentBadMlp3() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(0)
        .setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoAttributes1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).setEnrichedAttributes(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoAttributes2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).setEnrichedAttributes(new ArrayList<>());
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoForeignKey1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).setForeignKey(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoForeignKey2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).setForeignKey(new ArrayList<>());
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoValueOrValuePickFirst1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(0)
        .setValuePickFirst(null);
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(1)
        .setValuePickFirst(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoValueOrValuePickFirst2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(0)
        .setValuePickFirst(new ArrayList<>());
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(1)
        .setValuePickFirst(new ArrayList<>());
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoValueOrValuePickFirst3() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(0)
        .setValuePickFirst(null);
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(1)
        .setValuePickFirst(null);
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(0)
        .setValue("");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentEmptyAs() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(0).getEnrichedAttributes().get(1).setAs("");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNonexistentAttribute() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("resourceType.nonexistentAttribute");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNonexistentContext() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("nonexistentContext.nonexistentAttribute");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNonexistentContext2() throws Throwable {
    setupForContextEnrichment();
    EnrichmentConfigContext enrichment =
        new EnrichmentConfigContext("nonexistentContextEnrichment");
    enrichment.setForeignKey(Arrays.asList("resourceTypeNum"));
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("nonexistentContext.nonexistentAttribute");
    enrichment.addEnrichedAttribute(enrichedAttribute);
    enrichedContextConfig.addEnrichment(enrichment);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValueFormat1() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("noDelimiter");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValueFormat2() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("two.Delimiters.here");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValueFormat3() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("bad-Delimiter");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValueFormat4() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue(".emptyPart1");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValueFormat5() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("emptyPart2.");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentMixDifferentContexts() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("staff.aliasName");
    enrichedContextConfig.getContextEnrichments().get(0).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoAsForValuePickFirst1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(1).getEnrichedAttributes().get(0).setAs(null);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentNoAsForValuePickFirst2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig.getContextEnrichments().get(1).getEnrichedAttributes().get(0).setAs("");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentInvalidValuePickFirstFormat() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValuePickFirst(Arrays.asList(".emptyPart1"));
    enrichedAttribute.setAs("test");
    enrichedContextConfig.getContextEnrichments().get(1).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentMixSimpleAndValuePickFirst() throws Throwable {
    setupForContextEnrichment();
    EnrichedAttribute enrichedAttribute = new EnrichedAttribute();
    enrichedAttribute.setValue("resourceType.resourceCategory");
    enrichedContextConfig.getContextEnrichments().get(1).addEnrichedAttribute(enrichedAttribute);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentMixDifferentDataTypes() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(1)
        .getEnrichedAttributes()
        .get(0)
        .getValuePickFirst()
        .add("staff.aliasName");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentDuplicateAttributeNames1() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(0)
        .getEnrichedAttributes()
        .get(1)
        .setAs("resourceId");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testContextEnrichmentDuplicateAttributeNames2() throws Throwable {
    setupForContextEnrichment();
    enrichedContextConfig
        .getContextEnrichments()
        .get(0)
        .getEnrichedAttributes()
        .get(1)
        .setAs("resourceTypeCode");
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);
  }

  @Test
  public void testContextEnrichmentAddSignal() throws Throwable {
    setupForContextEnrichment();
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);

    var signalConfig =
        AdminTestUtils.getStreamConfig("src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName2, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, signalConfig, RequestPhase.FINAL);
  }

  @Test
  public void testProxiesAddStream() throws Throwable {
    setupForContextEnrichment();

    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);

    var signalConfig =
        AdminTestUtils.getStreamConfig("src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName2, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, signalConfig, RequestPhase.FINAL);

    var tenant = admin.getTenant(tenantName2);
    var s1 = admin.getStream(tenantName2, "_usage");
    var s2 = admin.getStream(tenantName2, "resourceType");
    var s9 = admin.getStream(tenantName2, "enrichedContext");
    var s10 = admin.getStream(tenantName2, "patientVisits");
    assertFalse(tenant.isProxyInformationDirty());
    assertEquals(10, tenant.getMaxAllocatedStreamNameProxy().intValue());
    assertEquals(1, s1.getStreamNameProxy().shortValue());
    assertEquals(2, s2.getStreamNameProxy().shortValue());
    assertEquals(9, s9.getStreamNameProxy().shortValue());
    assertEquals(10, s10.getStreamNameProxy().shortValue());
    assertEquals(3, s2.getMaxAttributeProxy().shortValue());
    assertEquals(7, s9.getMaxAttributeProxy().shortValue());
    assertEquals(9, s10.getMaxAttributeProxy().shortValue());
    assertEquals(1, s2.getAttributeProxyInfo().get("resourcetypenum").getProxy());
    assertEquals(2, s2.getAttributeProxyInfo().get("resourcetypecode").getProxy());
    assertEquals(3, s2.getAttributeProxyInfo().get("resourcecategory").getProxy());
    assertEquals(1, s9.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(4, s9.getAttributeProxyInfo().get("resourcetypecode").getProxy());
    assertEquals(5, s9.getAttributeProxyInfo().get("myresourcecategory").getProxy());
    assertEquals(6, s9.getAttributeProxyInfo().get("cost").getProxy());
    assertEquals(7, s9.getAttributeProxyInfo().get("resourcevalue").getProxy());
    assertEquals(9, s10.getAttributeProxyInfo().get("resourcevalue").getProxy());
  }

  @Test
  public void testProxiesModifyAndRemoveStream() throws Throwable {
    setupForContextEnrichment();

    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, enrichedContextConfig, RequestPhase.FINAL);

    var signalConfig =
        AdminTestUtils.getStreamConfig("src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName2, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, signalConfig, RequestPhase.FINAL);
    var tenant = admin.getTenant(tenantName2);
    var s = admin.getStream(tenantName2, "patientVisits");
    assertFalse(tenant.isProxyInformationDirty());
    assertEquals(10, s.getStreamNameProxy().shortValue());
    assertEquals(10, tenant.getMaxAllocatedStreamNameProxy().intValue());
    assertEquals(9, s.getMaxAttributeProxy().shortValue());
    verifyUnchangedAttributeProxies(s);
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());

    // Add newAttribute1 at end.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 10, "src/test/resources/SignalForContextEnrichmentTfos-modified1.json");
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());

    // Reorder existing attributes patientId and resourceId, enriched attributes zipcode and
    // resourceTypeCode, delete attribute visitId and enriched attribute cost.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 10, "src/test/resources/SignalForContextEnrichmentTfos-modified2.json");
    assertNull(s.getAttributeProxyInfo().get("cost"));
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());

    // Add visitId and cost again.
    s =
        modifySignalAndVerifyExistingProxies(
            (short) 12, "src/test/resources/SignalForContextEnrichmentTfos-modified3.json");
    assertEquals(10, s.getAttributeProxyInfo().get("newattribute1").getProxy());
    assertEquals(11, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(12, s.getAttributeProxyInfo().get("cost").getProxy());

    // Delete the stream and re-create it.
    long timestamp2 = System.currentTimeMillis();
    admin.removeStream(tenantName2, "patientVisits", RequestPhase.INITIAL, timestamp2);
    admin.removeStream(tenantName2, "patientVisits", RequestPhase.FINAL, timestamp2);
    signalConfig =
        AdminTestUtils.getStreamConfig("src/test/resources/SignalForContextEnrichmentTfos.json");
    admin.addStream(tenantName2, signalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName2, signalConfig, RequestPhase.FINAL);
    tenant = admin.getTenant(tenantName2);
    s = admin.getStream(tenantName2, "patientVisits");
    assertFalse(tenant.isProxyInformationDirty());
    assertEquals(11, s.getStreamNameProxy().shortValue());
    assertEquals(11, tenant.getMaxAllocatedStreamNameProxy().intValue());
    assertEquals(9, s.getMaxAttributeProxy().shortValue());
    verifyUnchangedAttributeProxies(s);
    assertEquals(1, s.getAttributeProxyInfo().get("visitid").getProxy());
    assertEquals(8, s.getAttributeProxyInfo().get("cost").getProxy());
  }

  private StreamDesc modifySignalAndVerifyExistingProxies(short maxAttributeProxy, String jsonFile)
      throws Exception {
    sleep(1); // To ensure the new timestamp is higher than previous timestamp.
    var signalConfig = AdminTestUtils.getStreamConfig(jsonFile);
    admin.modifyStream(
        tenantName2,
        "patientVisits",
        signalConfig,
        RequestPhase.INITIAL,
        AttributeModAllowance.FORCE,
        Set.of());
    admin.modifyStream(
        tenantName2,
        "patientVisits",
        signalConfig,
        RequestPhase.FINAL,
        AttributeModAllowance.FORCE,
        Set.of());
    var tenant = admin.getTenant(tenantName2);
    var s = admin.getStream(tenantName2, "patientVisits");
    assertFalse(tenant.isProxyInformationDirty());
    assertEquals(10, tenant.getMaxAllocatedStreamNameProxy().intValue());
    assertEquals(10, s.getStreamNameProxy().shortValue());
    assertEquals(maxAttributeProxy, s.getMaxAttributeProxy().shortValue());
    verifyUnchangedAttributeProxies(s);
    // System.out.println(TfosObjectMapperProvider.get().writeValueAsString(tenant));
    // System.out.println(TfosObjectMapperProvider.get().writeValueAsString(s));
    return s;
  }

  private void verifyUnchangedAttributeProxies(StreamDesc s) throws JsonProcessingException {
    assertEquals(2, s.getAttributeProxyInfo().get("patientid").getProxy());
    assertEquals(3, s.getAttributeProxyInfo().get("resourceid").getProxy());
    assertEquals(4, s.getAttributeProxyInfo().get("resourcetypenum").getProxy());
    assertEquals(5, s.getAttributeProxyInfo().get("zipcode").getProxy());
    assertEquals(6, s.getAttributeProxyInfo().get("resourcetypecode").getProxy());
    assertEquals(7, s.getAttributeProxyInfo().get("myresourcecategory").getProxy());
    assertEquals(9, s.getAttributeProxyInfo().get("resourcevalue").getProxy());
  }

  private StreamDesc decodeConfig(String src) throws JsonProcessingException {
    return new StreamDesc(objectMapper.readValue(src.replaceAll("'", "\""), StreamConfig.class))
        .setVersion(System.currentTimeMillis());
  }
}
