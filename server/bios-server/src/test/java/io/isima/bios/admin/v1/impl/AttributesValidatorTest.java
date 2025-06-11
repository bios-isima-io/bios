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
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AttributesValidatorTest {

  private static ObjectMapper objectMapper;
  private static String tenantName;

  private AdminImpl admin;

  private StreamConfig streamConfig;

  @BeforeClass
  public static void setUpClass() {
    objectMapper = TfosObjectMapperProvider.get();
    tenantName = "attributes_validator_test";
  }

  @Before
  public void setUp() throws Exception {
    admin = new AdminImpl(null, new MetricsStreamProvider());
    final TenantConfig tenant = new TenantConfig(tenantName);
    Long timestamp = System.currentTimeMillis();
    admin.addTenant(tenant, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant, RequestPhase.FINAL, timestamp);
    final String templateSrc =
        "{"
            + "  'name': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'string_attr', 'type': 'string'},"
            + "    {'name': 'enum_attr', 'type': 'enum',"
            + "      'enum': ['ONE', 'TWO', 'THREE']}"
            + "  ]"
            + "}";
    streamConfig = objectMapper.readValue(templateSrc.replace("'", "\""), StreamConfig.class);
    streamConfig.setVersion(timestamp);
  }

  @Test
  public void testNoError() throws TfosException, ApplicationException {
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, streamConfig, RequestPhase.FINAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testMissingValuePolicyMissing() throws Exception {
    streamConfig.setMissingValuePolicy(null);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test
  public void testMvpOverriding() throws Exception {
    final AttributeDesc enumAttr = streamConfig.getAttributes().get(1);
    enumAttr.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    enumAttr.setDefaultValue("TWO");
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, streamConfig, RequestPhase.FINAL);
    StreamDesc registered = admin.getStream(tenantName, streamConfig.getName());
    assertEquals("TWO", registered.getAttributes().get(1).getDefaultValue());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testInvalidEnumDefault() throws Exception {
    final AttributeDesc enumAttr = streamConfig.getAttributes().get(1);
    enumAttr.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    enumAttr.setDefaultValue("FOUR");
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test
  public void testMissingDefault() throws Exception {
    streamConfig.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    try {
      admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
      fail("exception is expected");
    } catch (ConstraintViolationException e) {
      assertEquals(
          "Constraint violation:"
              + " Property 'defaultValue' must be set when missing value policy is 'use_default';"
              + " tenant=attributes_validator_test, signal=signal, attribute=string_attr",
          e.getMessage());
    }
  }

  @Test(expected = ConstraintViolationException.class)
  public void testEnumDefinitionMissing() throws Exception {
    streamConfig.getAttributes().get(1).setEnum(null);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testEnumDefinitionEmpty() throws Exception {
    streamConfig.getAttributes().get(1).setEnum(Collections.emptyList());
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = InvalidEnumException.class)
  public void testEmptyEnumValue() throws Exception {
    streamConfig.getAttributes().get(1).getEnum().add("");
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = InvalidEnumException.class)
  public void testNullEnumValue() throws Exception {
    streamConfig.getAttributes().get(1).getEnum().add(null);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = InvalidEnumException.class)
  public void testDuplicateEnumValues() throws Exception {
    streamConfig.getAttributes().get(1).getEnum().add("ONE");
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  /** Enum values match is case sensitive. */
  @Test
  public void testEnumEntryCaseSensitivity() throws Exception {
    streamConfig.getAttributes().get(1).getEnum().add("one");
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, streamConfig, RequestPhase.FINAL);
    StreamDesc registered = admin.getStream(tenantName, streamConfig.getName());
    assertEquals(
        Arrays.asList("ONE", "TWO", "THREE", "one"), registered.getAttributes().get(1).getEnum());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testMissingAttributeName() throws Exception {
    final AttributeDesc desc = new AttributeDesc();
    desc.setAttributeType(InternalAttributeType.INT);
    streamConfig.addAttribute(desc);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testEmptyAttributeName() throws Exception {
    final AttributeDesc desc = new AttributeDesc("", InternalAttributeType.INT);
    streamConfig.addAttribute(desc);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }

  @Test(expected = ConstraintViolationException.class)
  public void testMissingAttributeType() throws Exception {
    final AttributeDesc desc = new AttributeDesc("int_value", null);
    streamConfig.addAttribute(desc);
    admin.addStream(tenantName, streamConfig, RequestPhase.INITIAL);
  }
}
