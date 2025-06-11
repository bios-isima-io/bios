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

import static io.isima.bios.models.AttributeModAllowance.CONVERTIBLES_ONLY;
import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.StreamConversion;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import java.util.Set;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StreamModValidatorTest {

  private AdminImpl admin;
  private String tenantName;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {
    admin = new AdminImpl(null, new MetricsStreamProvider());
    TenantDesc tempTenantDesc =
        new TenantDesc("testTenant", System.currentTimeMillis(), Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());
    tenantName = tempTenantDesc.getName();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testBasic() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'int'}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());
    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, CONVERTIBLES_ONLY);
    final StreamConversion conv = oldDesc.getStreamConversion();
    assertNotNull(conv);

    assertEquals("country", conv.getAttributeConversion("country").getNewName());
    assertEquals(
        StreamConversion.ConversionType.NO_CHANGE,
        conv.getAttributeConversion("country").getConversionType());
    assertEquals("country", conv.getAttributeConversion("country").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.STRING,
        conv.getAttributeConversion("country").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("country").getDefaultValue());

    assertEquals("state", conv.getAttributeConversion("state").getNewName());
    assertEquals(
        StreamConversion.ConversionType.NO_CHANGE,
        conv.getAttributeConversion("state").getConversionType());
    assertEquals("state", conv.getAttributeConversion("state").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.STRING,
        conv.getAttributeConversion("state").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("state").getDefaultValue());

    assertEquals("city", conv.getAttributeConversion("city").getNewName());
    assertEquals(
        StreamConversion.ConversionType.ADD,
        conv.getAttributeConversion("city").getConversionType());
    assertEquals("city", conv.getAttributeConversion("city").getNewName());
    assertEquals("n/a", conv.getAttributeConversion("city").getDefaultValue());

    assertEquals("value", conv.getAttributeConversion("value").getNewName());
    assertEquals(
        StreamConversion.ConversionType.CONVERT,
        conv.getAttributeConversion("value").getConversionType());
    assertEquals("value", conv.getAttributeConversion("value").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.INT,
        conv.getAttributeConversion("value").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("value").getDefaultValue());
  }

  @Test
  public void testMissingDefault() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'int'}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'city', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    thrown.expect(ConstraintViolationException.class);
    thrown.expectMessage(
        Matchers.equalTo(
            "Constraint violation: Default value must be configured for an adding attribute;"
                + " tenant=testTenant, signal=testStream, attribute=city"));

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, CONVERTIBLES_ONLY);
  }

  @Test
  public void testNonConvertible() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'value', 'type': 'int'}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    final var thrown =
        assertThrows(
            ConstraintViolationException.class,
            () -> admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, CONVERTIBLES_ONLY));
    assertThat(
        thrown.getErrorMessage(),
        containsString(
            "Constraint violation: Unsupported attribute type transition; "
                + "tenant=testTenant, signal=testStream, attribute=value, fromType=LONG, toType=INT"));
  }

  @Test
  public void testNonConvertibleWithoutDefault() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'int'}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    thrown.expect(ConstraintViolationException.class);
    thrown.expectMessage(
        Matchers.equalTo(
            "Constraint violation:"
                + " Default value must be configured for non-convertible attribute modification;"
                + " tenant=testTenant, signal=testStream, attribute=value"));

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, FORCE);
  }

  @Test
  public void testDeleteContextKey() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'type': 'context',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'type': 'context',"
            + "'attributes': ["
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'value', 'type': 'long'}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    thrown.expect(ConstraintViolationException.class);
    thrown.expectMessage(
        Matchers.equalTo(
            "Constraint violation: Primary key attributes may not be modified;"
                + " tenant=testTenant, context=testStream"));

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, FORCE);
  }

  @Test
  public void testAddEnumentry() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'EUROPE', 'MIDDLE_EAST', 'AFRICA', 'AMERICA']}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'EUROPE', 'MIDDLE_EAST', 'AFRICA', 'AMERICA', 'ARCTIC']}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, FORCE);

    final StreamConversion conv = oldDesc.getStreamConversion();
    assertNotNull(conv);

    assertEquals("country", conv.getAttributeConversion("country").getNewName());
    assertEquals(
        StreamConversion.ConversionType.NO_CHANGE,
        conv.getAttributeConversion("country").getConversionType());
    assertEquals("country", conv.getAttributeConversion("country").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.STRING,
        conv.getAttributeConversion("country").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("country").getDefaultValue());

    assertEquals("state", conv.getAttributeConversion("state").getNewName());
    assertEquals(
        StreamConversion.ConversionType.NO_CHANGE,
        conv.getAttributeConversion("state").getConversionType());
    assertEquals("state", conv.getAttributeConversion("state").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.STRING,
        conv.getAttributeConversion("state").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("state").getDefaultValue());

    assertEquals("region", conv.getAttributeConversion("region").getNewName());
    assertEquals(
        StreamConversion.ConversionType.NO_CHANGE,
        conv.getAttributeConversion("region").getConversionType());
    assertEquals("region", conv.getAttributeConversion("region").getOldDesc().getName());
    assertEquals(
        InternalAttributeType.ENUM,
        conv.getAttributeConversion("region").getOldDesc().getAttributeType());
    assertNull(conv.getAttributeConversion("region").getDefaultValue());
  }

  @Test
  public void testDeleteEnumentry() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'EUROPE', 'MIDDLE_EAST', 'AFRICA', 'AMERICA']}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'EUROPE', 'MIDDLE_EAST', 'AFRICA']}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    thrown.expect(ConstraintViolationException.class);
    thrown.expectMessage(
        Matchers.equalTo(
            "Constraint violation: Enum entry may not be modified;"
                + " tenant=testTenant, signal=testStream, attribute=region,"
                + " index=5, oldEntry=AMERICA, newEntry=null"));

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, FORCE);
  }

  @Test
  public void testModifyEnumentry() throws Exception {
    long timestamp = System.currentTimeMillis();
    final String origSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'AMERICA', 'EUROPE', 'MIDDLE_EAST', 'AFRICA']}"
            + "]}";
    AdminTestUtils.populateStream(admin, tenantName, origSrc, timestamp);
    final TenantDesc tenantDesc = admin.getTenant("testTenant");
    final StreamDesc oldDesc = tenantDesc.getStream("testStream");

    final String newSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string'},"
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['ASIA', 'PACIFIC', 'AMERICAS', 'EUROPE', 'MIDDLE_EAST', 'AFRICA']}"
            + "]}";
    final StreamConfig newConf = AdminTestUtils.makeStreamConfig(newSrc, ++timestamp);
    final StreamDesc newDesc = new StreamDesc(newConf);
    admin.initStreamConfig(tenantDesc, newDesc, Set.of());

    thrown.expect(ConstraintViolationException.class);
    thrown.expectMessage(
        Matchers.equalTo(
            "Constraint violation: Enum entry may not be modified;"
                + " tenant=testTenant, signal=testStream, attribute=region,"
                + " index=2, oldEntry=AMERICA, newEntry=AMERICAS"));

    admin.validateStreamMod(tenantDesc, oldDesc, newDesc, true, FORCE);
  }
}
