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
package io.isima.bios.data.impl;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.FilterSyntaxException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Base64;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterTest {
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();
  private static final String FILTER_NOT_COVERED_BY_INDEX =
      "stringAttr = 'abc' AND numberAttr = 6507226550";

  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;

  private static TenantDesc tenantDesc;
  private static StreamDesc streamDesc;
  private static CassStream cassStream;
  private static CassStream cassIndex;

  private static long timestamp = System.currentTimeMillis();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(FilterTest.class);
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    String tenantSrc =
        "{"
            + "'name': 'filter_validator_test',"
            + "'streams': [{"
            + "  'name': 'test_stream',"
            + "  'type': 'signal',"
            + "  'attributes': ["
            + "    {'name': 'stringAttr', 'type': 'string'},"
            + "    {'name': 'booleanAttr', 'type': 'boolean'},"
            + "    {'name': 'intAttr', 'type': 'int'},"
            + "    {'name': 'numberAttr', 'type': 'number'},"
            + "    {'name': 'doubleAttr', 'type': 'double'},"
            + "    {'name': 'inetAttr', 'type': 'inet'},"
            + "    {'name': 'dateAttr', 'type': 'date'},"
            + "    {'name': 'uuidAttr', 'type': 'uuid'},"
            + "    {'name': 'enumAttr', 'type': 'enum',"
            + "     'enum': ['one', 'two', 'three', 'four', 'five', 'twenty one']},"
            + "    {'name': 'blobAttr', 'type': 'blob'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'by_string_int_enum',"
            + "    'groupBy': ['stringAttr', 'intAttr', 'enumAttr'],"
            + "    'attributes': ['doubleAttr']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'by_string_int_enum',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_string_int_enum',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}]}";
    tenantDesc =
        new TenantDesc(mapper.readValue(tenantSrc.replaceAll("'", "\""), TenantConfig.class));

    final TenantConfig tenantConfig = tenantDesc.toTenantConfig();
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    streamDesc = admin.getStream(tenantDesc.getName(), "test_stream");
    String indexName =
        CassStream.generateQualifiedNameSubstream(
            StreamType.INDEX, "test_stream", "by_string_int_enum");
    StreamDesc indexDesc = admin.getStream(tenantDesc.getName(), indexName);
    cassStream = dataEngine.getCassStream(streamDesc);
    cassIndex = dataEngine.getCassStream(indexDesc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.removeTenant(tenantDesc.toTenantConfig(), RequestPhase.INITIAL, ++timestamp);
    admin.removeTenant(tenantDesc.toTenantConfig(), RequestPhase.FINAL, timestamp);
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  private String interpretFilter(String src) throws TfosException, ApplicationException {
    return cassStream.interpretFilter(
        cassStream.parseFilter(src).stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  private String interpretIndexFilter(String src) throws TfosException, ApplicationException {
    return cassIndex.interpretFilter(
        cassIndex.parseFilter(src).stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  @Test
  public void testInterpretingFilterStringEqString() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringAttr = '322768'");
    assertEquals("evt_stringattr = '322768'", interpreted);
  }

  @Test
  public void testInterpretingFilterStringEqNumber() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringAttr = 322768");
    assertEquals("evt_stringattr = 322768", interpreted);
  }

  @Test
  public void testInterpretingFilterStringEqNameConflict()
      throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringAttr = 'stringAttr'");
    assertEquals("evt_stringattr = 'stringAttr'", interpreted);
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterStringMissingQuotes()
      throws TfosException, ApplicationException {
    interpretFilter("stringAttr = stringAttr");
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterStringDoubleQuotes()
      throws TfosException, ApplicationException {
    interpretFilter("stringAttr = \"stringAttr\"");
  }

  @Test
  public void testInterpretingFilterInt() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("intAttr = 6507226550");
    assertEquals("evt_intattr = 6507226550", interpreted);
  }

  @Test
  public void testInterpretingFilterNumber() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("numberAttr = 98765432109876543210987");
    assertEquals("evt_numberattr = 98765432109876543210987", interpreted);
  }

  @Test
  public void testInterpretingFilterDouble() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("doubleAttr = 3.14159");
    assertEquals("evt_doubleattr = 3.14159", interpreted);
  }

  @Test
  public void testInterpretingFilterInet() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("inetAttr = '192.168.0.1'");
    assertEquals("evt_inetattr = '192.168.0.1'", interpreted);
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterInetWithoutQuotes() throws TfosException, ApplicationException {
    interpretFilter("inetAttr = 192.168.0.1");
  }

  @Test
  public void testInterpretingFilterDate() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("dateAttr = '2001-02-10'");
    assertEquals("evt_dateattr = '2001-02-10'", interpreted);
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterDateWithoutQuotes() throws TfosException, ApplicationException {
    interpretFilter("dateAttr = 2001-02-10");
  }

  @Test
  public void testInterpretingFilterUuid() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("uuidAttr = a38f1aa6-c0fc-11e8-a355-529269fb1459");
    assertEquals("evt_uuidattr = a38f1aa6-c0fc-11e8-a355-529269fb1459", interpreted);
  }

  @Test
  public void testInterpretingFilterEnum() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("enumAttr = 'four'");
    assertEquals("evt_enumattr = 3", interpreted);
  }

  @Test
  public void testInterpretingFilterComparison() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("intAttr > 40792");
    assertEquals("evt_intattr > 40792", interpreted);
  }

  @Test
  public void testInterpretingFilterEnum2() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("enumAttr = 'twenty one'");
    assertEquals("evt_enumattr = 5", interpreted);
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterStringEqMissingRhs()
      throws TfosException, ApplicationException {
    interpretFilter("stringAttr = ");
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInvalidOperator() throws TfosException, ApplicationException {
    interpretFilter("numberAttr * 'abc'");
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInvalidSecondOperator() throws TfosException, ApplicationException {
    interpretFilter("stringAttr = 'abc' AND numberAttr * 'abc'");
  }

  @Test
  public void testInterpretingFilterInOperatorString() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringAttr IN ('Abc', 'Def')");
    assertEquals("evt_stringattr IN ('Abc', 'Def')", interpreted);
  }

  @Test
  public void testInterpretingInOperatorInt() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("intAttr IN(6507226550, 6503155062)");
    assertEquals("evt_intattr IN (6507226550, 6503155062)", interpreted);
  }

  @Test
  public void testBooleanFalse() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringattr = 'xyz' AND booleanAttr = false");
    assertEquals("evt_stringattr = 'xyz' AND evt_booleanattr = false", interpreted);
  }

  @Test
  public void testBooleanTrue() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("stringAttr = 'xyz' AND booleanAttr = TRUE");
    assertEquals("evt_stringattr = 'xyz' AND evt_booleanattr = TRUE", interpreted);
  }

  @Test(expected = FilterSyntaxException.class)
  public void testCorruptTerm() throws TfosException, ApplicationException {
    interpretFilter(" '' AND booleanAttr = False AND stringAttr = 'djdhfkd'");
  }

  @Test(expected = InvalidFilterException.class)
  public void testInterpretingFilterInvalidEnum() throws TfosException, ApplicationException {
    interpretFilter("enumAttr = 'billion'");
  }

  @Test(expected = InvalidFilterException.class)
  public void testInterpretingFilterInvalidEnum2() throws TfosException, ApplicationException {
    interpretFilter("enumAttr = ''");
  }

  @Test(expected = InvalidFilterException.class)
  public void testInterpretingFilterInvalidEnum3() throws TfosException, ApplicationException {
    interpretFilter("enumAttr = 123");
  }

  @Test(expected = FilterSyntaxException.class)
  public void testInterpretingFilterInvalidEnum4() throws TfosException, ApplicationException {
    interpretFilter("enumAttr =");
  }

  private static String base64(String src) {
    return Base64.getEncoder().encodeToString(src.getBytes());
  }

  @Test(expected = InvalidFilterException.class)
  public void testInterpretingFilterBlob() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter("blobAttr = '" + base64("12345") + "'");
    assertEquals("evt_blobattr = 0x3132333435", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterSimple() throws TfosException, ApplicationException {
    String interpreted = interpretIndexFilter("stringAttr = 'abc'");
    assertEquals("evt_stringattr = 'abc'", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterTwoDimensionalEq()
      throws TfosException, ApplicationException {
    String interpreted = interpretIndexFilter("stringAttr = 'abc' AND intAttr = 12345");
    assertEquals("evt_stringattr = 'abc' AND evt_intattr = 12345", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterTwoDimensionalNonEq()
      throws TfosException, ApplicationException {
    String interpreted = interpretIndexFilter("stringAttr = 'abc' AND intAttr > 12345");
    assertEquals("evt_stringattr = 'abc' AND evt_intattr > 12345", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterThreeDimensionalEq()
      throws TfosException, ApplicationException {
    String interpreted =
        interpretIndexFilter("stringAttr = 'abc' AND intAttr = 12345 AND enumAttr = 'five'");
    assertEquals(
        "evt_stringattr = 'abc' AND evt_intattr = 12345 AND evt_enumattr = 4", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterThreeDimensionalIn()
      throws TfosException, ApplicationException {
    String interpreted =
        interpretIndexFilter(
            "stringAttr IN ('abc', 'def') AND intAttr IN (12, 345)"
                + " AND enumAttr IN ('five', 'twenty one')");
    assertEquals(
        "evt_stringattr IN ('abc', 'def') AND evt_intattr IN (12, 345)"
            + " AND evt_enumattr IN (4, 5)",
        interpreted);
  }

  @Test
  public void testInterpretingIndexFilterThreeDimensionalNonEq()
      throws TfosException, ApplicationException {
    String interpreted = interpretIndexFilter("stringAttr = 'abc' AND intAttr = 12345");
    assertEquals("evt_stringattr = 'abc' AND evt_intattr = 12345", interpreted);
  }

  @Test
  public void testInterpretingIndexFilterThreeDimensionalNonEqChangeOrder()
      throws TfosException, ApplicationException {
    String interpreted = interpretIndexFilter("intAttr = 12345 AND stringAttr = 'abc'");
    assertEquals("evt_intattr = 12345 AND evt_stringattr = 'abc'", interpreted);
  }

  @Test
  public void testValidateIndexFilterNoInterpreting() throws TfosException, ApplicationException {
    cassIndex.validateFilter(
        cassIndex.parseFilter("intAttr in (12345, 2345) AND stringAttr = 'abc'").stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testInterpretingIndexFilterTwoDimensionalPrecedingNonEq()
      throws TfosException, ApplicationException {
    interpretIndexFilter("stringAttr < 'M' AND intAttr = 12345");
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testInterpretingIndexFilterTwoDimensionalPrecedingNonEqReverseOrder()
      throws TfosException, ApplicationException {
    interpretIndexFilter("intAttr = 12345 AND stringAttr < 'M'");
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testInterpretingIndexFilterMissingPreceding()
      throws TfosException, ApplicationException {
    interpretIndexFilter("intAttr = 12345");
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testValidateIndexFilterNegative() throws TfosException, ApplicationException {
    cassIndex.validateFilter(
        cassIndex.parseFilter("intAttr = 12345 AND stringAttr > 'abc'").stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  @Test(expected = InvalidFilterException.class)
  public void testValidatingIndexFilterInvalidEnumWithIn()
      throws TfosException, ApplicationException {
    cassIndex.validateFilter(
        cassIndex
            .parseFilter(
                "stringAttr IN ('abc', 'def') AND intAttr IN (12, 345)"
                    + " AND enumAttr IN ('five', 'zero')")
            .stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testInterpretingIndexFilterNotCovered() throws TfosException, ApplicationException {
    interpretIndexFilter(FILTER_NOT_COVERED_BY_INDEX);
  }

  @Test(expected = FilterNotApplicableException.class)
  public void testValidatingIndexFilterNotCovered() throws TfosException, ApplicationException {
    cassIndex.validateFilter(
        cassIndex.parseFilter(FILTER_NOT_COVERED_BY_INDEX).stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }

  public void testInterpretingFilterNotCoveredByIndex() throws TfosException, ApplicationException {
    final String interpreted = interpretFilter(FILTER_NOT_COVERED_BY_INDEX);
    assertEquals("evt_stringattr = 'abc' AND evt_numberattr = 6507226550", interpreted);
  }

  public void testValidatingFilterNotCoveredByIndex() throws TfosException, ApplicationException {
    cassStream.validateFilter(
        cassStream.parseFilter(FILTER_NOT_COVERED_BY_INDEX).stream()
            .map((relation) -> relation)
            .collect(Collectors.toList()));
  }
}
