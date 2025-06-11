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
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.View;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SummarizeRequestValidatorTest {

  private static ObjectMapper mapper = TfosObjectMapperProvider.get();

  private static AdminInternal admin;
  private static DataEngineImpl dataEngine;

  private static TenantConfig tenantConfig;

  private static StreamDesc streamDesc;

  private static CassStream cassStream;

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static Aggregate sum(String by, String as) {
    return new Aggregate(MetricFunction.SUM, by, as);
  }

  private static Aggregate min(String by, String as) {
    return new Aggregate(MetricFunction.MIN, by, as);
  }

  private static View sort(String by, boolean reverse) {
    final View sort = new View();
    sort.setFunction(ViewFunction.SORT);
    sort.setBy(by);
    sort.setReverse(reverse);
    return sort;
  }

  @Parameter(0)
  public String requestSrc;

  @Parameter(1)
  public Class<? extends Throwable> expectedException;

  @Parameter(2)
  public String message;

  @Parameter(3)
  public Long startTime;

  @Parameter(4)
  public Long endTime;

  @Parameter(5)
  public Long interval;

  @Parameter(6)
  public TimeZone timezone;

  @Parameter(7)
  public Long horizon;

  @Parameter(8)
  public List<Aggregate> aggregates;

  @Parameter(9)
  public List<String> group;

  @Parameter(10)
  public View sort;

  @Parameter(11)
  public Integer limit;

  @Parameter(12)
  public String filter;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(SummarizeRequestValidatorTest.class);

    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();

    long timestamp = System.currentTimeMillis();
    final StreamConfig signal = new StreamConfig("testSignal").setVersion(timestamp);
    signal.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("sales", InternalAttributeType.INT));
    signal.addAttribute(new AttributeDesc("sequence", InternalAttributeType.INT));
    tenantConfig = new TenantConfig("testTenant");
    tenantConfig.addStream(signal);

    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // it's ok
    }

    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);

    streamDesc = admin.getStream("testTenant", "testSignal");
    cassStream = dataEngine.getCassStream(streamDesc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    long timestamp = System.currentTimeMillis();
    admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
    admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Parameters
  public static Collection<Object[]> data() {
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    final TimeZone ist = TimeZone.getTimeZone("IST");
    final TimeZone pst = TimeZone.getTimeZone("PST");
    final Aggregate count = new Aggregate(MetricFunction.COUNT, null, null);
    final List<String> emptyGroup = Arrays.asList();
    final String oneDay = "86400000";

    return Arrays.asList(
        new Object[][] {
          // [0] least
          {
            "{'startTime': 100, 'endTime': 1000, 'interval': 10,"
                + " 'aggregates': [{'function': 'count'}]}",
            null,
            null,
            100L,
            1000L,
            10L,
            utc,
            10L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [1] set horizon explicitly
          {
            "{'startTime': 100, 'endTime': 1000, 'interval': 10, 'horizon': 30,"
                + " 'aggregates': [{'function': 'sum', 'by': 'sequence'}]}",
            null,
            null,
            100L,
            1000L,
            10L,
            utc,
            30L,
            Arrays.asList(sum("sequence", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [2] invalid time range
          {
            "{'startTime': 1000, 'endTime': 100, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'sum', 'by': 'sequence'}]}",
            ConstraintViolationException.class,
            "Constraint violation: endTime may not be earlier than startTime",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [3] time range does not cover summary checkpoint, UTC
          {
            "{'startTime': 1000, 'endTime': 10000, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}]}",
            ConstraintViolationException.class,
            "Constraint violation: time range should cover at least one summary checkpoint",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [4] time range covers summary checkpoint, UTC
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [5] time range does not cover summary checkpoint, IST
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}], 'timezone': 'UTC+05:30'}",
            ConstraintViolationException.class,
            "Constraint violation: time range should cover at least one summary checkpoint",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [6] time range covers summary checkpoint, IST
          {
            "{'startTime': 1548441000000, 'endTime': 1548441000001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}], 'timezone': 'UTC+05:30'}",
            null,
            null,
            1548441000000L,
            1548527400000L,
            86400000L,
            ist,
            86400000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [7] time range does not cover summary checkpoint, PST
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}], 'timezone': 'UTC-08:00'}",
            ConstraintViolationException.class,
            "Constraint violation: time range should cover at least one summary checkpoint",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [8] time range covers summary checkpoint, PST
          {
            "{'startTime': 1548489600000, 'endTime': 1548489600001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}], 'timezone': 'UTC-08:00'}",
            null,
            null,
            1548489600000L,
            1548576000000L,
            86400000L,
            pst,
            86400000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [9] negative start time
          {
            "{'startTime': -1, 'endTime': 10000, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}]}",
            InvalidValueException.class,
            "Invalid value: startTime may not be negative",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [10] negative interval
          {
            "{'startTime': 1000, 'endTime': 10000, 'interval': -1,"
                + " 'aggregates': [{'function': 'count'}]}",
            InvalidValueException.class,
            "Invalid value: interval value should be positive",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [11] zero interval
          {
            "{'startTime': 1000, 'endTime': 10000, 'interval': 0,"
                + " 'aggregates': [{'function': 'count'}]}",
            InvalidValueException.class,
            "Invalid value: interval value should be positive",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [12] negative horizon
          {
            "{'startTime': 1000, 'endTime': 10000, 'interval': 10, 'horizon': -10,"
                + " 'aggregates': [{'function': 'count'}]}",
            InvalidValueException.class,
            "Invalid value: horizon value should be positive",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [12] negative horizon
          {
            "{'startTime': 1000, 'endTime': 10000, 'interval': 10, 'horizon': 0,"
                + " 'aggregates': [{'function': 'count'}]}",
            InvalidValueException.class,
            "Invalid value: horizon value should be positive",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [14] horizon equal to interval
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'horizon': 86400000, 'aggregates': [{'function': 'count'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [15] horizon is interval * 3
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'horizon': 259200000, 'aggregates': [{'function': 'count'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            259200000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [16] horizon less than interval
          {
            "{'startTime': 1548000000000, 'endTime': 1549000000001, 'interval': 86400000,"
                + " 'horizon': 86399999, 'aggregates': [{'function': 'count'}]}",
            ConstraintViolationException.class,
            "Constraint violation: horizon=86399999 is not multiple of interval=86400000",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [17] horizon not multiple of interval
          {
            "{'startTime': 1548000000000, 'endTime': 1549000000001, 'interval': 86400000,"
                + " 'horizon': 99872509810, 'aggregates': [{'function': 'count'}]}",
            ConstraintViolationException.class,
            "Constraint violation: horizon=99872509810 is not multiple of interval=86400000",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [18] multiple aggregates
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [19] null aggregates
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000 }",
            InvalidValueException.class,
            "Invalid value: aggregates may not be null or empty",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [20] empty aggregates
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': []}",
            InvalidValueException.class,
            "Invalid value: aggregates may not be null or empty",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [21] null aggregate member
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [null]}",
            InvalidValueException.class,
            "Invalid value: aggregates[0]: aggregate entry may not be null",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [22] duplicate aggregates
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales'},"
                + "{'function': 'count'}]}",
            ConstraintViolationException.class,
            "Constraint violation: aggregates[2]: duplicate event attribute key",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [23] duplicate aggregates again
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales'},"
                + "{'function': 'sum','by': 'sales'}]}",
            ConstraintViolationException.class,
            "Constraint violation: aggregates[2]: duplicate event attribute key",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [24] non-existing sum attribute
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'zipcode'}]}",
            TfosException.class,
            "Attribute name is invalid: zipcode",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [25] non-addable sum attribute
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'country'}]}",
            ConstraintViolationException.class,
            "Attribute 'country' of type STRING for SUM aggregate is not addable",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [26] different case
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'Sales'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [27] duplicate with different cases
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'Sales'},"
                + "{'function': 'sum','by': 'sales'}]}",
            ConstraintViolationException.class,
            "Constraint violation: aggregates[2]: duplicate event attribute key",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [28] same function, different attributes
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales'},"
                + "{'function': 'sum','by': 'sequence'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null), sum("sequence", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [29] different functions, same attribute
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales'},"
                + "{'function': 'min','by': 'sales'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null), min("sales", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [30] different functions, same attribute, different output name
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales', 'as': 'total_sales'},"
                + "{'function': 'min','by': 'sales', 'as': 'minimum_sales'}]}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", "total_sales"), min("sales", "minimum_sales")),
            emptyGroup,
            null,
            null,
            null
          },
          // [31] different functions, same attribute, output name conflict
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales', 'as': 'total'},"
                + "{'function': 'min','by': 'sales', 'as': 'total'}]}",
            ConstraintViolationException.class,
            "Constraint violation: aggregates[2]: duplicate event attribute key",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [32] aggregate output key name conflicts a stream attribute
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': ["
                + "{'function': 'count'},"
                + "{'function': 'sum','by': 'sales', 'as': 'country'},"
                + "{'function': 'min','by': 'sales', 'as': 'minimum'}]}",
            ConstraintViolationException.class,
            "Constraint violation: aggregates[1]:"
                + " event attribute key conflicts with an existing attribute",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [33] specify empty group
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': []}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            emptyGroup,
            null,
            null,
            null
          },
          // [34] specify one group
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country']}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            Arrays.asList("country"),
            null,
            null,
            null
          },
          // [35] specify three groups
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['Country', 'State', 'City']}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            Arrays.asList("country", "state", "city"),
            null,
            null,
            null
          },
          // [36] group keys include null
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', null, 'state']}",
            InvalidValueException.class,
            "Invalid value: group[1]: group key may not be null or empty",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [37] group keys include empty string
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', '', 'state']}",
            InvalidValueException.class,
            "Invalid value: group[1]: group key may not be null or empty",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [38] group keys include non-existing attribute
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', 'state', 'city', 'street']}",
            TfosException.class,
            "Attribute name is invalid: street",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [39] group keys conflict
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', 'state', 'Country']}",
            ConstraintViolationException.class,
            "Constraint violation: group[2]: duplicate attribute; country",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [40] specify sort
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['Country', 'State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'country'}}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            Arrays.asList("country", "state", "city"),
            sort("country", false),
            null,
            null
          },
          // [41] specify sort for aggregate output
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['Country', 'State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'count()'}}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", null)),
            Arrays.asList("country", "state", "city"),
            sort("count()", false),
            null,
            null
          },
          // [42] specify sort for aggregate output #2
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'},"
                + " {'function': 'sum', 'by': 'sales', 'as': 'total_sales'}],"
                + " 'group': ['Country', 'State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'total_sales'}}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", "total_sales")),
            Arrays.asList("country", "state", "city"),
            sort("total_sales", false),
            null,
            null
          },
          // [43] specify non-sort function to sort parameter
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['Country', 'State', 'City'],"
                + " 'sort': {'function': 'distinct', 'by': 'country'}}",
            ConstraintViolationException.class,
            "Constraint violation: non-sort function is specified for sort parameter",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [44] specify sort for non-existing key
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', 'state', 'city'],"
                + " 'sort': {'function': 'sort', 'by': 'street'}}",
            ConstraintViolationException.class,
            "Constraint violation: sort key 'street' does not match any of output attributes",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [45] specify sort for key that is missing in output key but exists in stream config
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}, {'function': 'sum', 'by': 'sales'}],"
                + " 'group': ['country', 'state'],"
                + " 'sort': {'function': 'sort', 'by': 'city'}}",
            ConstraintViolationException.class,
            "Constraint violation: sort key 'city' does not match any of output attributes",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [46] test limit and filter
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'},"
                + " {'function': 'sum', 'by': 'sales', 'as': 'total_sales'}],"
                + " 'group': ['State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'total_sales'},"
                + " 'limit': 12345, 'filter': 'country = `USA`'}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", "total_sales")),
            Arrays.asList("state", "city"),
            sort("total_sales", false),
            12345,
            "country = 'USA'"
          },
          // [47] test negative limitation
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'},"
                + " {'function': 'sum', 'by': 'sales', 'as': 'total_sales'}],"
                + " 'group': ['Country', 'State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'total_sales'},"
                + " 'limit': -1, 'filter': 'street = 123'}",
            InvalidValueException.class,
            "Invalid value: limit=-1: the value must be positive",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [48] test limit and filter
          {
            "{'startTime': 1548460800000, 'endTime': 1548460800001, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'},"
                + " {'function': 'sum', 'by': 'sales', 'as': 'total_sales'}],"
                + " 'group': ['State', 'City'],"
                + " 'sort': {'function': 'sort', 'by': 'total_sales'},"
                + " 'limit': 1, 'filter': 'Country = `usa`'}",
            null,
            null,
            1548460800000L,
            1548547200000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count, sum("sales", "total_sales")),
            Arrays.asList("state", "city"),
            sort("total_sales", false),
            1,
            "Country = 'usa'"
          },
          // [49] Summary range out of boundary
          {
            "{'startTime': 0, 'endTime': 1000, 'interval': 86400000, 'horizon': 172800000,"
                + " 'aggregates': [{'function': 'sum', 'by': 'sequence'}]}",
            ConstraintViolationException.class,
            "Constraint violation: The first summarize window goes out of boundary",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
          // [50] starts earlier than the first checkpoint
          {
            "{'startTime': 1548400000000, 'endTime': 1549000000000, 'interval': 86400000,"
                + " 'aggregates': [{'function': 'count'}]}",
            null,
            null,
            1548400000000L,
            1549065600000L,
            86400000L,
            utc,
            86400000L,
            Arrays.asList(count),
            emptyGroup,
            null,
            null,
            null
          },
          // [51] try to pick up too many summarize time points
          {
            "{'startTime': 1548400000000, 'endTime': 1549000000000, 'interval': 1000,"
                + " 'aggregates': [{'function': 'count'}]}",
            TfosException.class,
            "Too large query scale: Number of summarize time points ",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
          },
        });
  }

  @Test
  public void test() throws Exception {
    if (expectedException != null) {
      thrown.expect(expectedException);
      thrown.expectMessage(message);
    }
    SummarizeRequest request =
        mapper.readValue(
            requestSrc.replaceAll("'", "\"").replaceAll("`", "'"), SummarizeRequest.class);
    SummarizeState state =
        new SummarizeState(
            null, cassStream.getTenantName(), cassStream.getStreamName(), dataEngine.getExecutor());
    state.setInput(request);
    SignalSummarizer.validateSummarizeRequest(cassStream, state);
    assertEquals(startTime, request.getStartTime());
    assertEquals(endTime, request.getEndTime());
    assertEquals(interval, request.getInterval());
    assertEquals(timezone.getRawOffset(), request.getTimezone().getRawOffset());
    assertEquals(horizon, request.getHorizon());
    assertEquals(aggregates, request.getAggregates());
    assertEquals(group, request.getGroup());
    assertEquals(sort, request.getSort());
    assertEquals(limit, request.getLimit());
    assertEquals(filter, request.getFilter());
  }
}
