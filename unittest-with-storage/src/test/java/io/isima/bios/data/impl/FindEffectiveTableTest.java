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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.errors.exception.NoFeatureFoundException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.Count;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FindEffectiveTableTest {

  private static AdminImpl admin;
  private static DataEngineImpl dataEngine;

  private static TenantConfig dummyTenant;
  private static Long timestamp;

  private static StreamDesc streamDescBasic;
  private static SignalCassStream cassStreamBasic;
  private static Long basicIntervalBasic;

  private static String testTenant;

  @BeforeClass
  public static void setUpClass() throws Throwable {
    Bios2TestModules.startModules(
        true,
        FindEffectiveTableTest.class,
        Map.of(DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY, "5000"));
    admin = (AdminImpl) BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();

    testTenant = FindEffectiveTableTest.class.getSimpleName();
    timestamp = System.currentTimeMillis();
    dummyTenant = new TenantConfig(testTenant).setVersion(timestamp);
    admin.addTenant(dummyTenant, RequestPhase.INITIAL, timestamp);
    admin.addTenant(dummyTenant, RequestPhase.FINAL, timestamp);

    final String src =
        "{"
            + "  'name': 'testDefault',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'region', 'type': 'string'},"
            + "    {'name': 'country', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'},"
            + "    {'name': 'city', 'type': 'string'},"
            + "    {'name': 'temperature', 'type': 'long'},"
            + "    {'name': 'humidity', 'type': 'long'},"
            + "    {'name': 'latitude', 'type': 'int'},"
            + "    {'name': 'longtitude', 'type': 'int'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'temperatureByRegionCountryState',"
            + "    'groupBy': ['region', 'country', 'state'],"
            + "    'attributes': ['temperature', 'humidity']"
            + "  }, {"
            + "    'name': 'temperatureByCountryState',"
            + "    'groupBy': ['country', 'state'],"
            + "    'attributes': ['temperature', 'humidity']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'temperatureByRegionCountryState',"
            + "    'rollups': [{"
            + "      'name': 'regionCountryState_1_1',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }, {"
            + "      'name': 'regionCountryState_5_10',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 10, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'temperatureByCountryState',"
            + "    'rollups': [{"
            + "      'name': 'countryState_1_1',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }, {"
            + "      'name': 'countryState_5_10',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 10, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    // build signal desc
    AdminTestUtils.populateStream(admin, testTenant, src, ++timestamp);
    streamDescBasic = admin.getStream(testTenant, "testDefault");

    basicIntervalBasic = 60000L;

    // make the signal cass stream
    cassStreamBasic = (SignalCassStream) dataEngine.getCassStream(streamDescBasic);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Throwable {
    admin.removeTenant(dummyTenant, RequestPhase.INITIAL, ++timestamp);
    admin.removeTenant(dummyTenant, RequestPhase.FINAL, timestamp);

    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Throwable {}

  @After
  public void tearDown() throws Throwable {}

  @Test
  public void testBasicBasic() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testFilterNotSupported() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("longtitude < 40");

    SignalSummarizer.validateSummarizeRequest(cassStreamBasic, state);

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicLongerHorizon() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic * 3);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicLongerInterval() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic * 10;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic * 3);
    request.setHorizon(basicIntervalBasic * 3);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicEvenLongerInterval() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic * 10;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic * 5);
    request.setHorizon(basicIntervalBasic * 5);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_5_10", cassStream.getStreamName());
  }

  @Test
  public void testBasicFewerGroups() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicNoGroupKeys() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("temperature"), new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicExcessiveGroupKeys() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state", "city"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicFullGroupKeys() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state", "region"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("regionCountryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicWithRegionFilter() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("region = 'Americas'");

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals(StreamType.ROLLUP, cassStream.getStreamDesc().getType());
    assertEquals("regionCountryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicWithCountryFilter() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("country in ('USA', 'Canada')");

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals(StreamType.ROLLUP, cassStream.getStreamDesc().getType());
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicWithRegionCountryFilter() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("region = 'Americas' AND country in ('USA', 'Canada')");

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals(StreamType.ROLLUP, cassStream.getStreamDesc().getType());
    assertEquals("regionCountryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicWithNonIndexFilter() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("region"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("country = 'India'");

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals(StreamType.ROLLUP, cassStream.getStreamDesc().getType());
    assertEquals("regionCountryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicWithFilterForNonGroupKey() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);
    request.setFilter("city = 'Dublin'");

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicGroupKeysDoNotMatch() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "city"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicFewerAggregates() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity")));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  @Test
  public void testBasicAggregatesDoNotMatch() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Min("humidity"), new Max("longtitude")));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicExcessiveAggregates() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(
        Arrays.asList(new Min("humidity"), new Sum("temperature"), new Max("longtitude")));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    assertThrows(
        NoFeatureFoundException.class, () -> findTable(cassStreamBasic, state, startTime, endTime));
  }

  @Test
  public void testBasicNoAggregates() throws Throwable {
    final SummarizeState state =
        new SummarizeState(
            null, "testTenant", cassStreamBasic.getStreamName(), dataEngine.getExecutor());
    final long startTime = timestamp;
    final long endTime = startTime + basicIntervalBasic;
    final SummarizeRequest request = new SummarizeRequest();
    state.setInput(request);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    request.setGroup(Arrays.asList("country", "state"));
    request.setAggregates(Arrays.asList(new Count()));
    request.setInterval(basicIntervalBasic);
    request.setHorizon(basicIntervalBasic);

    final SignalCassStream cassStream = findTable(cassStreamBasic, state, startTime, endTime);

    assertNotNull(cassStream);
    assertEquals("countryState_1_1", cassStream.getStreamName());
  }

  private SignalCassStream findTable(
      SignalCassStream cassStream, SummarizeState state, Long startTime, Long endTime)
      throws Throwable {
    SignalSummarizer.validateSummarizeRequest(cassStream, state);
    try {
      return cassStreamBasic.findEffectiveTable(state, startTime, endTime).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
}
