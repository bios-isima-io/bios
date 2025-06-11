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

import static io.isima.bios.admin.v1.StreamConversion.ConversionType.ADD;
import static io.isima.bios.admin.v1.StreamConversion.ConversionType.CONVERT;
import static io.isima.bios.admin.v1.StreamConversion.ConversionType.NO_CHANGE;
import static io.isima.bios.models.AttributeModAllowance.CONVERTIBLES_ONLY;
import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.utils.AdminConfigCreator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiGenModStreamTest {
  private static final Logger logger = LoggerFactory.getLogger(MultiGenModStreamTest.class);

  private static AdminImpl admin;
  private static AdminStore adminStore;
  private static DataEngineImpl dataEngine;
  private static DataServiceHandler dataServiceHandler;
  private static long timestamp;

  // tenant used for a test
  private String testTenant;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModules(
        true,
        MultiGenModStreamTest.class,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "1000",
            "prop.rollupDebugSignal",
            "testStreamAAA"));
    admin = (AdminImpl) BiosModules.getAdminInternal();
    adminStore = BiosModules.getAdminStore();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    timestamp = System.currentTimeMillis();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    timestamp = System.currentTimeMillis();
    admin.setForceUnspecifiedFormat(false);
  }

  @After
  public void tearDown() throws Exception {
    /*
    admin.removeTenant(new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL,
        timestamp);
    admin.removeTenant(new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL,
        timestamp);
     */
  }

  @Test
  public void testSimple() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_simple";
    try {
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ignore
    }

    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    final String firstSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'sequence', 'type': 'long'}"
            + "]}";
    final StreamConfig firstConf = AdminConfigCreator.makeStreamConfig(firstSrc, ++timestamp);
    admin.addStream(testTenant, firstConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, firstConf, RequestPhase.FINAL);

    timestamp += 120000;
    final var resp =
        TestUtils.insert(dataServiceHandler, testTenant, firstConf, ++timestamp, "USA,1");
    TestUtils.insert(dataServiceHandler, testTenant, firstConf, ++timestamp, "Japan,2");
    timestamp += 120000;

    final String secondSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': 'no state info'},"
            + "  {'name': 'sequence', 'type': 'double'}"
            + "]}";
    final StreamConfig secondConf = AdminConfigCreator.makeStreamConfig(secondSrc, ++timestamp);
    admin.modifyStream(
        testTenant, "testStream", secondConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, "testStream", secondConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    timestamp += 120000;
    final var resp2 =
        TestUtils.insert(dataServiceHandler, testTenant, secondConf, ++timestamp, "USA,CA,10");
    TestUtils.insert(dataServiceHandler, testTenant, secondConf, ++timestamp, "Japan,Tokyo,20");
    timestamp += 120000;

    final String thirdSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': 'no state info'},"
            + "  {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'sequence', 'type': 'string'}"
            + "]}";
    final StreamConfig thirdConf = AdminConfigCreator.makeStreamConfig(thirdSrc, ++timestamp);
    admin.modifyStream(
        testTenant, "testStream", thirdConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, "testStream", thirdConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    timestamp += 120000;
    final var resp3 =
        TestUtils.insert(
            dataServiceHandler, testTenant, thirdConf, ++timestamp, "USA,CA,Redwood City,100");
    TestUtils.insert(
        dataServiceHandler, testTenant, thirdConf, ++timestamp, "Japan,Kanagawa,Yokohama,200");
    timestamp += 120000;

    List<Consumer<StreamDesc>> checkers = new ArrayList<>();

    // third
    checkers.add((thirdDesc) -> {});

    // second
    checkers.add(
        (streamDesc) -> {
          final StreamConversion conv = streamDesc.getStreamConversion();
          assertNotNull(conv);
          assertEquals(NO_CHANGE, conv.getAttributeConversion("country").getConversionType());
          assertEquals(CONVERT, conv.getAttributeConversion("sequence").getConversionType());
          assertEquals(
              InternalAttributeType.DOUBLE,
              conv.getAttributeConversion("sequence").getOldDesc().getAttributeType());
        });

    // first
    checkers.add(
        (streamDesc) -> {
          final StreamConversion conv = streamDesc.getStreamConversion();
          assertNotNull(conv);
          assertEquals(NO_CHANGE, conv.getAttributeConversion("country").getConversionType());
          assertEquals(ADD, conv.getAttributeConversion("state").getConversionType());
          assertEquals(CONVERT, conv.getAttributeConversion("sequence").getConversionType());
          assertEquals(
              InternalAttributeType.LONG,
              conv.getAttributeConversion("sequence").getOldDesc().getAttributeType());
        });

    runVerification(checkers, thirdConf, secondConf, firstConf);

    final StreamDesc streamDesc = admin.getStream(testTenant, thirdConf.getName());
    {
      long start = resp.getTimeStamp() - 1800000;
      ExtractRequest request = new ExtractRequest().setStartTime(start).setEndTime(start + 3600000);

      List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request);

      assertEquals(6, events.size());
      assertEquals("USA", events.get(0).get("country"));
      assertEquals("Japan", events.get(1).get("country"));
      assertEquals("USA", events.get(2).get("country"));
      assertEquals("Japan", events.get(3).get("country"));
      assertEquals("USA", events.get(4).get("country"));
      assertEquals("Japan", events.get(5).get("country"));

      assertEquals("no state info", events.get(0).get("state"));
      assertEquals("no state info", events.get(1).get("state"));
      assertEquals("CA", events.get(2).get("state"));
      assertEquals("Tokyo", events.get(3).get("state"));
      assertEquals("CA", events.get(4).get("state"));
      assertEquals("Kanagawa", events.get(5).get("state"));

      assertEquals("n/a", events.get(0).get("city"));
      assertEquals("n/a", events.get(1).get("city"));
      assertEquals("n/a", events.get(2).get("city"));
      assertEquals("n/a", events.get(3).get("city"));
      assertEquals("Redwood City", events.get(4).get("city"));
      assertEquals("Yokohama", events.get(5).get("city"));

      assertThat(events.get(0).get("sequence"), isLike("1"));
      assertThat(events.get(1).get("sequence"), isLike("2"));
      assertThat(events.get(2).get("sequence"), isLike("10"));
      assertThat(events.get(3).get("sequence"), isLike("20"));
      assertThat(events.get(4).get("sequence"), isLike("100"));
      assertThat(events.get(5).get("sequence"), isLike("200"));
    }
    {
      long start2 = resp.getTimeStamp();
      long end2 = resp2.getTimeStamp() - 100;
      ExtractRequest request2 = new ExtractRequest().setStartTime(start2).setEndTime(end2);

      List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(2, events.size());
      assertEquals("no state info", events.get(0).get("state"));
      assertEquals("no state info", events.get(1).get("state"));

      assertEquals("n/a", events.get(0).get("city"));
      assertEquals("n/a", events.get(1).get("city"));

      assertThat(events.get(0).get("sequence"), isLike("1"));
      assertThat(events.get(1).get("sequence"), isLike("2"));
    }
    {
      long start3 = resp2.getTimeStamp();
      long end3 = resp3.getTimeStamp() - 100;
      final ExtractRequest request3 = new ExtractRequest().setStartTime(start3).setEndTime(end3);

      final List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request3);
      assertEquals(2, events.size());
      assertEquals("USA", events.get(0).get("country"));
      assertEquals("Japan", events.get(1).get("country"));

      assertEquals("CA", events.get(0).get("state"));
      assertEquals("Tokyo", events.get(1).get("state"));

      assertEquals("n/a", events.get(0).get("city"));
      assertEquals("n/a", events.get(1).get("city"));

      assertThat(events.get(0).get("sequence"), isLike("10"));
      assertThat(events.get(1).get("sequence"), isLike("20"));
    }
    {
      long start = resp3.getTimeStamp();
      long end = start + 10000;
      final ExtractRequest request3 = new ExtractRequest().setStartTime(start).setEndTime(end);

      final List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request3);
      assertEquals(2, events.size());

      assertEquals("USA", events.get(0).get("country"));
      assertEquals("Japan", events.get(1).get("country"));

      assertEquals("CA", events.get(0).get("state"));
      assertEquals("Kanagawa", events.get(1).get("state"));

      assertEquals("Redwood City", events.get(0).get("city"));
      assertEquals("Yokohama", events.get(1).get("city"));

      assertThat(events.get(0).get("sequence"), isLike("100"));
      assertThat(events.get(1).get("sequence"), isLike("200"));
    }
  }

  private static Matcher<Object> isLike(String param) {
    return isOneOf(param, param + ".0");
  }

  @Test
  public void testRevert() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_revert";
    try {
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ignore
    }
    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    final String firstSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['EMEA', 'APAC', 'AMERICAS']},"
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'sequence', 'type': 'double'}"
            + "]}";
    final StreamConfig firstConf = AdminConfigCreator.makeStreamConfig(firstSrc, ++timestamp);
    admin.addStream(testTenant, firstConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, firstConf, RequestPhase.FINAL);

    timestamp += 120000;
    final var resp =
        TestUtils.insert(
            dataServiceHandler, testTenant, firstConf, ++timestamp, "AMERICAS,USA,6507226550.2");
    TestUtils.insert(
        dataServiceHandler, testTenant, firstConf, ++timestamp, "APAC,Australia,6507226551.3");
    timestamp += 120000;

    final String secondSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': 'no state info'},"
            + "  {'name': 'sequence', 'type': 'long'}"
            + "]}";
    final StreamConfig secondConf = AdminConfigCreator.makeStreamConfig(secondSrc, ++timestamp);

    assertThrows(
        ConstraintViolationException.class,
        () ->
            admin.modifyStream(
                testTenant,
                "testStream",
                secondConf,
                RequestPhase.INITIAL,
                CONVERTIBLES_ONLY,
                Set.of()));

    assertThrows(
        ConstraintViolationException.class,
        () ->
            admin.modifyStream(
                testTenant, "testStream", secondConf, RequestPhase.INITIAL, FORCE, Set.of()));

    secondConf.getAttributes().get(2).setDefaultValue(-1);
    admin.modifyStream(testTenant, "testStream", secondConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(testTenant, "testStream", secondConf, RequestPhase.FINAL, FORCE, Set.of());

    timestamp += 120000;
    TestUtils.insert(dataServiceHandler, testTenant, secondConf, ++timestamp, "Canada,Alberta,11");
    TestUtils.insert(dataServiceHandler, testTenant, secondConf, ++timestamp, "India,Goa,22");
    timestamp += 120000;

    final String thirdSrc =
        "{"
            + "'name': 'testStream',"
            + "'attributes': ["
            + "  {'name': 'region', 'type': 'enum',"
            + "   'enum': ['EMEA', 'APAC', 'AMERICAS', 'UNSPECIFIED']},"
            + "  {'name': 'country', 'type': 'string'},"
            + "  {'name': 'state', 'type': 'string', 'defaultValue': 'no state info'},"
            + "  {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "  {'name': 'sequence', 'type': 'double'}"
            + "]}";
    final StreamConfig thirdConf = AdminConfigCreator.makeStreamConfig(thirdSrc, ++timestamp);

    assertThrows(
        ConstraintViolationException.class,
        () ->
            admin.modifyStream(
                testTenant,
                "testStream",
                thirdConf,
                RequestPhase.INITIAL,
                CONVERTIBLES_ONLY,
                Set.of()));

    thirdConf.getAttributes().get(0).setDefaultValue("UNSPECIFIED");
    admin.modifyStream(
        testTenant, "testStream", thirdConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, "testStream", thirdConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    timestamp += 120000;
    TestUtils.insert(
        dataServiceHandler,
        testTenant,
        thirdConf,
        ++timestamp,
        "EMEA,UK,England,London,807321600000.4");
    TestUtils.insert(
        dataServiceHandler,
        testTenant,
        thirdConf,
        ++timestamp,
        "APAC,Japan,Miyagi,Sendai,981763200000.5");
    timestamp += 120000;

    List<Consumer<StreamDesc>> checkers = new ArrayList<>();

    // third
    checkers.add((thirdDesc) -> {});

    // second
    checkers.add(
        (secondDesc) -> {
          final StreamConversion conv = secondDesc.getStreamConversion();
          assertNotNull(conv);
          assertEquals(ADD, conv.getAttributeConversion("region").getConversionType());
          assertEquals(3, conv.getAttributeConversion("region").getDefaultValue());
          assertEquals(NO_CHANGE, conv.getAttributeConversion("country").getConversionType());
          assertEquals(NO_CHANGE, conv.getAttributeConversion("state").getConversionType());
          assertEquals(ADD, conv.getAttributeConversion("city").getConversionType());
          assertEquals("n/a", conv.getAttributeConversion("city").getDefaultValue());
          assertEquals(CONVERT, conv.getAttributeConversion("sequence").getConversionType());
          assertEquals(
              InternalAttributeType.LONG,
              conv.getAttributeConversion("sequence").getOldDesc().getAttributeType());
        });

    // first
    checkers.add(
        (streamDesc) -> {
          final StreamConversion conv = streamDesc.getStreamConversion();
          assertNotNull(conv);
          assertEquals(NO_CHANGE, conv.getAttributeConversion("region").getConversionType());
          assertEquals(NO_CHANGE, conv.getAttributeConversion("country").getConversionType());
          assertEquals(ADD, conv.getAttributeConversion("state").getConversionType());
          assertEquals("no state info", conv.getAttributeConversion("state").getDefaultValue());
          assertEquals(ADD, conv.getAttributeConversion("city").getConversionType());
          assertEquals("n/a", conv.getAttributeConversion("city").getDefaultValue());
          assertEquals(NO_CHANGE, conv.getAttributeConversion("sequence").getConversionType());
          assertEquals(
              InternalAttributeType.DOUBLE,
              conv.getAttributeConversion("sequence").getOldDesc().getAttributeType());
        });

    runVerification(checkers, thirdConf, secondConf, firstConf);

    final StreamDesc streamDesc = admin.getStream(testTenant, thirdConf.getName());
    {
      long start = resp.getTimeStamp() - 1800000;
      long end = start + 3600000;
      ExtractRequest request = new ExtractRequest().setStartTime(start).setEndTime(end);

      List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request);

      assertEquals(6, events.size());
      assertEquals("AMERICAS", events.get(0).get("region"));
      assertEquals("APAC", events.get(1).get("region"));
      assertEquals("UNSPECIFIED", events.get(2).get("region"));
      assertEquals("UNSPECIFIED", events.get(3).get("region"));
      assertEquals("EMEA", events.get(4).get("region"));
      assertEquals("APAC", events.get(5).get("region"));

      assertEquals("USA", events.get(0).get("country"));
      assertEquals("Australia", events.get(1).get("country"));
      assertEquals("Canada", events.get(2).get("country"));
      assertEquals("India", events.get(3).get("country"));
      assertEquals("UK", events.get(4).get("country"));
      assertEquals("Japan", events.get(5).get("country"));

      assertEquals("no state info", events.get(0).get("state"));
      assertEquals("no state info", events.get(1).get("state"));
      assertEquals("Alberta", events.get(2).get("state"));
      assertEquals("Goa", events.get(3).get("state"));
      assertEquals("England", events.get(4).get("state"));
      assertEquals("Miyagi", events.get(5).get("state"));

      assertEquals("n/a", events.get(0).get("city"));
      assertEquals("n/a", events.get(1).get("city"));
      assertEquals("n/a", events.get(2).get("city"));
      assertEquals("n/a", events.get(3).get("city"));
      assertEquals("London", events.get(4).get("city"));
      assertEquals("Sendai", events.get(5).get("city"));

      assertThat((Double) events.get(0).get("sequence"), closeTo(6507226550.2, 1.e-5));
      assertThat((Double) events.get(1).get("sequence"), closeTo(6507226551.3, 1.e-5));
      assertThat((Double) events.get(2).get("sequence"), closeTo(11.0, 1.e-5));
      assertThat((Double) events.get(3).get("sequence"), closeTo(22.0, 1.e-5));
      assertThat((Double) events.get(4).get("sequence"), closeTo(807321600000.4, 1.e-5));
      assertThat((Double) events.get(5).get("sequence"), closeTo(981763200000.5, 1.e-5));
    }
  }

  @Test
  public void testRollupChange() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_rollup";
    assertTrue(FormatVersion.LATEST > FormatVersion.INITIAL);
    final Integer expectedFormatVersion = FormatVersion.LATEST;

    try {
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(++timestamp), RequestPhase.INITIAL, timestamp);
      admin.removeTenant(
          new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ignore
    }

    TenantDesc tempTenantDesc = new TenantDesc(testTenant, ++timestamp, Boolean.FALSE);
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.INITIAL, tempTenantDesc.getVersion());
    admin.addTenant(
        tempTenantDesc.toTenantConfig(), RequestPhase.FINAL, tempTenantDesc.getVersion());

    final long interval = 60 * 1000;

    if (timestamp % interval > interval * 3 / 4) {
      timestamp += interval / 4 + SignalCassStream.TIME_RANGE_OVERLAP_MILLIS;
    }

    String signalName = "testStreamAAA";
    final String firstSrc =
        "{"
            + "  'name': 'testStreamAAA',"
            + "  'attributes': ["
            + "    {'name': 'country', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'}"
            + "  ]"
            + "}";
    System.out.println("Creating signal (no features), version=" + (timestamp + 1));
    final StreamConfig firstConf = AdminConfigCreator.makeStreamConfig(firstSrc, timestamp + 1);
    admin.addStream(testTenant, firstConf, RequestPhase.INITIAL);
    admin.addStream(testTenant, firstConf, RequestPhase.FINAL);

    timestamp += interval * 2;
    final var firstResp =
        TestUtils.insert(dataServiceHandler, testTenant, firstConf, timestamp + 1, "USA,WA,1");
    TestUtils.insert(
        dataServiceHandler, testTenant, firstConf, timestamp + 2, "Australia,Tasmania,2");
    timestamp += interval * 2;

    final String secondSrc =
        "{"
            + "  'name': 'testStreamAAA',"
            + "  'attributes': ["
            + "    {'name': 'country', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'by_country_state',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'by_country_state',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1_5',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    System.out.println("Modifying the signal to second (adding rollup_1_5), version=" + timestamp);
    final StreamConfig secondConf = AdminConfigCreator.makeStreamConfig(secondSrc, timestamp);
    admin.modifyStream(testTenant, signalName, secondConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(testTenant, signalName, secondConf, RequestPhase.FINAL, FORCE, Set.of());

    TestUtils.insert(
        dataServiceHandler, testTenant, secondConf, timestamp + 10, "Canada,British Columbia,11");
    timestamp += interval / 6;
    TestUtils.insert(dataServiceHandler, testTenant, secondConf, timestamp, "India,Goa,22");
    timestamp += interval * 5 / 6;

    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime(timestamp + 10000);
    Thread.sleep(12000);

    final String thirdSrc =
        "{"
            + "  'name': 'testStreamAAA',"
            + "  'attributes': ["
            + "    {'name': 'country', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'double'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'by_country',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }, {"
            + "    'name': 'by_country_state',"
            + "    'groupBy': ['country', 'state'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'by_country',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1_5',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'by_country_state',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1_2',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    System.out.println(
        "Modifying the signal to third (adding rollup_1_2), version=" + (timestamp + 1));
    final StreamConfig thirdConf = AdminConfigCreator.makeStreamConfig(thirdSrc, ++timestamp);
    admin.modifyStream(testTenant, signalName, thirdConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(testTenant, signalName, thirdConf, RequestPhase.FINAL, FORCE, Set.of());

    TestUtils.insert(dataServiceHandler, testTenant, thirdConf, timestamp, "USA,CA,6507226550");
    TestUtils.insert(
        dataServiceHandler, testTenant, thirdConf, timestamp + 1, "Germany,Hamburg,12");
    TestUtils.insert(dataServiceHandler, testTenant, thirdConf, timestamp + 2, "Japan,Tokyo,953");
    timestamp += interval / 6;
    TestUtils.insert(dataServiceHandler, testTenant, thirdConf, timestamp, "Japan,Mie,990");
    TestUtils.insert(dataServiceHandler, testTenant, thirdConf, timestamp + 1, "USA,CA,6507220010");
    timestamp += interval * 5 / 6;

    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime(timestamp + interval / 6);
    Thread.sleep(12000);

    final String fourthSrc =
        "{"
            + "  'name': 'testStreamAAA',"
            + "  'attributes': ["
            + "    {'name': 'country', 'type': 'string'},"
            + "    {'name': 'state', 'type': 'string'},"
            + "    {'name': 'score', 'type': 'double'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'by_country',"
            + "    'groupBy': ['country'],"
            + "    'attributes': ['score']"
            + "  }, {"
            + "    'name': 'by_country_state',"
            + "    'groupBy': ['country', 'state'],"
            + "    'attributes': ['score']"
            + "  }, {"
            + "    'name': 'by_country_state2',"
            + "    'groupBy': ['country', 'state'],"
            + "    'attributes': ['score']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'by_country',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1_5',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'by_country_state2',"
            + "    'rollups': [{"
            + "      'name': 'rollup_60min',"
            + "      'interval': {'value': 60, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 60, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'by_country_state',"
            + "    'rollups': [{"
            + "      'name': 'rollup_1_2',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    System.out.println(
        "Modifying the signal to fourth (adding rollup_60min), version=" + timestamp);
    final StreamConfig fourthConf = AdminConfigCreator.makeStreamConfig(fourthSrc, timestamp);
    admin.modifyStream(testTenant, signalName, fourthConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(testTenant, signalName, fourthConf, RequestPhase.FINAL, FORCE, Set.of());

    timestamp += interval;

    TestUtils.insert(
        dataServiceHandler, testTenant, fourthConf, timestamp, "Germany,Hamburg,6507226551");
    TestUtils.insert(
        dataServiceHandler, testTenant, fourthConf, timestamp + 1, "USA,OR,6507226559");
    timestamp += interval / 6;
    TestUtils.insert(dataServiceHandler, testTenant, fourthConf, timestamp, "Japan,Hokkaido,993");
    TestUtils.insert(
        dataServiceHandler, testTenant, fourthConf, timestamp + 1, "USA,NV,5507223959");
    final var lastResp =
        TestUtils.insert(
            dataServiceHandler, testTenant, fourthConf, timestamp + 2, "Japan,Hokkaido,879");

    ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .setRollupStopTime((timestamp + interval - 1) / interval * interval);
    System.out.println("\n***************** Sleeping *********************\n");
    Thread.sleep(12000);
    // Thread.sleep(24000);

    List<Consumer<StreamDesc>> checkers = new ArrayList<>();

    // fourth
    checkers.add(
        new Consumer<>() {
          @Override
          public void accept(StreamDesc fourthDesc) {
            assertEquals(fourthConf.getVersion(), fourthDesc.getVersion());
            assertEquals(thirdConf.getVersion(), fourthDesc.getSchemaVersion());
            assertEquals(secondConf.getName(), fourthDesc.getPrevName());
            assertEquals(secondConf.getVersion(), fourthDesc.getPrevVersion());
            assertEquals(expectedFormatVersion, fourthDesc.getFormatVersion());

            final TenantDesc tenantDesc = fourthDesc.getParent();
            assertNotNull(
                tenantDesc.getStream(
                    fourthDesc.getName() + ".view.by_country_state",
                    fourthDesc.getVersion(),
                    true));
            assertNotNull(
                tenantDesc.getStream(
                    fourthDesc.getName() + ".index.by_country_state",
                    fourthDesc.getVersion(),
                    true));
            assertNotNull(tenantDesc.getStream("rollup_1_5", fourthDesc.getVersion(), true));
            assertNotNull(tenantDesc.getStream("rollup_60min", fourthDesc.getVersion(), true));
          }
        });

    // third desc would be skipped since there was no change in signal

    // second
    checkers.add(
        new Consumer<>() {
          @Override
          public void accept(StreamDesc secondDesc) {
            assertEquals(secondConf.getVersion(), secondDesc.getVersion());
            assertEquals(firstConf.getVersion(), secondDesc.getSchemaVersion());
            assertNull(secondDesc.getPrevName());
            assertNull(secondDesc.getPrevVersion());
            assertEquals(expectedFormatVersion, secondDesc.getFormatVersion());

            final StreamConversion conv = secondDesc.getStreamConversion();
            assertNotNull(conv);
            assertEquals(NO_CHANGE, conv.getAttributeConversion("country").getConversionType());
            assertEquals(NO_CHANGE, conv.getAttributeConversion("state").getConversionType());
            assertEquals(CONVERT, conv.getAttributeConversion("score").getConversionType());
            assertEquals(
                InternalAttributeType.LONG,
                conv.getAttributeConversion("score").getOldDesc().getAttributeType());
          }
        });

    runVerification(checkers, fourthConf, secondConf);

    {
      final StreamDesc streamDesc = admin.getStream(testTenant, thirdConf.getName());
      long start = firstResp.getTimeStamp() - 1800000;
      long end = lastResp.getTimeStamp() + 1000;
      ExtractRequest request = new ExtractRequest().setStartTime(start).setEndTime(end);

      List<Event> events = TestUtils.extract(dataServiceHandler, streamDesc, request);
      System.out.println("start=" + start + ", end=" + end);
      for (Event event : events) {
        System.out.println(event);
      }

      assertEquals(14, events.size());

      int i = 0;
      assertEquals("USA", events.get(i).get("country"));
      assertEquals("Australia", events.get(++i).get("country"));
      assertEquals("Canada", events.get(++i).get("country"));
      assertEquals("India", events.get(++i).get("country"));
      assertEquals("USA", events.get(++i).get("country"));
      assertEquals("Germany", events.get(++i).get("country"));
      assertEquals("Japan", events.get(++i).get("country"));
      assertEquals("Japan", events.get(++i).get("country"));
      assertEquals("USA", events.get(++i).get("country"));
      assertEquals("Germany", events.get(++i).get("country"));
      assertEquals("USA", events.get(++i).get("country"));
      assertEquals("Japan", events.get(++i).get("country"));
      assertEquals("USA", events.get(++i).get("country"));
      assertEquals("Japan", events.get(++i).get("country"));

      i = 0;
      assertEquals("WA", events.get(i).get("state"));
      assertEquals("Tasmania", events.get(++i).get("state"));
      assertEquals("British Columbia", events.get(++i).get("state"));
      assertEquals("Goa", events.get(++i).get("state"));
      assertEquals("CA", events.get(++i).get("state"));
      assertEquals("Hamburg", events.get(++i).get("state"));
      assertEquals("Tokyo", events.get(++i).get("state"));
      assertEquals("Mie", events.get(++i).get("state"));
      assertEquals("CA", events.get(++i).get("state"));
      assertEquals("Hamburg", events.get(++i).get("state"));
      assertEquals("OR", events.get(++i).get("state"));
      assertEquals("Hokkaido", events.get(++i).get("state"));
      assertEquals("NV", events.get(++i).get("state"));
      assertEquals("Hokkaido", events.get(++i).get("state"));

      i = 0;
      assertEquals(1.0, (Double) events.get(i).get("score"), 1.e-5);
      assertEquals(2.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(11.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(22.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(6507226550.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(12.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(953.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(990.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(6507220010.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(6507226551.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(6507226559.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(993.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(5507223959.0, (Double) events.get(++i).get("score"), 1.e-5);
      assertEquals(879.0, (Double) events.get(++i).get("score"), 1.e-5);
    }

    {
      final StreamDesc streamDesc = admin.getStream(testTenant, "rollup_1_5");
      long start = (firstResp.getTimeStamp() / interval + 1) * interval;
      long end = start + interval * 15;
      ExtractRequest request = new ExtractRequest().setStartTime(start).setEndTime(end);

      final List<Event> events = TestUtils.extractRollup(dataEngine, streamDesc, request);
      System.out.println("start=" + start + ", end=" + end);
      for (Event event : events) {
        System.out.println(event);
      }
      final long[] ts = new long[6];
      for (int i = 0; i < 6; ++i) {
        ts[i] = start + interval * i;
      }
      final Map<String, Object[]> expected = new LinkedHashMap<>();
      expected.put(
          "ingestTimestamp",
          new Object[] {
            ts[0], ts[0], ts[2], ts[2], ts[3], ts[3], ts[3], ts[5], ts[5], ts[5],
          });
      expected.put(
          "country",
          new Object[] {
            "Australia",
            "USA",
            "Canada",
            "India",
            "Germany",
            "Japan",
            "USA",
            "Germany",
            "Japan",
            "USA",
          });
      expected.put(
          "count",
          new Object[] {
            1L, 1L, 1L, 1L, 1L, 2L, 2L, 1L, 2L, 2L,
          });
      expected.put(
          "score_sum",
          new Object[] {
            2.0, 1.0, 11.0, 22.0, 12.0, 1943.0, 13014446560.0, 6507226551.0, 1872.0, 12014450518.0,
          });
      expected.put(
          "score_min",
          new Object[] {
            2.0, 1.0, 11.0, 22.0, 12.0, 953.0, 6507220010.0, 6507226551.0, 879.0, 5507223959.0,
          });
      expected.put(
          "score_max",
          new Object[] {
            2.0, 1.0, 11.0, 22.0, 12.0, 990.0, 6507226550.0, 6507226551.0, 993.0, 6507226559.0,
          });
      verifyRollup(streamDesc.getName(), expected, events);
    }
    {
      final StreamDesc streamDesc = admin.getStream(testTenant, "rollup_1_2");
      long start = (firstResp.getTimeStamp() / interval + 1) * interval;
      long end = start + interval * 15;
      ExtractRequest request = new ExtractRequest().setStartTime(start).setEndTime(end);

      List<Event> events = TestUtils.extractRollup(dataEngine, streamDesc, request);
      System.out.println("start=" + start + ", end=" + end);
      for (Event event : events) {
        System.out.println(event);
      }
      final long[] ts = new long[6];
      for (int i = 0; i < 6; ++i) {
        ts[i] = start + interval * i;
      }
      final Map<String, Object[]> expected = new LinkedHashMap<>();
      expected.put(
          "ingestTimestamp",
          new Object[] {
            ts[0], ts[0], ts[2], ts[2], ts[3], ts[3], ts[3], ts[3], ts[5], ts[5], ts[5], ts[5],
          });
      expected.put(
          "country",
          new Object[] {
            "Australia",
            "USA",
            "Canada",
            "India",
            "Germany",
            "Japan",
            "Japan",
            "USA",
            "Germany",
            "Japan",
            "USA",
            "USA",
          });
      expected.put(
          "state",
          new Object[] {
            "Tasmania",
            "WA",
            "British Columbia",
            "Goa",
            "Hamburg",
            "Mie",
            "Tokyo",
            "CA",
            "Hamburg",
            "Hokkaido",
            "NV",
            "OR",
          });
      expected.put(
          "count",
          new Object[] {
            1L, 1L, 1L, 1L, 1L, 1L, 1L, 2L, 1L, 2L, 1L, 1L,
          });
      expected.put(
          "score_sum",
          new Object[] {
            2.0,
            1.0,
            11.0,
            22.0,
            12.0,
            990.0,
            953.0,
            13014446560.0,
            6507226551.0,
            1872.0,
            5507223959.0,
            6507226559.0,
          });
      expected.put(
          "score_min",
          new Object[] {
            2.0,
            1.0,
            11.0,
            22.0,
            12.0,
            990.0,
            953.0,
            6507220010.0,
            6507226551.0,
            879.0,
            5507223959.0,
            6507226559.0,
          });
      expected.put(
          "score_max",
          new Object[] {
            2.0,
            1.0,
            11.0,
            22.0,
            12.0,
            990.0,
            953.0,
            6507226550.0,
            6507226551.0,
            993.0,
            5507223959.0,
            6507226559.0,
          });
      verifyRollup(streamDesc.getName(), expected, events);
    }
  }

  // end test cases ///////////////////////////////////////////////////////////

  private void verifyRollup(String name, Map<String, Object[]> expected, List<Event> events) {
    int count = Integer.MAX_VALUE;
    for (Object[] values : expected.values()) {
      count = Math.min(values.length, count);
    }
    for (int i = 0; i < events.size(); ++i) {
      logger.info("events[{}]={}", i, events.get(i));
    }
    assertEquals(count, events.size());
    for (Map.Entry<String, Object[]> entry : expected.entrySet()) {
      final String key = entry.getKey();
      final Object[] values = entry.getValue();
      for (int i = 0; i < values.length; ++i) {
        final String message = String.format("%s[%d]['%s']", name, i, key);
        if (key.equals("ingestTimestamp")) {
          assertEquals(message, values[i], events.get(i).getIngestTimestamp().getTime());
        } else {
          final Object value = values[i];
          if (value instanceof Double) {
            assertEquals(message, (Double) value, (Double) events.get(i).get(key), 1.e-5);
          } else {
            assertEquals(message, value, events.get(i).get(key));
          }
        }
      }
    }
  }

  private void runVerification(List<Consumer<StreamDesc>> checkers, StreamConfig... confArray)
      throws Exception {
    verify(admin, dataEngine, checkers, confArray);

    // reload and verify chain again
    DataEngineImpl reloadedEngine = new DataEngineImpl(BiosModules.getCassandraConnection());
    final AdminImpl reloadedAdmin =
        new AdminImpl(adminStore, new MetricsStreamProvider(), reloadedEngine);
    verify(reloadedAdmin, reloadedEngine, checkers, confArray);
  }

  private void verify(
      AdminInternal admin,
      DataEngineImpl dataEngine,
      List<Consumer<StreamDesc>> checkers,
      StreamConfig[] confArray)
      throws Exception {
    if (confArray.length < 2) {
      throw new IllegalArgumentException("specify at least two");
    }
    // verify chain
    int i;
    for (i = 0; i < confArray.length - 1; ++i) {
      final StreamConfig newConf = confArray[i];
      final StreamConfig oldConf = confArray[i + 1];
      verifyChain(admin, dataEngine, checkers.get(i), checkers.get(i + 1), newConf, oldConf);
    }
    verifyChain(admin, dataEngine, checkers.get(i), null, confArray[i], null);
  }

  private void verifyChain(
      AdminInternal admin,
      DataEngineImpl dataEngine,
      Consumer<StreamDesc> checker,
      Consumer<StreamDesc> prevChecker,
      StreamConfig newConf,
      StreamConfig oldConf)
      throws Exception {

    final boolean hasPrev = oldConf != null;
    // Check AdminInternal
    final TenantDesc tenantDesc = admin.getTenant(testTenant);
    final StreamDesc newDesc = tenantDesc.getStream(newConf.getName(), newConf.getVersion(), true);
    basicCheck(newDesc, newConf);
    checker.accept(newDesc);
    if (hasPrev) {
      basicCheck(newDesc.getPrev(), oldConf);
      prevChecker.accept(newDesc.getPrev());
      assertEquals(oldConf.getName(), newDesc.getPrevName());
      assertEquals(oldConf.getVersion(), newDesc.getPrevVersion());
      final StreamDesc oldDesc =
          tenantDesc.getStream(oldConf.getName(), oldConf.getVersion(), true);
      assertEquals(newDesc.getPrev(), oldDesc);
      prevChecker.accept(oldDesc);
    } else {
      assertNull(newDesc.getPrev());
      assertNull(newDesc.getPrevName());
      assertNull(newDesc.getPrevVersion());
    }

    // Check Data Engine
    final CassStream newCassStream = dataEngine.getCassStream(newDesc);
    assertNotNull(newCassStream);
    basicCheck(newCassStream.getStreamDesc(), newConf);
    checker.accept(newCassStream.getStreamDesc());
    // checkNew(newCassStream.getStreamDesc(), newConf, oldConf);
    if (hasPrev) {
      assertNotNull(newCassStream.getPrev());
      assertEquals(newDesc.getPrev(), newCassStream.getPrev().getStreamDesc());
      prevChecker.accept(newCassStream.getPrev().getStreamDesc());

      final CassStream oldCassStream = dataEngine.getCassStream(newDesc.getPrev());
      assertNotNull(oldCassStream);
      assertSame(oldCassStream, newCassStream.getPrev());
      // basicCheck(oldCassStream.getStreamDesc(), origConf);
      // checkOld(oldCassStream.getStreamDesc(), newConf, origConf);
    } else {
      assertNull(newCassStream.getPrev());
      assertNull(newCassStream.getStreamDesc().getPrev());
    }
    /*
     * extraCheck(admin, dataEngine, newConf, origConf);
     */
  }

  private void basicCheck(StreamDesc desc, StreamConfig conf) {
    assertNotNull(desc);
    assertEquals(conf.getName(), desc.getName());
    assertEquals(conf.getVersion(), desc.getVersion());
  }
}
