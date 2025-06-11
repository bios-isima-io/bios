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
package io.isima.bios.maintenance;

import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.maintenance.DataSketchSpecifier;
import io.isima.bios.data.impl.maintenance.DigestSpecifier;
import io.isima.bios.data.impl.maintenance.PostProcessScheduler;
import io.isima.bios.data.impl.maintenance.PostProcessSpecifiers;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.AdminConfigCreator;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore("TODO(BIOS-6727): Disabled temporarily")
public class PostProcessSchedulerTest {
  Logger logger = LoggerFactory.getLogger(PostProcessSchedulerTest.class);
  private static final String TEST_TABLE_NAME = "post_process_records_test";
  private static final String TEST_TABLE_NAME2 = "data_sketch_records_test";

  private static final String TEST_NAME = PostProcessSchedulerTest.class.getSimpleName();

  private static String savedPostProcessSchedulerTableName;
  private static String savedPostProcessSchedulerTableName2;
  private static String savedDefaultTimeToLive;

  private static AdminInternal admin;
  private static PostProcessScheduler scheduler;
  private static DataEngineImpl dataEngine;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    savedPostProcessSchedulerTableName =
        System.getProperty(PostProcessScheduler.PROPERTY_TABLE_NAME);
    System.setProperty(PostProcessScheduler.PROPERTY_TABLE_NAME, TEST_TABLE_NAME);
    savedPostProcessSchedulerTableName2 =
        System.getProperty(PostProcessScheduler.PROPERTY_SKETCH_RECORDS_TABLE_NAME);
    System.setProperty(PostProcessScheduler.PROPERTY_SKETCH_RECORDS_TABLE_NAME, TEST_TABLE_NAME2);
    savedDefaultTimeToLive = System.getProperty(TfosConfig.SIGNAL_DEFAULT_TIME_TO_LIVE);
    Bios2TestModules.startModulesWithoutMaintenance(PostProcessSchedulerTest.class);
    admin = BiosModules.getAdminInternal();
    dataEngine = (DataEngineImpl) BiosModules.getDataEngine();
    scheduler = dataEngine.getPostProcessScheduler();

    Long timestamp = System.currentTimeMillis() - 10; // To avoid stream versions being older
    final TenantConfig tenantConfig = new TenantConfig(TEST_NAME);
    try {
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // this is fine
    }
    admin.addTenant(tenantConfig, RequestPhase.INITIAL, ++timestamp);
    admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      final TenantConfig tenantConfig = new TenantConfig(TEST_NAME);
      final Long timestamp = System.currentTimeMillis();
      admin.removeTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
      admin.removeTenant(tenantConfig, RequestPhase.FINAL, timestamp);
    } catch (NoSuchTenantException e) {
      // ok
    }
    scheduler.dropTables();
    Bios2TestModules.shutdown();
    TestUtils.revertProperty(
        PostProcessScheduler.PROPERTY_TABLE_NAME, savedPostProcessSchedulerTableName);
    TestUtils.revertProperty(
        PostProcessScheduler.PROPERTY_SKETCH_RECORDS_TABLE_NAME,
        savedPostProcessSchedulerTableName2);
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    TestUtils.revertProperty(TfosConfig.SIGNAL_DEFAULT_TIME_TO_LIVE, savedDefaultTimeToLive);
  }

  @Test
  public void testBasic() throws Exception {
    final String src =
        "{"
            + "  'name': 'test_basic',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOneTwo',"
            + "    'groupBy': ['one', 'two'],"
            + "    'attributes': ['value'],"
            + "    'dataSketches': ['Moments']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOneTwo',"
            + "    'rollups': [{"
            + "      'name': 'rollupByOneTwo',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    long timestamp = System.currentTimeMillis();
    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(src, timestamp);
    admin.addStream(TEST_NAME, signalConf, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf, RequestPhase.FINAL);
    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval = signalDesc.getIndexingInterval();
    assertEquals(300000L, indexingInterval);

    final long signalOriginTimestamp = timestamp / indexingInterval * indexingInterval;

    // indexing time has not come yet
    final var state = TestUtils.makeGenericState();
    final PostProcessSpecifiers specifiers0 =
        scheduler.scheduleForTest(signalDesc, timestamp, state).get();
    assertTrue(specifiers0.isEmpty());

    long schedule1 = 0;
    long schedule2 = 0;
    long schedule3 = 0;

    // test initial scheduling
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();
      try {
        assertEquals(1, specifiers.getIndexes().size());
        schedule1 = specifiers.getIndexes().get(0).getStartTime();
        verifyDigestSpec(
            "test_basic.view.byOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            specifiers.getIndexes().get(0));

        assertEquals(1, specifiers.getRollups().size());
        verifyDigestSpec(
            "rollupByOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            specifiers.getRollups().get(0));

        assertTrue(specifiers.getSketches().size() >= 1);
        verifySketchSpec(
            DataSketchDuration.MINUTES_5,
            (short) 3,
            DataSketchType.MOMENTS,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            specifiers.getSketches());
      } catch (Throwable t) {
        logger.error("FAILURE: s1={}, specifiers: {}", schedule1, specifiers);
        throw t;
      }

      // If you don't post process, the scheduler returns a new schedule with the latest possible
      // coverage
      final PostProcessSpecifiers specifiers2 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      try {
        assertEquals(1, specifiers2.getIndexes().size());
        schedule2 = specifiers2.getIndexes().get(0).getStartTime();
        verifyDigestSpec(
            "test_basic.view.byOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers2.getIndexes().get(0));

        assertEquals(1, specifiers2.getRollups().size());
        verifyDigestSpec(
            "rollupByOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers2.getRollups().get(0));

        assertTrue(specifiers2.getSketches().size() >= 1);
        verifySketchSpec(
            DataSketchDuration.MINUTES_5,
            (short) 3,
            DataSketchType.MOMENTS,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers2.getSketches());
      } catch (Throwable t) {
        logger.error("FAILURE: s1={}, s2={}, specifiers: {}", schedule1, schedule2, specifiers2);
        throw t;
      }

      simulatePostProcessCompletion(signalDesc, specifiers2);
    }

    {
      // Now the indexing has proceeded the latest coverage. timestamp + indexingInterval * 2 should
      // return another schedule with an interval older
      PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      try {
        assertEquals(1, specifiers.getIndexes().size());
        schedule3 = specifiers.getIndexes().get(0).getStartTime();

        verifyDigestSpec(
            "test_basic.view.byOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers.getIndexes().get(0));

        assertEquals(1, specifiers.getRollups().size());
        verifyDigestSpec(
            "rollupByOneTwo",
            signalDesc.getVersion(),
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers.getRollups().get(0));

        assertTrue(specifiers.getSketches().size() >= 1);
        verifySketchSpec(
            DataSketchDuration.MINUTES_5,
            (short) 3,
            DataSketchType.MOMENTS,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp,
            signalOriginTimestamp + indexingInterval * 2,
            specifiers.getSketches());

        simulatePostProcessCompletion(signalDesc, specifiers);

        // The feature has established its full coverage, scheduling again returns none
        PostProcessSpecifiers specifiers2 =
            scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();
        assertEquals(0, specifiers2.getIndexes().size());
        assertEquals(0, specifiers2.getRollups().size());
        assertEquals(0, specifiers2.getSketches().size());
        // assertEquals(specifiers2, specifiers);

      } catch (Throwable t) {
        logger.error(
            "FAILURE: s1={}, s2={}, s3={}, specifiers: {}",
            schedule1,
            schedule2,
            schedule3,
            specifiers);
        throw t;
      }
    }

    {
      // The scheduler returns the next PP specifiers when the time proceeds
      PostProcessSpecifiers specifiers3 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 3, state).get();

      assertEquals(1, specifiers3.getIndexes().size());

      verifyDigestSpec(
          "test_basic.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 3,
          specifiers3.getIndexes().get(0));

      assertEquals(1, specifiers3.getRollups().size());
      verifyDigestSpec(
          "rollupByOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 3,
          specifiers3.getRollups().get(0));

      assertTrue(specifiers3.getSketches().size() >= 1);
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 3,
          DataSketchType.MOMENTS,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 3,
          specifiers3.getSketches());

      simulatePostProcessCompletion(signalDesc, specifiers3);
    }

    // If the timestamp does not proceed, the scheduler does not return PP specs anymore.
    {
      PostProcessSpecifiers specifiers4 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 3, state).get();
      assertTrue(specifiers4.isEmpty());

      specifiers4 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 4, state).get();

      assertEquals(1, specifiers4.getIndexes().size());

      verifyDigestSpec(
          "test_basic.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp + indexingInterval * 4,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 4,
          specifiers4.getIndexes().get(0));

      assertEquals(1, specifiers4.getRollups().size());
      verifyDigestSpec(
          "rollupByOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp + indexingInterval * 4,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 4,
          specifiers4.getRollups().get(0));
    }
  }

  @Test
  public void testMultiViewMultiRollups() throws Exception {
    final String src =
        "{"
            + "  'name': 'test_m_view_m_rollups',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'long'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['two', 'value'],"
            + "    'dataSketches': ['Moments']"
            + "  }, {"
            + "    'name': 'byTwo',"
            + "    'groupBy': ['two'],"
            + "    'attributes': ['value'],"
            + "    'dataSketches': ['Moments']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'rollup3_byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'byTwo',"
            + "    'rollups': [{"
            + "      'name': 'rollup3_byTwo',"
            + "      'interval': {'value': 15, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 15, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    long timestamp = System.currentTimeMillis();

    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(src, timestamp);
    admin.addStream(TEST_NAME, signalConf, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf, RequestPhase.FINAL);
    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval = signalDesc.getIndexingInterval();
    final long longerRollupInterval =
        signalDesc.findRollup("rollup3_byTwo").getInterval().getValueInMillis();
    assertEquals(300000L, indexingInterval);

    final long signalOriginTimestamp = timestamp / indexingInterval * indexingInterval;

    // indexing time has not come yet
    final var state = TestUtils.makeGenericState();
    final PostProcessSpecifiers specifiers0 =
        scheduler.scheduleForTest(signalDesc, timestamp, state).get();
    assertTrue(specifiers0.isEmpty());

    final long longerRollupOrigin = timestamp / longerRollupInterval * longerRollupInterval;

    boolean longerReady;
    boolean rolledup = false;

    // test initial scheduling
    Map<String, Pair<Long, Long>> allIndexEdges = new HashMap<>();
    Map<String, Pair<Long, Long>> allRollupEdges = new HashMap<>();

    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      longerReady =
          longerRollupOrigin
              <= specifiers.getIndexes().get(0).getDoneUntil() - longerRollupInterval;
      System.out.println(longerReady ? "ready" : "not yet");
      final long expectedNumSpecifiers = longerReady ? 4 : 1;
      specifiers.getIndexes().forEach((spec) -> System.out.println(spec));
      assertEquals(expectedNumSpecifiers, specifiers.getIndexes().size());

      verifyDigestSpecs(
          specifiers.getIndexes(),
          "test_m_view_m_rollups.view.byOne",
          "test_m_view_m_rollups.view.byTwo",
          allIndexEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval);

      verifyDigestSpecs(
          specifiers.getRollups(),
          "rollup3_byOne",
          "rollup3_byTwo",
          allRollupEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval);

      assertTrue(specifiers.getSketches().size() >= (longerReady ? 3 : 2));
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 2,
          DataSketchType.MOMENTS,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          specifiers.getSketches());
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 3,
          DataSketchType.MOMENTS,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          specifiers.getSketches());
      if (longerReady) {
        verifySketchSpec(
            DataSketchDuration.MINUTES_15,
            (short) 3,
            DataSketchType.MOMENTS,
            signalOriginTimestamp - indexingInterval * 2,
            signalOriginTimestamp + indexingInterval,
            signalOriginTimestamp - indexingInterval * 2,
            signalOriginTimestamp + indexingInterval,
            specifiers.getSketches());
        rolledup = true;
      }

      // If you don't post process, the scheduler returns the same answer.
      final PostProcessSpecifiers specifiers2 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();
      assertEquals(specifiers, specifiers2);

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // Now the indexing has proceeded one step. timestamp + indexingInterval should return none now.
    {
      longerReady = (timestamp + indexingInterval * 2) % longerRollupInterval < indexingInterval;

      final PostProcessSpecifiers specifiers2 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();
      assertTrue(specifiers2.isEmpty());

      // The scheduler returns the next PP specifiers when the time proceeds
      final PostProcessSpecifiers specifiers3 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      assertThat(specifiers3.getIndexes().size(), greaterThan(0));

      verifyDigestSpecs(
          specifiers3.getIndexes(),
          "test_m_view_m_rollups.view.byOne",
          "test_m_view_m_rollups.view.byTwo",
          allIndexEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval * 2);

      verifyDigestSpecs(
          specifiers3.getRollups(),
          "rollup3_byOne",
          "rollup3_byTwo",
          allRollupEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval * 2);

      if (rolledup) {
        longerReady = false;
      }
      System.out.println(longerReady ? "ready" : "not yet");
      if (longerReady) {
        rolledup = true;
      }

      assertTrue(specifiers3.getSketches().size() >= (longerReady ? 3 : 2));
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 2,
          DataSketchType.MOMENTS,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 2,
          specifiers3.getSketches());
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 3,
          DataSketchType.MOMENTS,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 2,
          specifiers3.getSketches());
      if (longerReady) {
        verifySketchSpec(
            DataSketchDuration.MINUTES_15,
            (short) 3,
            DataSketchType.MOMENTS,
            longerRollupOrigin,
            longerRollupOrigin + indexingInterval * 3,
            longerRollupOrigin,
            longerRollupOrigin + indexingInterval * 3,
            specifiers3.getSketches());
        rolledup = true;
      }

      simulatePostProcessCompletion(signalDesc, specifiers3);
    }

    // If the timestamp does not proceed, the scheduler does not return PP specs anymore.
    {
      longerReady = (timestamp + indexingInterval * 3) % longerRollupInterval < indexingInterval;

      final PostProcessSpecifiers specifiers3 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();
      assertTrue(specifiers3.isEmpty());

      final PostProcessSpecifiers specifiers4 =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 3, state).get();

      assertThat(specifiers4.getIndexes().size(), greaterThan(0));

      verifyDigestSpecs(
          specifiers4.getIndexes(),
          "test_m_view_m_rollups.view.byOne",
          "test_m_view_m_rollups.view.byTwo",
          allIndexEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval * 3);

      verifyDigestSpecs(
          specifiers4.getRollups(),
          "rollup3_byOne",
          "rollup3_byTwo",
          allRollupEdges,
          indexingInterval,
          longerRollupInterval,
          signalOriginTimestamp,
          timestamp + indexingInterval * 3);

      if (rolledup) {
        longerReady = false;
      }
      System.out.println(longerReady ? "ready" : "not yet");

      assertTrue(specifiers4.getSketches().size() >= (longerReady ? 3 : 2));
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 2,
          DataSketchType.MOMENTS,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 3,
          specifiers4.getSketches());
      verifySketchSpec(
          DataSketchDuration.MINUTES_5,
          (short) 3,
          DataSketchType.MOMENTS,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 3,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval * 3,
          specifiers4.getSketches());
      if (longerReady) {
        verifySketchSpec(
            DataSketchDuration.MINUTES_15,
            (short) 3,
            DataSketchType.MOMENTS,
            longerRollupOrigin,
            longerRollupOrigin + indexingInterval * 3,
            longerRollupOrigin,
            longerRollupOrigin + indexingInterval * 3,
            specifiers4.getSketches());
      }
    }
  }

  @Test
  public void testAddARollup() throws Exception {
    final String initialSrc =
        "{"
            + "  'name': 'test_add_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ]"
            + "}";
    final long timestamp0 = System.currentTimeMillis();
    final long timestamp = timestamp0 + 3600000;
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, timestamp0);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.FINAL);

    final String modifiedSrc =
        "{"
            + "  'name': 'test_add_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOneTwo',"
            + "    'groupBy': ['one', 'two'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOneTwo',"
            + "    'rollups': [{"
            + "      'name': 'rollup4',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(modifiedSrc, timestamp);
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.FINAL, FORCE, Set.of());

    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval = signalDesc.getIndexingInterval();
    assertEquals(300000L, indexingInterval);

    final long signalOriginTimestamp = timestamp / indexingInterval * indexingInterval;

    // indexing time has not come yet
    // final PostProcessSpecifiers specifiers0 = scheduler.scheduleInternal(signalDesc, timestamp);
    // assertTrue(specifiers0.isEmpty());

    final var state = TestUtils.makeGenericState();
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_add_a_rollup.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup4",
          signalDesc.getVersion(),
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp + indexingInterval,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // if we ask for scheduling with the same timestamp, the scheduler finds a time window backwards
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_add_a_rollup.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp + indexingInterval,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup4",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp,
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp + indexingInterval,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // The same timestamp, still schedule backwards
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_add_a_rollup.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp + indexingInterval,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup4",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp - indexingInterval,
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp + indexingInterval,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // When the timestamp proceeds, the scheduler gives forwards time
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_add_a_rollup.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 2,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup4",
          signalDesc.getVersion(),
          signalOriginTimestamp + indexingInterval,
          signalOriginTimestamp + indexingInterval * 2,
          signalOriginTimestamp - indexingInterval * 2,
          signalOriginTimestamp + indexingInterval * 2,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // Try reaching the bottom
    for (int i = 2; i < 60 / 5; ++i) {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_add_a_rollup.view.byOneTwo",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval * (i + 1),
          signalOriginTimestamp - indexingInterval * i,
          signalOriginTimestamp - indexingInterval * (i + 1),
          signalOriginTimestamp + indexingInterval * 2,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup4",
          signalDesc.getVersion(),
          signalOriginTimestamp - indexingInterval * (i + 1),
          signalOriginTimestamp - indexingInterval * i,
          signalOriginTimestamp - indexingInterval * (i + 1),
          signalOriginTimestamp + indexingInterval * 2,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // reached to the bottom.
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();
      assertTrue(specifiers.getIndexes().isEmpty());
      assertTrue(specifiers.getRollups().isEmpty());
    }
  }

  @Test
  public void testAppendARollup() throws Exception {
    final String initialSrc =
        "{"
            + "  'name': 'test_append_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'rollup5_byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final long timestamp0 = System.currentTimeMillis();
    final long timestamp = timestamp0 + 3600000;
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, timestamp0);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.FINAL);

    final StreamDesc signalDesc0 = admin.getStream(TEST_NAME, signalConf0.getName());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval0 = signalDesc0.getIndexingInterval();
    assertEquals(300000L, indexingInterval0);

    final long originTimestamp0 = timestamp0 / indexingInterval0 * indexingInterval0;

    // schedule post processes for one hour + 1
    final var state = TestUtils.makeGenericState();
    for (int i = 0; i <= 60 / 5; ++i) {
      final PostProcessSpecifiers specifiers =
          scheduler
              .scheduleForTest(signalDesc0, timestamp0 + indexingInterval0 * (i + 1), state)
              .get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_append_a_rollup.view.byOne",
          signalDesc0.getVersion(),
          originTimestamp0 + indexingInterval0 * i,
          originTimestamp0 + indexingInterval0 * (i + 1),
          originTimestamp0,
          originTimestamp0 + indexingInterval0 * (i + 1),
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup5_byOne",
          signalDesc0.getVersion(),
          originTimestamp0 + indexingInterval0 * i,
          originTimestamp0 + indexingInterval0 * (i + 1),
          originTimestamp0,
          originTimestamp0 + indexingInterval0 * (i + 1),
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc0, specifiers);
    }

    // append a rollup
    final String modifiedSrc =
        "{"
            + "  'name': 'test_append_a_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['value']"
            + "  }, {"
            + "    'name': 'byTwo',"
            + "    'groupBy': ['two'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'rollup5_byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }, {"
            + "    'view': 'byTwo',"
            + "    'rollups': [{"
            + "      'name': 'rollup5_byTwo',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";

    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(modifiedSrc, timestamp);
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.FINAL, FORCE, Set.of());

    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval = signalDesc.getIndexingInterval();
    assertEquals(300000L, indexingInterval);

    final long originTimestamp = timestamp / indexingInterval * indexingInterval;

    // scheduling has one-cycle indexing overlap (old -- done, new --not)
    // so the post process of only new view and rollup would be scheduled.
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_append_a_rollup.view.byTwo",
          signalDesc.getVersion(),
          originTimestamp,
          originTimestamp + indexingInterval,
          originTimestamp,
          originTimestamp + indexingInterval,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup5_byTwo",
          signalDesc.getVersion(),
          originTimestamp,
          originTimestamp + indexingInterval,
          originTimestamp,
          originTimestamp + indexingInterval,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // schedule a cycle of retroactive rollup
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval, state).get();

      assertEquals(1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_append_a_rollup.view.byTwo",
          signalDesc.getVersion(),
          originTimestamp - indexingInterval,
          originTimestamp,
          originTimestamp - indexingInterval,
          originTimestamp + indexingInterval,
          specifiers.getIndexes().get(0));

      assertEquals(1, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup5_byTwo",
          signalDesc.getVersion(),
          originTimestamp - indexingInterval,
          originTimestamp,
          originTimestamp - indexingInterval,
          originTimestamp + indexingInterval,
          specifiers.getRollups().get(0));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // forward scheduling would be back to normal
    {
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp + indexingInterval * 2, state).get();

      assertEquals(2, specifiers.getIndexes().size());
      final Map<String, DigestSpecifier> specs = new HashMap<>();
      specifiers.getIndexes().forEach(entry -> specs.put(entry.getName(), entry));
      verifyDigestSpec(
          "test_append_a_rollup.view.byOne",
          signalDesc.getSchemaVersion(),
          originTimestamp + indexingInterval,
          originTimestamp + indexingInterval * 2,
          originTimestamp0,
          originTimestamp + indexingInterval * 2,
          specs.get("test_append_a_rollup.view.byOne"));
      verifyDigestSpec(
          "test_append_a_rollup.view.byTwo",
          signalDesc.getVersion(),
          originTimestamp + indexingInterval,
          originTimestamp + indexingInterval * 2,
          originTimestamp - indexingInterval,
          originTimestamp + indexingInterval * 2,
          specs.get("test_append_a_rollup.view.byTwo"));

      assertEquals(2, specifiers.getRollups().size());
      verifyDigestSpec(
          "rollup5_byOne",
          signalDesc.getSchemaVersion(),
          originTimestamp + indexingInterval,
          originTimestamp + indexingInterval * 2,
          originTimestamp0,
          originTimestamp + indexingInterval * 2,
          specifiers.getRollups().get(0));
      verifyDigestSpec(
          "rollup5_byTwo",
          signalDesc.getVersion(),
          originTimestamp + indexingInterval,
          originTimestamp + indexingInterval * 2,
          originTimestamp - indexingInterval,
          originTimestamp + indexingInterval * 2,
          specifiers.getRollups().get(1));

      simulatePostProcessCompletion(signalDesc, specifiers);
    }
  }

  @Test
  public void testScheduleForNoRollup() throws Exception {
    final String src =
        "{"
            + "  'name': 'test_scheduleForNoRollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ]"
            + "}";
    final StreamConfig signalConf =
        AdminConfigCreator.makeStreamConfig(src, System.currentTimeMillis());
    admin.addStream(TEST_NAME, signalConf, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf, RequestPhase.FINAL);
    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());
    final var state = TestUtils.makeGenericState();
    final PostProcessSpecifiers schedule =
        scheduler.schedule(signalDesc, System.currentTimeMillis(), 0, state).get();
    assertTrue(schedule.getIndexes().isEmpty());
    assertTrue(schedule.getRollups().isEmpty());
  }

  @Test
  public void testScheduleNoRollupsButSomeViews() throws Exception {
    final String initialSrc =
        "{"
            + "  'name': 'test_scheduleForViewOnly',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOneTwo',"
            + "    'groupBy': ['one', 'two'],"
            + "    'attributes': ['value'],"
            + "    'indexTableEnabled': true,"
            + "    'timeIndexInterval': 300000"
            + "  }]"
            + "}";
    final long timestamp = System.currentTimeMillis();
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, timestamp);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.FINAL);

    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf0.getName());

    final long interval = signalDesc.getIndexingInterval();
    final long expectedInterval = TfosConfig.indexingDefaultIntervalSeconds() * 1000L;
    assertEquals(expectedInterval, interval);

    final var state = TestUtils.makeGenericState();
    final PostProcessSpecifiers schedule =
        scheduler.scheduleForTest(signalDesc, timestamp, state).get();
    assertTrue(schedule.getIndexes().isEmpty());
    assertTrue(schedule.getRollups().isEmpty());

    final long signalOriginTimestamp = signalDesc.getPostProcessOrigin();

    final PostProcessSpecifiers schedule2 =
        scheduler.scheduleForTest(signalDesc, timestamp + interval, state).get();
    assertEquals(1, schedule2.getIndexes().size());
    verifyDigestSpec(
        "test_scheduleForViewOnly.view.byOneTwo",
        signalDesc.getVersion(),
        signalOriginTimestamp,
        signalOriginTimestamp + interval,
        signalOriginTimestamp,
        signalOriginTimestamp + interval,
        schedule2.getIndexes().get(0));
  }

  @Test
  public void testRetroactiveRollup() throws Exception {
    // make the TTL very short
    System.setProperty(TfosConfig.SIGNAL_DEFAULT_TIME_TO_LIVE, "3600");

    final String initialSrc =
        "{"
            + "  'name': 'test_retroactive_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ]"
            + "}";
    final long currentTime = System.currentTimeMillis();
    // make a one-year-old signal
    final long timestamp0 = currentTime;
    final long timestamp = timestamp0 + 365 * 24 * 3600 * 1000;
    final StreamConfig signalConf0 = AdminConfigCreator.makeStreamConfig(initialSrc, timestamp0);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.INITIAL);
    admin.addStream(TEST_NAME, signalConf0, RequestPhase.FINAL);

    // ensure the TTL is set as expected
    final StreamDesc signalDesc0 = admin.getStream(TEST_NAME, signalConf0.getName());
    final var signalCassStream0 = dataEngine.getCassStream(signalDesc0);
    assertEquals(3600000, signalCassStream0.getTtlInTable());

    // append a rollup
    final String modifiedSrc =
        "{"
            + "  'name': 'test_retroactive_rollup',"
            + "  'attributes': ["
            + "    {'name': 'one', 'type': 'string'},"
            + "    {'name': 'two', 'type': 'string'},"
            + "    {'name': 'value', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'byOne',"
            + "    'groupBy': ['one'],"
            + "    'attributes': ['value']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'byOne',"
            + "    'rollups': [{"
            + "      'name': 'test_retroactive_rollup.rollup.byOne',"
            + "      'interval': {'value': 5, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 5, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    final StreamConfig signalConf = AdminConfigCreator.makeStreamConfig(modifiedSrc, timestamp);
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(
        TEST_NAME, signalConf.getName(), signalConf, RequestPhase.FINAL, FORCE, Set.of());

    final StreamDesc signalDesc = admin.getStream(TEST_NAME, signalConf.getName());

    // check the TTL again
    final var signalCassStream = dataEngine.getCassStream(signalDesc);
    assertEquals(3600000, signalCassStream.getTtlInTable());

    // verify the signal descriptor returns correct indexing interval
    final long indexingInterval = signalDesc.getIndexingInterval();
    assertEquals(300000L, indexingInterval);

    final long expectedDoneUntil = timestamp / indexingInterval * indexingInterval;

    // schedule post processes for one hour - 5 mins
    long expectedDoneSince = Long.MIN_VALUE;
    final var state = TestUtils.makeGenericState();
    for (int i = 0; i < 55 / 5; ++i) {
      final var message = String.format("index=%d, %d-%d minutes ago", i, (i + 1) * 5, i * 5);
      final PostProcessSpecifiers specifiers =
          scheduler.scheduleForTest(signalDesc, timestamp, state).get();

      expectedDoneSince = expectedDoneUntil - indexingInterval * (i + 1);
      final long expectedEndTime = expectedDoneSince + indexingInterval;
      assertEquals(message, 1, specifiers.getIndexes().size());
      verifyDigestSpec(
          "test_retroactive_rollup.view.byOne",
          signalDesc.getVersion(),
          expectedDoneSince,
          expectedEndTime,
          expectedDoneSince,
          expectedDoneUntil,
          specifiers.getIndexes().get(0));

      assertEquals(message, 1, specifiers.getRollups().size());
      verifyDigestSpec(
          "test_retroactive_rollup.rollup.byOne",
          signalDesc.getVersion(),
          expectedDoneSince,
          expectedEndTime,
          expectedDoneSince,
          expectedDoneUntil,
          specifiers.getRollups().get(0));

      assertEquals(message, 8, specifiers.getSketches().size());
      assertEquals(message, expectedDoneSince, specifiers.getSketches().get(0).getStartTime());
      assertEquals(message, expectedEndTime, specifiers.getSketches().get(0).getEndTime());
      assertEquals(message, expectedDoneSince, specifiers.getSketches().get(0).getDoneSince());
      assertEquals(message, expectedDoneUntil, specifiers.getSketches().get(0).getDoneUntil());

      simulatePostProcessCompletion(signalDesc, specifiers);
    }

    // At this point, done since should not go prior to one hour ago
    assertThat(expectedDoneSince, lessThan(timestamp - 3600000 + indexingInterval));
    assertThat(expectedDoneSince, greaterThan(timestamp - 3600000));

    // the scheduler should not go beyond one hour
    final PostProcessSpecifiers specifiers =
        scheduler.scheduleForTest(signalDesc, timestamp, state).get();
    assertEquals(0, specifiers.getIndexes().size());
    assertEquals(0, specifiers.getRollups().size());
    assertEquals(0, specifiers.getSketches().size());
  }

  private void verifyDigestSpec(
      String name,
      Long version,
      long startTime,
      long endTime,
      long doneSince,
      long doneUntil,
      DigestSpecifier digestSpecifier) {
    assertNotNull(digestSpecifier);
    assertEquals(name, digestSpecifier.getName());
    assertEquals(version, digestSpecifier.getVersion());
    assertEquals(startTime, digestSpecifier.getStartTime());
    assertEquals(endTime, digestSpecifier.getEndTime());
    assertEquals(doneSince, digestSpecifier.getDoneSince());
    assertEquals(doneUntil, digestSpecifier.getDoneUntil());
  }

  private void verifyDigestSpecs(
      List<DigestSpecifier> specifiers,
      String byOne,
      String byTwo,
      Map<String, Pair<Long, Long>> allEdges,
      long shorterInterval,
      long longerInterval,
      long signalOriginTimestamp,
      long rollupPoint) {
    for (var spec : specifiers) {
      final var name = spec.getName();
      final var edges = allEdges.get(name);
      final long interval = name.equals(byOne) ? shorterInterval : longerInterval;
      assertEquals(spec.getEndTime(), spec.getStartTime() + interval);
      final Pair<Long, Long> nextEdges;
      if (edges == null) {
        final var begin = signalOriginTimestamp / interval * interval;
        assertEquals(begin, spec.getStartTime());
        assertEquals(begin, spec.getDoneSince());
        assertEquals(spec.getEndTime(), spec.getDoneUntil());
        nextEdges = Pair.of(spec.getDoneSince(), spec.getDoneUntil());
      } else {
        if (edges.getRight().longValue() == spec.getStartTime()) {
          assertEquals(edges.getLeft(), Long.valueOf(spec.getDoneSince()));
          assertEquals(spec.getEndTime(), spec.getDoneUntil());
        } else {
          assertEquals(edges.getLeft().longValue(), spec.getEndTime());
          assertEquals(spec.getStartTime(), spec.getDoneSince());
          assertEquals(edges.getRight().longValue(), spec.getDoneUntil());
        }
        nextEdges = Pair.of(spec.getDoneSince(), spec.getDoneUntil());
      }
      allEdges.put(name, nextEdges);
    }

    if (allEdges.containsKey(byOne)) {
      assertEquals(
          rollupPoint / shorterInterval * shorterInterval,
          allEdges.get(byOne).getRight().longValue());
    }

    if (allEdges.containsKey(byTwo)) {
      assertEquals(
          rollupPoint / longerInterval * longerInterval,
          allEdges.get(byTwo).getRight().longValue());
    }
  }

  private void verifySketchSpec(
      DataSketchDuration duration,
      short attributeProxy,
      DataSketchType sketchType,
      long startTime,
      long endTime,
      long doneSince,
      long doneUntil,
      Collection<DataSketchSpecifier> sketchSpecifiers) {
    assertNotNull(sketchSpecifiers);
    boolean foundSpec = false;
    for (final var sketchSpecifier : sketchSpecifiers) {
      if ((sketchSpecifier.getDataSketchDuration() == duration)
          && (sketchSpecifier.getAttributeProxy() == attributeProxy)
          && (sketchSpecifier.getDataSketchType() == sketchType)) {
        foundSpec = true;
        assertEquals(duration, sketchSpecifier.getDataSketchDuration());
        assertEquals(attributeProxy, sketchSpecifier.getAttributeProxy());
        assertEquals(startTime, sketchSpecifier.getStartTime());
        assertEquals(endTime, sketchSpecifier.getEndTime());
        assertEquals(doneSince, sketchSpecifier.getDoneSince());
        assertEquals(doneUntil, sketchSpecifier.getDoneUntil());
      }
    }
    assertTrue(foundSpec);
  }

  private void simulatePostProcessCompletion(
      StreamDesc signalDesc, PostProcessSpecifiers specifiers) throws Exception {
    final var state = TestUtils.makeGenericState();
    for (DigestSpecifier spec : specifiers.getIndexes()) {
      scheduler.recordCompletion(signalDesc, spec, state).get();
    }
    for (DigestSpecifier spec : specifiers.getRollups()) {
      scheduler.recordCompletion(signalDesc, spec, state).get();
    }
    for (DataSketchSpecifier spec : specifiers.getSketches()) {
      scheduler.recordCompletion(signalDesc, spec, state).get();
    }
  }
}
