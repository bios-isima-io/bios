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

import static io.isima.bios.models.AttributeModAllowance.CONVERTIBLES_ONLY;
import static io.isima.bios.models.AttributeModAllowance.FORCE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.AdminTestUtils;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.AdminConfigCreator;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExtractAcrossVersionsTest {

  private static AdminImpl admin;
  private static AdminStore adminStore;
  private static DataServiceHandler dataServiceHandler;
  private static long timestamp;
  private static CassandraConnection conn;

  private String testTenant;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(ExtractAcrossVersionsTest.class);
    admin = (AdminImpl) BiosModules.getAdminInternal();
    adminStore = BiosModules.getAdminStore();
    dataServiceHandler = BiosModules.getDataServiceHandler();
    timestamp = System.currentTimeMillis();
    conn = BiosModules.getCassandraConnection();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    timestamp = System.currentTimeMillis();
  }

  @After
  public void tearDown() throws Exception {
    timestamp += 60000;
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.INITIAL, timestamp);
    admin.removeTenant(
        new TenantConfig(testTenant).setVersion(timestamp), RequestPhase.FINAL, timestamp);
  }

  @Test
  public void testSimple() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_testSimple";
    TenantDesc temp = new TenantDesc(testTenant, System.currentTimeMillis(), Boolean.FALSE);
    admin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    admin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());

    final String testSignal = "testSignal";
    final String origSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'value', 'type': 'long'},"
            + "      {'name': 'when', 'type': 'long'}"
            + "  ]"
            + "}";
    AdminTestUtils.populateStream(admin, testTenant, origSrc, System.currentTimeMillis());

    StreamDesc streamDesc = admin.getStream(testTenant, testSignal);

    final var resp =
        TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "USA,10,20190228");
    TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "Japan,1000,20190301");

    final String newSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'state', 'type': 'string', 'defaultValue': 'n/a'},"
            + "      {'name': 'value', 'type': 'long'},"
            + "      {'name': 'when', 'type': 'double'},"
            + "      {'name': 'record_time', 'type': 'long', 'defaultValue': 1500036268574}"
            + "  ]"
            + "}";
    StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, System.currentTimeMillis());
    admin.modifyStream(
        testTenant, testSignal, newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, testSignal, newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    streamDesc = admin.getStream(testTenant, testSignal);

    TestUtils.insert(
        dataServiceHandler,
        testTenant,
        streamDesc.getName(),
        "USA,CA,20,1551447911000,1551488449000");
    TestUtils.insert(
        dataServiceHandler,
        testTenant,
        streamDesc.getName(),
        "Japan,Tokyo,2000,1551447912000,1551487449000");

    final long start = resp.getTimeStamp();
    final long end = start + 60000;

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);

    List<Event> out = TestUtils.extract(dataServiceHandler, streamDesc, request);
    /*
     * System.out.println(state);
     * for (Event event : out) {
     * System.out.println(event);
     * }
     */

    assertEquals(4, out.size());

    assertEquals("USA", out.get(0).get("country"));
    assertEquals("n/a", out.get(0).get("state"));
    assertEquals(10L, out.get(0).get("value"));
    assertEquals(20190228.0, out.get(0).get("when"));
    assertEquals(1500036268574L, out.get(0).get("record_time"));

    assertEquals("Japan", out.get(1).get("country"));
    assertEquals("n/a", out.get(1).get("state"));
    assertEquals(1000L, out.get(1).get("value"));
    assertEquals(20190301.0, out.get(1).get("when"));

    assertEquals("USA", out.get(2).get("country"));
    assertEquals("CA", out.get(2).get("state"));
    assertEquals(20L, out.get(2).get("value"));
    assertEquals(1551447911000.0, out.get(2).get("when"));

    assertEquals("Japan", out.get(3).get("country"));
    assertEquals("Tokyo", out.get(3).get("state"));
    assertEquals(2000L, out.get(3).get("value"));
    assertEquals(1551447912000.0, out.get(3).get("when"));

    // Now remove an attribute.
    final String thirdSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'state', 'type': 'string', 'defaultValue': 'n/a'},"
            + "      {'name': 'when', 'type': 'double'},"
            + "      {'name': 'record_time', 'type': 'long', 'defaultValue': 1500036268574}"
            + "  ]"
            + "}";
    StreamConfig thirdConf =
        AdminConfigCreator.makeStreamConfig(thirdSrc, System.currentTimeMillis());
    admin.modifyStream(
        testTenant, testSignal, thirdConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, testSignal, thirdConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    streamDesc = admin.getStream(testTenant, testSignal);

    TestUtils.insert(
        dataServiceHandler,
        testTenant,
        streamDesc.getName(),
        "USA-3,CA-3,1551447911003,1551488449003");
    final var resp3 =
        TestUtils.insert(
            dataServiceHandler,
            testTenant,
            streamDesc.getName(),
            "Japan-3,Tokyo-3,1551447912003,1551487449003");

    final long start3 = resp.getTimeStamp();
    final long end3 = resp3.getTimeStamp() + 1000;

    final ExtractRequest request3 = new ExtractRequest();
    request3.setStartTime(start3);
    request3.setEndTime(end3);

    out = TestUtils.extract(dataServiceHandler, streamDesc, request3);
    /*
     * System.out.println(state);
     * for (Event event : out) {
     * System.out.println(event);
     * }
     */

    assertEquals(6, out.size());

    assertEquals("USA", out.get(0).get("country"));
    assertEquals("n/a", out.get(0).get("state"));
    assertEquals(20190228.0, out.get(0).get("when"));
    assertEquals(1500036268574L, out.get(0).get("record_time"));

    assertEquals("Japan", out.get(1).get("country"));
    assertEquals("n/a", out.get(1).get("state"));
    assertEquals(20190301.0, out.get(1).get("when"));

    assertEquals("USA", out.get(2).get("country"));
    assertEquals("CA", out.get(2).get("state"));
    assertEquals(1551447911000.0, out.get(2).get("when"));

    assertEquals("Japan", out.get(3).get("country"));
    assertEquals("Tokyo", out.get(3).get("state"));
    assertEquals(1551447912000.0, out.get(3).get("when"));

    assertEquals("USA-3", out.get(4).get("country"));
    assertEquals("CA-3", out.get(4).get("state"));
    assertEquals(1551447911003.0, out.get(4).get("when"));

    assertEquals("Japan-3", out.get(5).get("country"));
    assertEquals("Tokyo-3", out.get(5).get("state"));
    assertEquals(1551447912003.0, out.get(5).get("when"));

    // reload and try again
    DataEngineImpl engineReloaded = new DataEngineImpl(conn);
    new AdminImpl(adminStore, new MetricsStreamProvider(), engineReloaded);

    List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request3);
    for (Event event : out2) {
      System.out.println(event);
    }
    assertEquals(6, out2.size());

    for (int i = 0; i < 6; ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out2.get(i);
      assertEquals(event1.getEventId(), event2.getEventId());
      assertEquals(event1.getIngestTimestamp(), event2.getIngestTimestamp());
      assertEquals(event1.get("country"), event2.get("country"));
      assertEquals(event1.get("state"), event2.get("state"));
    }
  }

  @Test
  public void testFilter() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_testFilter";
    long timestamp = System.currentTimeMillis();
    TenantDesc temp = new TenantDesc(testTenant, timestamp, Boolean.FALSE);
    admin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    admin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());

    final String testSignal = "testSignal";
    final String origSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'state', 'type': 'enum',"
            + "       'enum': ['CA', 'WA', 'OR']},"
            + "      {'name': 'street', 'type': 'string'},"
            + "      {'name': 'score', 'type': 'long'},"
            + "      {'name': 'zip', 'type': 'double'}"
            + "  ]"
            + "}";
    AdminTestUtils.populateStream(admin, testTenant, origSrc, ++timestamp);

    final StreamDesc streamDesc0 = admin.getStream(testTenant, testSignal);

    final var resp =
        TestUtils.insert(
            dataServiceHandler,
            testTenant,
            streamDesc0.getName(),
            "USA1,CA,Madison Avenue,10,94061");
    TestUtils.insert(
        dataServiceHandler, testTenant, streamDesc0.getName(), "USA2,WA,Hancock,20,95012");

    timestamp += 90000;

    final String newSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'country', 'type': 'string'},"
            + "      {'name': 'state', 'type': 'enum',"
            + "       'enum': ['CA', 'WA', 'OR', 'NV']},"
            + "      {'name': 'city', 'type': 'string', 'defaultValue': 'n/a'},"
            + "      {'name': 'score', 'type': 'double'},"
            + "      {'name': 'zip', 'type': 'long', 'defaultValue': 0}"
            + "  ]"
            + "}";
    StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, System.currentTimeMillis());

    admin.modifyStream(testTenant, testSignal, newConf, RequestPhase.INITIAL, FORCE, Set.of());
    admin.modifyStream(testTenant, testSignal, newConf, RequestPhase.FINAL, FORCE, Set.of());

    final var streamDesc = admin.getStream(testTenant, testSignal);

    timestamp += 90000;

    final var resp2 =
        TestUtils.insert(
            dataServiceHandler, testTenant, streamDesc.getName(), "USA1,OR,Portland,30,94062");
    TestUtils.insert(
        dataServiceHandler, testTenant, streamDesc.getName(), "USA2,NV,Las Vegas,40,12345");

    final long start = resp.getTimeStamp();
    final long end = start + 600000;

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);

    List<Event> out = TestUtils.extract(dataServiceHandler, streamDesc, request);

    assertEquals(4, out.size());

    assertEquals("USA1", out.get(0).get("country"));
    assertEquals("CA", out.get(0).get("state"));
    assertEquals("n/a", out.get(0).get("city"));
    assertNull(out.get(0).get("street"));
    assertEquals(10.0, out.get(0).get("score"));
    assertEquals(0L, out.get(0).get("zip"));

    assertEquals("USA2", out.get(1).get("country"));
    assertEquals("WA", out.get(1).get("state"));
    assertEquals("n/a", out.get(1).get("city"));
    assertNull(out.get(1).get("street"));
    assertEquals(20.0, out.get(1).get("score"));
    assertEquals(0L, out.get(1).get("zip"));

    assertEquals("USA1", out.get(2).get("country"));
    assertEquals("OR", out.get(2).get("state"));
    assertEquals("Portland", out.get(2).get("city"));
    assertNull(out.get(2).get("street"));
    assertEquals(30.0, out.get(2).get("score"));
    assertEquals(94062L, out.get(2).get("zip"));

    assertEquals("USA2", out.get(3).get("country"));
    assertEquals("NV", out.get(3).get("state"));
    assertEquals("Las Vegas", out.get(3).get("city"));
    assertNull(out.get(3).get("street"));
    assertEquals(40.0, out.get(3).get("score"));
    assertEquals(12345L, out.get(3).get("zip"));

    {
      // test filter against unchanged attribute
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("country = 'USA1'");

      List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      for (Event event : out2) {
        System.out.println(event);
      }
      assertEquals(2, out2.size());
      assertEquals(out2.get(0).get("state"), out.get(0).get("state"));
      assertEquals(out2.get(1).get("state"), out.get(2).get("state"));
    }
    {
      // test filter against converted attribute (enum)
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("state = 'WA'");

      List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      for (Event event : out2) {
        System.out.println(event);
      }
      assertEquals(1, out2.size());
      assertEquals(out2.get(0).get("state"), out.get(1).get("state"));
    }
    {
      // test filter against converted attribute (int -> long, including old)
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("score <= 30");

      final List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(3, out2.size());
      for (var event : out2) {
        assertThat((Double) event.get("score"), lessThanOrEqualTo(Double.valueOf(30)));
      }
    }
    {
      // test filter against converted attribute (int -> long, excluding old)
      final ExtractRequest request2 = new ExtractRequest();
      long start2 = resp2.getTimeStamp();
      request2.setStartTime(start2);
      request2.setEndTime(start2 + 60000);
      request2.setFilter("score < 4294967296");

      List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(2, out2.size());
      assertEquals(out2.get(0).get("state"), out.get(2).get("state"));
      assertEquals(out2.get(1).get("state"), out.get(3).get("state"));
    }
    {
      // test filter against converted attribute (long -> int, including old)
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("zip = 94062");

      final var out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(1, out2.size());
      assertEquals("Portland", out2.get(0).get("city"));
    }
    {
      // test filter against converted attribute (long -> int, excluding old)
      final ExtractRequest request2 = new ExtractRequest();
      long start2 = resp2.getTimeStamp();
      request2.setStartTime(start2);
      request2.setEndTime(start2 + 60000);
      request2.setFilter("zip = 94062");

      List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(1, out2.size());
      assertEquals(out2.get(0).get("state"), out.get(2).get("state"));
    }
    {
      // test filter against added attribute (including old)
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("city = 'Las Vegas'");

      final var out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(1, out2.size());
      assertEquals("USA2", out2.get(0).get("country"));
    }
    {
      // test filter against added attribute (excluding old)
      final ExtractRequest request2 = new ExtractRequest();
      long start2 = resp2.getTimeStamp();
      request2.setStartTime(start2);
      request2.setEndTime(start2 + 60000);
      request2.setFilter("city = 'Las Vegas'");

      List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request2);
      assertEquals(1, out2.size());
      assertEquals(out2.get(0).get("state"), out.get(3).get("state"));
    }
    {
      // test filter against deleted attribute (including old)
      final ExtractRequest request2 = new ExtractRequest();
      request2.setStartTime(start);
      request2.setEndTime(end);
      request2.setFilter("street = 'Hancock'");

      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, streamDesc, request2));
      assertThat(exception, instanceOf(FilterNotApplicableException.class));
      assertThat(
          exception.getMessage(),
          is("Invalid filter: Attribute 'street' is not in stream attributes"));
    }
    {
      // test filter against deleted attribute (excluding old)
      final ExtractRequest request2 = new ExtractRequest();
      long start2 = resp2.getTimeStamp();
      request2.setStartTime(start2);
      request2.setEndTime(start2 + 60000);
      request2.setFilter("street = 'Hancock'");

      final var exception =
          assertThrows(
              TfosException.class,
              () -> TestUtils.extract(dataServiceHandler, streamDesc, request2));
      assertThat(exception, instanceOf(FilterNotApplicableException.class));
      assertThat(
          exception.getMessage(),
          is("Invalid filter: Attribute 'street' is not in stream attributes"));
    }

    // reload and try again
    DataEngineImpl engineReloaded = new DataEngineImpl(conn);
    new AdminImpl(adminStore, new MetricsStreamProvider(), engineReloaded);

    List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request);

    assertEquals(4, out2.size());

    for (int i = 0; i < 4; ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out2.get(i);
      assertEquals(event1.getEventId(), event2.getEventId());
      assertEquals(event1.getIngestTimestamp(), event2.getIngestTimestamp());
      assertEquals(event1.get("country"), event2.get("country"));
      assertEquals(event1.get("state"), event2.get("state"));
      assertEquals(event1.get("value"), event2.get("value"));
    }

    request.setFilter("country = 'USA2'");
    List<Event> out3 = TestUtils.extract(dataServiceHandler, streamDesc, request);
    assertEquals(2, out3.size());

    request.setFilter("country = 'USA3'");
    List<Event> out4 = TestUtils.extract(dataServiceHandler, streamDesc, request);
    assertEquals(0, out4.size());
  }

  @Test
  public void testEnumConversion() throws Throwable {
    testTenant = this.getClass().getSimpleName() + "_testEnumConversion";
    TenantDesc temp = new TenantDesc(testTenant, System.currentTimeMillis(), Boolean.FALSE);
    admin.addTenant(temp.toTenantConfig(), RequestPhase.INITIAL, temp.getVersion());
    admin.addTenant(temp.toTenantConfig(), RequestPhase.FINAL, temp.getVersion());

    final String testSignal = "testSignal";
    final String origSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'animal_class', 'type': 'enum',"
            + "       'enum': ['MAMMALS', 'BIRDS', 'FISH', 'REPTILES']}"
            + "  ]"
            + "}";
    AdminTestUtils.populateStream(admin, testTenant, origSrc, System.currentTimeMillis());

    StreamDesc streamDesc = admin.getStream(testTenant, testSignal);

    final var resp = TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "FISH");
    TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "BIRDS");

    final String newSrc =
        "{"
            + "  'name': 'testSignal',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "      {'name': 'animal_class', 'type': 'string'}"
            + "  ]"
            + "}";
    StreamConfig newConf = AdminConfigCreator.makeStreamConfig(newSrc, System.currentTimeMillis());
    admin.modifyStream(
        testTenant, testSignal, newConf, RequestPhase.INITIAL, CONVERTIBLES_ONLY, Set.of());
    admin.modifyStream(
        testTenant, testSignal, newConf, RequestPhase.FINAL, CONVERTIBLES_ONLY, Set.of());

    streamDesc = admin.getStream(testTenant, testSignal);

    final var resp2 =
        TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "Amphibians");
    TestUtils.insert(dataServiceHandler, testTenant, streamDesc.getName(), "Arthropods");

    final long start = resp.getTimeStamp();
    final long end = start + 60000;

    final ExtractRequest request = new ExtractRequest();
    request.setStartTime(start);
    request.setEndTime(end);

    List<Event> out = TestUtils.extract(dataServiceHandler, streamDesc, request);

    assertEquals(4, out.size());

    assertEquals("FISH", out.get(0).get("animal_class"));
    assertEquals("BIRDS", out.get(1).get("animal_class"));
    assertEquals("Amphibians", out.get(2).get("animal_class"));
    assertEquals("Arthropods", out.get(3).get("animal_class"));

    // reload and try again
    DataEngineImpl engineReloaded = new DataEngineImpl(conn);
    new AdminImpl(adminStore, new MetricsStreamProvider(), engineReloaded);

    List<Event> out2 = TestUtils.extract(dataServiceHandler, streamDesc, request);
    for (Event event : out2) {
      System.out.println(event);
    }
    assertEquals(4, out2.size());

    for (int i = 0; i < 4; ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out2.get(i);
      assertEquals(event1.getEventId(), event2.getEventId());
      assertEquals(event1.getIngestTimestamp(), event2.getIngestTimestamp());
      assertEquals(event1.get("animal_class"), event2.get("animal_class"));
    }
  }
}
