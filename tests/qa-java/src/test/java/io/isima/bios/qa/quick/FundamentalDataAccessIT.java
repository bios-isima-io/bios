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
package io.isima.bios.qa.quick;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.ISql;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.metrics.Metrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic enablement IT test case for Bios APIs.
 *
 * <p>Setup: ${ROOT}/it/java-qa-bios-setup/setup_fundamenta.py
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>This will eventually contain all new APIs. But now it uses a mix of new and old APIs.
 *   <li>The setup program may move into Java space when the control plain API methods are ready.
 * </ul>
 */
public class FundamentalDataAccessIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String SIGNAL_NAME = "basic";

  static {
    System.setProperty(Metrics.CLIENT_METRICS_ENABLED, "true");
  }

  private static String host;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Test
  public void testSelectGlobalWindowMinimum() throws Exception {
    // Insert; uses new API
    final Long startTime = insertTest();
    // Select; uses new API
    Consumer<List<Record>> recordConsumer =
        (recList) -> {
          assertThat(recList.size(), is(1));
          var rec = recList.get(0);
          assertThat(rec.getAttribute("one").asString(), is("hello"));
          assertThat(rec.getAttribute("two").asString(), is("world"));
          assertThat(rec.getAttribute("three").asLong(), is(99257L));
        };
    selectTest(() -> createQuery(startTime), recordConsumer);
  }

  @Test
  public void testAsyncExecution() throws Exception {
    // insert
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    final Long timestamp;
    try (final Session client = starter.connect()) {
      // Insert
      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME).csv("xxxx,yyyy,98765").build();
      final var future = client.executeAsync(insertStatement);
      final var response = future.toCompletableFuture().get();
      assertNotNull(response);
      assertEquals(1, response.getRecords().size());
      assertNotNull(response.getRecords().get(0).getEventId());
      assertNotNull(response.getRecords().get(0).getTimestamp());
      timestamp = response.getRecords().get(0).getTimestamp();
    }

    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user("extract@" + TENANT_NAME)
            .password("extract")
            .connect()) {
      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(timestamp, Duration.ofSeconds(10))
              .build();
      final var future = session.executeAsync(selectStatement);
      final var response = future.toCompletableFuture().get();
      assertNotNull(response);
      assertEquals(1, response.getDataWindows().size());
      assertEquals(1, response.getDataWindows().get(0).getRecords().size());
      assertEquals(
          "xxxx",
          response.getDataWindows().get(0).getRecords().get(0).getAttribute("one").asString());
    }
  }

  @Test
  public void testInsertBulkAndSelect() throws Exception {
    final Long startTime = insertBulkTest();
    // Select; uses new API
    Consumer<List<Record>> recordConsumer =
        (recList) -> {
          assertThat(recList.size(), is(3));
          final var rec0 = recList.get(0);
          final var rec1 = recList.get(1);
          final var rec2 = recList.get(2);
          assertThat(rec0.getAttribute("one").asString(), is("bee"));
          assertThat(rec0.getAttribute("Total").asLong(), is(100000L));
          assertThat(rec1.getAttribute("one").asString(), is("good"));
          assertThat(rec1.getAttribute("Total").asLong(), is(2020L));
          assertThat(rec2.getAttribute("one").asString(), is("hello"));
          assertThat(rec2.getAttribute("Total").asLong(), is(11000L));
        };
    selectTest(() -> createQuery2(startTime), recordConsumer);
  }

  @Test
  public void testInsertBulkWithErrorAndSelect() throws Exception {
    final Long startTime = insertBulkPartialErrorTest();
    // Select; uses new API
    Consumer<List<Record>> recordConsumer =
        (recList) -> {
          assertThat(recList.size(), is(2));
          var rec0 = recList.get(0);
          var rec1 = recList.get(1);
          assertThat(rec0.getAttribute("one").asString(), is("bee"));
          assertThat(rec0.getAttribute("Count").asLong(), is(1L));
          assertThat(rec1.getAttribute("one").asString(), is("hello"));
          assertThat(rec1.getAttribute("Count").asLong(), is(1L));
        };
    selectTest(() -> createQuery3(startTime), recordConsumer);
  }

  @Test
  public void testLastMetrics() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    final Long startTime;
    final Long endTime;
    try (final Session client = starter.connect()) {
      // Insert
      final var insertStatement =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  "dave,ordered,5",
                  "becky,ordered,1000",
                  "anton,entered,10000",
                  "anton,viewed,99301",
                  "charley,entered,699",
                  "becky,delivered,2020",
                  "charley,viewed,6",
                  "anton,ordered,3")
              .build();
      final var responses = client.execute(insertStatement);
      assertThat(responses.getRecords().size(), is(8));
      startTime = responses.getRecords().get(0).getTimestamp();
      endTime = responses.getRecords().get(7).getTimestamp();
    }
    selectTest(
        // query
        () ->
            Bios.isql()
                .select("one", "last(two)")
                .fromSignal(SIGNAL_NAME)
                .groupBy("one")
                .timeRange(startTime, endTime - startTime + 1)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(4));
          assertThat(records.get(0).attributes().size(), is(2));
          assertThat(records.get(0).getAttribute("one").asString(), is("anton"));
          assertThat(records.get(0).getAttribute("last(two)").asString(), is("ordered"));
          assertThat(records.get(1).attributes().size(), is(2));
          assertThat(records.get(1).getAttribute("one").asString(), is("becky"));
          assertThat(records.get(1).getAttribute("last(two)").asString(), is("delivered"));
          assertThat(records.get(2).attributes().size(), is(2));
          assertThat(records.get(2).getAttribute("one").asString(), is("charley"));
          assertThat(records.get(2).getAttribute("last(two)").asString(), is("viewed"));
          assertThat(records.get(3).attributes().size(), is(2));
          assertThat(records.get(3).getAttribute("one").asString(), is("dave"));
          assertThat(records.get(3).getAttribute("last(two)").asString(), is("ordered"));
        });
  }

  @Test
  public void testSelectGlobalWindowEmptyResult() throws Exception {
    selectTest(
        // query
        () ->
            Bios.isql()
                .select(new ArrayList<String>())
                .fromSignal(SIGNAL_NAME)
                .timeRange(0L, 1000L)
                .build(),
        // result
        (records) -> {
          assertTrue(records.isEmpty());
        });
  }

  private ISql.SignalSelect createQuery(long startTime) {
    return Bios.isql().select().fromSignal(SIGNAL_NAME).timeRange(startTime, 10000).build();
  }

  private ISql.SignalSelect createQuery2(long startTime) {
    return Bios.isql()
        .select("one", "sum(three) as Total")
        .fromSignal(SIGNAL_NAME)
        .groupBy("one")
        .timeRange(startTime, 20000)
        .build();
  }

  private ISql.SignalSelect createQuery3(long startTime) {
    return Bios.isql()
        .select(List.of("one", "count() AS Count"))
        .fromSignal(SIGNAL_NAME)
        .groupBy("one")
        .timeRange(startTime, 20000)
        .build();
  }

  /**
   * Bios style insert.
   *
   * @return startTime in OffsetDateTime as reported in {@code InsertResponse}
   * @throws BiosClientException Tfos exception on error
   */
  private Long insertTest() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    try (final Session client = starter.connect()) {

      // Insert
      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME).csv("hello,world,99257").build();
      final var resp = client.execute(insertStatement);
      assertNotNull(resp);
      assertEquals(1, resp.getRecords().size());
      assertNotNull(resp.getRecords().get(0).getEventId());
      assertNotNull(resp.getRecords().get(0).getTimestamp());
      return resp.getRecords().get(0).getTimestamp();
    }
  }

  /**
   * Bios style insert bulk.
   *
   * @return startTime in OffsetDateTime as reported in {@code InsertResponse}
   * @throws BiosClientException Tfos exception on error
   */
  private Long insertBulkTest() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    try (final Session client = starter.connect()) {

      // Insert
      final var insertStmt =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  Arrays.asList(
                      "hello,world,10000",
                      "hello,there,1000",
                      "bee,hive,99301",
                      "bee,good,699",
                      "good,bye,2020"))
              .build();
      final var response = client.execute(insertStmt);
      assertThat(response.getRecords().size(), is(5));
      Long startTime = null;
      for (var resp : response.getRecords()) {
        if (startTime == null) {
          startTime = resp.getTimestamp();
        }
        assertNotNull(resp.getEventId());
        assertNotNull(resp.getTimestamp());
      }
      return startTime;
    }
  }

  private Long insertBulkPartialErrorTest() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    try (final Session client = starter.connect()) {

      // Insert
      final var insertStmt =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  Arrays.asList(
                      "hello,world,10000",
                      "hello,there,1000,extra,something",
                      "bee,hive,99301,must,fail",
                      "bee,good,699",
                      "good,bye,2020,welcome,2021"))
              .build();
      try {
        client.execute(insertStmt);
        fail("Ingest bulk has partial errors");
      } catch (InsertBulkFailedException e) {
        var respList = e.getResponses().values();
        assertThat(respList.size(), is(2));
        Long startTime = null;
        for (var resp : respList) {
          if (startTime == null) {
            startTime = resp.getTimestamp();
          }
          assertNotNull(resp.getEventId());
          assertNotNull(resp.getTimestamp());
        }
        return startTime;
      }
    }
    return null;
  }

  @Test
  public void testInsertValues() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    long startTime;
    try (final Session client = starter.connect()) {
      // Insert
      final var insertStmt =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .values("a", "value with a comma, inside it", 1111L)
              .build();
      final var resp = client.execute(insertStmt);
      assertNotNull(resp);
      assertEquals(1, resp.getRecords().size());
      assertNotNull(resp.getRecords().get(0).getEventId());
      assertNotNull(resp.getRecords().get(0).getTimestamp());
      startTime = resp.getRecords().get(0).getTimestamp();
    }

    Consumer<List<Record>> recordConsumer =
        (recList) -> {
          assertThat(recList.size(), is(1));
          var rec = recList.get(0);
          assertThat(rec.getAttribute("one").asString(), is("a"));
          assertThat(rec.getAttribute("two").asString(), is("value with a comma, inside it"));
          assertThat(rec.getAttribute("three").asLong(), is(1111L));
        };
    selectTest(() -> createQuery(startTime), recordConsumer);
  }

  @Test
  public void testInsertValuesBulk() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    long startTime;
    try (final Session client = starter.connect()) {
      final var insertStmt =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .valuesBulk(
                  List.of(
                      List.of("b", "value with quote inside \" only once", 2222L),
                      List.of("c", "value with \" two quotes \" inside", 3333L),
                      List.of("d", "with \n newline and \r carriage return", 4444L)))
              .build();
      final var resp = client.execute(insertStmt);
      assertNotNull(resp);
      assertEquals(3, resp.getRecords().size());
      assertNotNull(resp.getRecords().get(0).getEventId());
      assertNotNull(resp.getRecords().get(0).getTimestamp());
      startTime = resp.getRecords().get(0).getTimestamp();
    }

    Consumer<List<Record>> recordConsumer =
        (recList) -> {
          assertThat(recList.size(), is(3));
          var rec = recList.get(0);
          assertThat(rec.getAttribute("one").asString(), is("b"));
          assertThat(
              rec.getAttribute("two").asString(), is("value with quote inside \" only once"));
          assertThat(rec.getAttribute("three").asLong(), is(2222L));
          rec = recList.get(1);
          assertThat(rec.getAttribute("one").asString(), is("c"));
          assertThat(rec.getAttribute("two").asString(), is("value with \" two quotes \" inside"));
          assertThat(rec.getAttribute("three").asLong(), is(3333L));
          rec = recList.get(2);
          assertThat(rec.getAttribute("one").asString(), is("d"));
          assertThat(
              rec.getAttribute("two").asString(), is("with \n newline and \r carriage return"));
          assertThat(rec.getAttribute("three").asLong(), is(4444L));
        };
    selectTest(() -> createQuery(startTime), recordConsumer);
  }

  @Test
  public void insertBulkLargeBatch() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");

    try (final Session client = starter.connect()) {
      final int numEvents = 100000;
      final var csvTexts = new ArrayList<String>();
      for (int i = 0; i < numEvents; ++i) {
        csvTexts.add(String.format("%d,%d,%d", i, i, i));
      }

      // Insert
      final var insertStatement = Bios.isql().insert().into(SIGNAL_NAME).csvBulk(csvTexts).build();
      final var startTime = System.currentTimeMillis();
      final var response = client.execute(insertStatement);
      assertThat(response.getRecords().size(), is(numEvents));

      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(startTime, System.currentTimeMillis() - startTime)
              .build();
      final var selectResponse = client.execute(selectStatement);
      assertThat(selectResponse.getDataWindows().get(0).getRecords().size(), is(numEvents));
      for (int i = 0; i < numEvents; ++i) {
        final var event = selectResponse.getDataWindows().get(0).getRecords().get(i);
        assertThat(event.getAttribute("three").asLong(), is((long) i));
      }
    }
  }

  private void selectTest(Supplier<Statement> querySupplier, Consumer<List<Record>> respConsumer)
      throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session client2 = starter.connect()) {
      final var queryResponse = client2.execute(querySupplier.get());
      final var data = queryResponse.getDataWindows();
      assertThat(data.size(), is(1));
      DataWindow dataWindow = data.get(0);
      List<Record> recList = dataWindow.getRecords();
      respConsumer.accept(recList);
    }
  }

  @Test
  public void testBadFunction() throws Exception {
    invalidSelectTest(
        () -> Bios.isql().select("abcd(a)").fromSignal(SIGNAL_NAME).timeRange(0, 1000).build());
  }

  @Test
  public void testBadAlias() throws Exception {
    invalidSelectTest(
        () ->
            Bios.isql()
                .select("sum(a) alias sum_of_a")
                .fromSignal(SIGNAL_NAME)
                .timeRange(0, 1000)
                .build());
  }

  @Test
  public void testMissingFunctionArgument() throws Exception {
    invalidSelectTest(
        () -> Bios.isql().select("sum()").fromSignal(SIGNAL_NAME).timeRange(0, 1000).build());
  }

  private void invalidSelectTest(Supplier<Statement> querySupplier) throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session client2 = starter.connect()) {
      assertThrows(Exception.class, () -> client2.execute(querySupplier.get()));
    }
  }
}
