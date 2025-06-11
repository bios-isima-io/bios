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
package io.isima.bios.qa.withpauses;

import static io.isima.bios.sdk.Bios.keys;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.common.BiosConstants;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.metrics.Metrics;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
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
public class ClientMetricsIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String SIGNAL_NAME = "basic";
  private static final String CONTEXT_NAME = "simpleContext";

  private static Logger logger = Logger.getLogger(ClientMetricsIT.class.getName());

  static {
    System.setProperty(Metrics.CLIENT_METRICS_ENABLED, "true");
  }

  private static String host;
  private static int port;

  private Session session;

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Before
  public void open() throws Exception {
    Thread.sleep(10000);
    session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
  }

  @After
  public void close() throws Exception {
    session.close();
  }

  @Test
  public void testInsert() throws Exception {
    final long insertTime = System.currentTimeMillis();
    doInsert();
    Thread.sleep(60000);

    selectTest(
        // query
        () ->
            Bios.isql()
                .select()
                .fromSignal(BiosConstants.STREAM_CLIENT_METRICS)
                .where(String.format("stream = '%s' AND request = 'INSERT'", SIGNAL_NAME))
                .timeRange(insertTime, 60000)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(1));
          Record record = records.get(0);
          assertThat(record.getAttribute("stream").asString(), is(SIGNAL_NAME));
          assertThat(record.getAttribute("request").asString(), is("INSERT"));
          assertThat(record.getAttribute("numFailedOperations").asLong(), is(0L));
          assertThat(record.getAttribute("numSuccessfulOperations").asLong(), is(1L));
          assertThat(record.getAttribute("numReads").asLong(), is(0L));
          assertThat(record.getAttribute("numWrites").asLong(), is(1L));

          assertThat(record.getAttribute("latencySum").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyMin").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyMax").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalSum").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalMin").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalMax").asLong(), greaterThan(0L));

          assertThat(
              record.getAttribute("latencySum").asLong(),
              greaterThan(record.getAttribute("latencyInternalSum").asLong()));
        });
  }

  @Test
  public void testInsertBulk() throws Exception {
    final long insertTime = System.currentTimeMillis();
    doInsertBulk();
    Thread.sleep(60000);

    selectTest(
        // query
        () ->
            Bios.isql()
                .select()
                .fromSignal(BiosConstants.STREAM_CLIENT_METRICS)
                .where(String.format("stream = '%s' AND request = 'INSERT'", SIGNAL_NAME))
                .timeRange(insertTime, 60000)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(1));
          Record record = records.get(0);
          assertThat(record.getAttribute("stream").asString(), is(SIGNAL_NAME));
          assertThat(record.getAttribute("request").asString(), is("INSERT"));
          assertThat(record.getAttribute("numFailedOperations").asLong(), is(0L));
          assertThat(record.getAttribute("numSuccessfulOperations").asLong(), is(1L));
          assertThat(record.getAttribute("numReads").asLong(), is(0L));
          assertThat(record.getAttribute("numWrites").asLong(), is(5L));
        });
  }

  @Test
  public void testSelect() throws Exception {
    final long insertTime = System.currentTimeMillis();
    doInsertBulk();
    Thread.sleep(10000);

    final long selectTime = System.currentTimeMillis();
    final var queryResponse =
        session.execute(
            Bios.isql()
                .select()
                .fromSignal(SIGNAL_NAME)
                .timeRange(insertTime, selectTime - insertTime + 1)
                .build());
    final var data = queryResponse.getDataWindows();
    assertThat(data.size(), is(1));
    assertEquals(5, data.get(0).getRecords().size());
    Thread.sleep(60000);

    selectTest(
        // query
        () ->
            Bios.isql()
                .select()
                .fromSignal(BiosConstants.STREAM_CLIENT_METRICS)
                .where(String.format("stream = '%s' AND request = 'SELECT'", SIGNAL_NAME))
                .timeRange(selectTime, 60000)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(1));
          Record record = records.get(0);
          assertThat(record.getAttribute("stream").asString(), is(SIGNAL_NAME));
          assertThat(record.getAttribute("request").asString(), is("SELECT"));
          assertThat(record.getAttribute("numFailedOperations").asLong(), is(0L));
          assertThat(record.getAttribute("numSuccessfulOperations").asLong(), is(1L));
          assertThat(record.getAttribute("numReads").asLong(), is(5L));
          assertThat(record.getAttribute("numWrites").asLong(), is(0L));
        });
  }

  @Test
  public void testContext() throws Exception {
    final long upsertTime = System.currentTimeMillis();
    doUpsert();
    Thread.sleep(60000);

    selectTest(
        // query
        () ->
            Bios.isql()
                .select()
                .fromSignal(BiosConstants.STREAM_CLIENT_METRICS)
                .where(String.format("stream = '%s' AND request = 'UPSERT'", CONTEXT_NAME))
                .timeRange(upsertTime, 60000)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(1));
          Record record = records.get(0);
          assertThat(record.getAttribute("stream").asString(), is(CONTEXT_NAME));
          assertThat(record.getAttribute("request").asString(), is("UPSERT"));
          assertThat(record.getAttribute("numFailedOperations").asLong(), is(0L));
          assertThat(record.getAttribute("numSuccessfulOperations").asLong(), is(1L));
          assertThat(record.getAttribute("numReads").asLong(), is(0L));
          assertThat(record.getAttribute("numWrites").asLong(), is(3L));

          assertThat(record.getAttribute("latencySum").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyMin").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyMax").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalSum").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalMin").asLong(), greaterThan(0L));
          assertThat(record.getAttribute("latencyInternalMax").asLong(), greaterThan(0L));

          assertThat(
              record.getAttribute("latencySum").asLong(),
              greaterThan(record.getAttribute("latencyInternalSum").asLong()));
        });

    final long selectTime = System.currentTimeMillis();
    final var resp =
        session.execute(
            Bios.isql()
                .select()
                .fromContext(CONTEXT_NAME)
                .where(keys().in(98901, 98902, 98902))
                .build());
    assertThat(resp.getRecords().size(), is(3));
    Thread.sleep(60000);

    selectTest(
        // query
        () ->
            Bios.isql()
                .select()
                .fromSignal(BiosConstants.STREAM_CLIENT_METRICS)
                .where(String.format("stream = '%s' AND request = 'SELECT'", CONTEXT_NAME))
                .timeRange(selectTime, 60000)
                .build(),
        // result
        (records) -> {
          assertThat(records.size(), is(1));
          Record record = records.get(0);
          assertThat(record.getAttribute("stream").asString(), is(CONTEXT_NAME));
          assertThat(record.getAttribute("request").asString(), is("SELECT"));
          assertThat(record.getAttribute("numFailedOperations").asLong(), is(0L));
          assertThat(record.getAttribute("numSuccessfulOperations").asLong(), is(1L));
          assertThat(record.getAttribute("numReads").asLong(), is(3L));
          assertThat(record.getAttribute("numWrites").asLong(), is(0L));
        });
  }

  private void doInsert() throws Exception {
    final var insertStmt = Bios.isql().insert().into(SIGNAL_NAME).csv("hey, there, 8080").build();
    final var resp = session.execute(insertStmt);
    assertNotNull(resp);
    assertEquals(1, resp.getRecords().size());
    assertNotNull(resp.getRecords().get(0).getEventId());
    assertNotNull(resp.getRecords().get(0).getTimestamp());
  }

  private void doInsertBulk() throws Exception {
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
    final var resp = session.execute(insertStmt);
    assertNotNull(resp);
    assertEquals(5, resp.getRecords().size());
  }

  private void doUpsert() throws Exception {
    final var upsertStmt =
        Bios.isql()
            .upsert()
            .intoContext(CONTEXT_NAME)
            .csvBulk(Arrays.asList("98901,aaa", "98902,bbb", "98903,ccc"))
            .build();
    session.execute(upsertStmt);
  }

  private void selectTest(Supplier<Statement> querySupplier, Consumer<List<Record>> respConsumer)
      throws Exception {
    final var queryResponse = session.execute(querySupplier.get());
    final var data = queryResponse.getDataWindows();
    assertThat(data.size(), is(1));
    DataWindow dataWindow = data.get(0);
    List<Record> recList = dataWindow.getRecords();
    respConsumer.accept(recList);
  }
}
