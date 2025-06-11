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

import static io.isima.bios.sdk.Bios.keys;
import static io.isima.bios.sdk.Bios.rollbackOnFailure;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.metrics.Metrics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
public class AtomicMutationIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String SIGNAL_NAME = "basic";
  private static final String CONTEXT_NAME = "simpleContext";

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

  /**
   * Tests atomic single insertion. Setting atomic option is meaningless for a single insertion, but
   * server accepts it, so it should function as expected.
   */
  @Test
  public void testSignalInsertion() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    final Long timestamp;
    try (final Session session = starter.connect()) {
      // Insert
      final var start = System.currentTimeMillis();
      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME).csv("aaaa,bbbb,12345").build();
      session.execute(rollbackOnFailure(insertStatement));

      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(start, Duration.ofSeconds(10))
              .build();
      final var response = session.execute(selectStatement);

      assertThat(response, notNullValue());
      assertThat(response.getDataWindows().size(), is(1));
      assertThat(response.getDataWindows().get(0).getRecords().size(), is(1));
      assertThat(
          response.getDataWindows().get(0).getRecords().get(0).getAttribute("one").asString(),
          is("aaaa"));
    }
  }

  @Test
  public void testInsertFailure() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final var start = System.currentTimeMillis();

      final var insertStatement =
          Bios.isql().insert().into(SIGNAL_NAME).csv("bee,hive,99301,must,fail").build();

      // Try the statement above, it includes invalid records
      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(insertStatement)));
      assertThat(exception.getCode(), is(BiosClientError.SCHEMA_MISMATCHED));

      // Verify that no records were written
      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(start, Duration.ofSeconds(10))
              .build();
      final var response = session.execute(selectStatement);
      assertThat(response, notNullValue());
      assertThat(response.getDataWindows().size(), is(1));
      assertThat(response.getDataWindows().get(0).getRecords().size(), is(0));
    }
  }

  @Test
  public void testSignalBulkInsertion() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    final Long timestamp;
    try (final Session session = starter.connect()) {
      // Insert
      final var start = System.currentTimeMillis();
      final var insertStatement =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk("xxxx,yyyy,98765", "aaaa,bbbb,12345")
              .build();
      session.execute(rollbackOnFailure(insertStatement));

      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(start, Duration.ofSeconds(10))
              .build();
      final var response = session.execute(selectStatement);

      assertThat(response, notNullValue());
      assertThat(response.getDataWindows().size(), is(1));
      assertThat(response.getDataWindows().get(0).getRecords().size(), is(2));
      assertThat(
          response.getDataWindows().get(0).getRecords().get(0).getAttribute("one").asString(),
          is("xxxx"));
    }
  }

  @Test
  public void testInsertBulkPartialFailure() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final var start = System.currentTimeMillis();

      final var insertStatement =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  List.of(
                      "hello,world,10000",
                      "hello,there,1000,extra,something",
                      "bee,hive,99301,must,fail",
                      "bee,good,699",
                      "good,bye,2020,welcome,2021"))
              .build();

      // Try the statement above, it includes invalid records
      final var exception =
          assertThrows(
              InsertBulkFailedException.class,
              () -> session.execute(rollbackOnFailure(insertStatement)));
      assertThat(exception.getCode(), is(BiosClientError.SCHEMA_MISMATCHED));

      // Verify that no records were written
      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(start, Duration.ofSeconds(10))
              .build();
      final var response = session.execute(selectStatement);
      assertThat(response, notNullValue());
      assertThat(response.getDataWindows().size(), is(1));
      assertThat(response.getDataWindows().get(0).getRecords().size(), is(0));
    }
  }

  @Test
  public void testSelectSignalWithRollback() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final var start = System.currentTimeMillis();

      // Verify that no records were written
      final var selectStatement =
          Bios.isql()
              .select()
              .fromSignal(SIGNAL_NAME)
              .timeRange(start, Duration.ofSeconds(10))
              .build();
      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(selectStatement)));
      assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
    }
  }

  @Test
  public void testContextUpsert() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  List.of(
                      keyStart + ",g",
                      (keyStart + 1) + ",h",
                      (keyStart + 2) + ",i",
                      (keyStart + 3) + ",j"))
              .build();
      session.execute(upsertStatement);

      var selectStatement =
          Bios.isql()
              .select()
              .fromContext(CONTEXT_NAME)
              .where(keys().in(keyStart, keyStart + 1, keyStart + 2, keyStart + 3))
              .build();

      final var response = session.execute(selectStatement);
      assertThat(response.getRecords().size(), is(4));
      assertThat(response.getRecords().get(0).getAttribute("the_value").asString(), is("g"));
      assertThat(response.getRecords().get(1).getAttribute("the_value").asString(), is("h"));
      assertThat(response.getRecords().get(2).getAttribute("the_value").asString(), is("i"));
      assertThat(response.getRecords().get(3).getAttribute("the_value").asString(), is("j"));
    }
  }

  @Test
  public void testContextUpsertPartialFailure() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  List.of(
                      keyStart + ",g",
                      (keyStart + 1) + ",h",
                      (keyStart + 2) + ",i,k",
                      (keyStart + 3) + ",j"))
              .build();
      final var exception =
          assertThrows(BiosClientException.class, () -> session.execute(upsertStatement));
      assertThat(exception.getCode(), is(BiosClientError.SCHEMA_MISMATCHED));

      var selectStatement =
          Bios.isql()
              .select()
              .fromContext(CONTEXT_NAME)
              .where(keys().in(keyStart, keyStart + 1, keyStart + 2, keyStart + 3))
              .build();

      final var response = session.execute(selectStatement);
      assertThat(response.getRecords().size(), is(0));
    }
  }

  @Test
  public void testContextUpsertWithRollback() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  List.of(
                      keyStart + ",g",
                      (keyStart + 1) + ",h",
                      (keyStart + 2) + ",i,k",
                      (keyStart + 3) + ",j"))
              .build();
      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(upsertStatement)));
      assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
    }
  }

  @Test
  public void testContextSelectWithRollback() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var selectStatement =
          Bios.isql()
              .select()
              .fromContext(CONTEXT_NAME)
              .where(keys().in(keyStart, keyStart + 1, keyStart + 2, keyStart + 3))
              .build();

      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(selectStatement)));
      assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
    }
  }

  @Test
  public void testContextUpdateWithRollback() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  List.of(
                      keyStart + ",g",
                      (keyStart + 1) + ",h",
                      (keyStart + 2) + ",i",
                      (keyStart + 3) + ",j"))
              .build();
      session.execute(upsertStatement);

      var deleteStatement =
          Bios.isql()
              .update(CONTEXT_NAME)
              .set(Map.of("the_value", "iiiii"))
              .where(keys().in(keyStart + 2))
              .build();
      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(deleteStatement)));
      assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
    }
  }

  @Test
  public void testContextUDeleteWithRollback() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin");
    try (final Session session = starter.connect()) {

      final long keyStart = new Random().nextLong();

      var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  List.of(
                      keyStart + ",g",
                      (keyStart + 1) + ",h",
                      (keyStart + 2) + ",i",
                      (keyStart + 3) + ",j"))
              .build();
      session.execute(upsertStatement);

      var deleteStatement =
          Bios.isql()
              .delete()
              .fromContext(CONTEXT_NAME)
              .where(keys().in(keyStart + 1, keyStart + 3))
              .build();
      final var exception =
          assertThrows(
              BiosClientException.class, () -> session.execute(rollbackOnFailure(deleteStatement)));
      assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
    }
  }
}
