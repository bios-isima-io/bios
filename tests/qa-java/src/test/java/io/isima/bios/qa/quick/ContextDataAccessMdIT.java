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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.CompositeKey;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic enablement IT test case for Bios Context Crud APIs.
 *
 * <p>Setup: ${ROOT}/it/java-qa-bios-setup/setup_fundamental.py
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>This will eventually contain all new APIs. But now it uses a mix of new and old APIs.
 *   <li>The setup program may move into Java space when the control plain API methods are ready.
 * </ul>
 */
public class ContextDataAccessMdIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String CONTEXT_NAME = "country_context_md";

  private static String host;
  private static int port;

  private Session testSession;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
    preLoadContextEntries();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (host != null) {
      deletePreLoaded();
    }
  }

  @Before
  public void open() throws Exception {
    testSession =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
  }

  @After
  public void close() throws Exception {
    testSession.close();
  }

  private static void preLoadContextEntries() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {
      final var upsertStatement =
          Bios.isql()
              .upsert()
              .intoContext(CONTEXT_NAME)
              .csvBulk(
                  "Japan,Kanagawa,Yokohama,10",
                  "United States,Colorado,Denver,20",
                  "Japan,Miyagi,Sendai,30",
                  "Japan,Kanagawa,Kawasaki,15",
                  "United States,California,Redwood City,40",
                  "Japan,Aomori,Yokohama,50")
              .build();
      session.execute(upsertStatement);
    }
  }

  private static void deletePreLoaded() throws Exception {
    try (final var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect()) {
      var delete1 =
          Bios.isql()
              .delete()
              .fromContext(CONTEXT_NAME)
              .where(
                  keys()
                      .in(
                          CompositeKey.of("Japan", "Kanagawa", "Yokohama"),
                          CompositeKey.of("United States", "Colorado", "Denver"),
                          CompositeKey.of("Japan", "Miyagi", "Sendai"),
                          CompositeKey.of("Japan", "Kanagawa", "Kawasaki"),
                          CompositeKey.of("Japan", "Aomori", "Yokohama"),
                          CompositeKey.of("United States", "California", "Redwood City")))
              .build();

      session.execute(delete1);
    }
  }

  @Test
  public void extractContextEntriesWithKeysInArray() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where(
                keys()
                    .in(
                        CompositeKey.of("Japan", "Kanagawa", "Yokohama"),
                        CompositeKey.of("Japan", "Kanagawa", "Kawasaki"),
                        CompositeKey.of("United States", "California", "Redwood City"),
                        CompositeKey.of("United States", "California", "San Jose")))
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(3));
    assertThat(records.get(0).getAttribute("country").asString(), is("Japan"));
    assertThat(records.get(0).getAttribute("state").asString(), is("Kanagawa"));
    assertThat(records.get(0).getAttribute("city").asString(), is("Yokohama"));
    assertThat(records.get(0).getAttribute("value").asLong(), is(10L));

    assertThat(records.get(1).getAttribute("country").asString(), is("Japan"));
    assertThat(records.get(1).getAttribute("state").asString(), is("Kanagawa"));
    assertThat(records.get(1).getAttribute("city").asString(), is("Kawasaki"));
    assertThat(records.get(1).getAttribute("value").asLong(), is(15L));

    assertThat(records.get(2).getAttribute("country").asString(), is("United States"));
    assertThat(records.get(2).getAttribute("state").asString(), is("California"));
    assertThat(records.get(2).getAttribute("city").asString(), is("Redwood City"));
    assertThat(records.get(2).getAttribute("value").asLong(), is(40L));
  }

  @Test
  public void extractContextEntriesWithKeysInList() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where(
                keys()
                    .in(
                        List.of(
                            CompositeKey.of("Japan", "Kanagawa", "Yokohama"),
                            CompositeKey.of("Japan", "Kanagawa", "Kawasaki"),
                            CompositeKey.of("United States", "California", "Redwood City"),
                            CompositeKey.of("United States", "California", "San Jose"))))
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(3));
    assertThat(records.get(0).getAttribute("country").asString(), is("Japan"));
    assertThat(records.get(0).getAttribute("state").asString(), is("Kanagawa"));
    assertThat(records.get(0).getAttribute("city").asString(), is("Yokohama"));
    assertThat(records.get(0).getAttribute("value").asLong(), is(10L));

    assertThat(records.get(1).getAttribute("country").asString(), is("Japan"));
    assertThat(records.get(1).getAttribute("state").asString(), is("Kanagawa"));
    assertThat(records.get(1).getAttribute("city").asString(), is("Kawasaki"));
    assertThat(records.get(1).getAttribute("value").asLong(), is(15L));

    assertThat(records.get(2).getAttribute("country").asString(), is("United States"));
    assertThat(records.get(2).getAttribute("state").asString(), is("California"));
    assertThat(records.get(2).getAttribute("city").asString(), is("Redwood City"));
    assertThat(records.get(2).getAttribute("value").asLong(), is(40L));
  }

  @Test
  public void extractContextEntriesWithKeysLackOfDimensions() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where(
                keys()
                    .in(
                        List.of(
                            CompositeKey.of("Japan", "Kanagawa", "Yokohama"),
                            CompositeKey.of("Japan", "Kanagawa", "Kawasaki"),
                            CompositeKey.of("United States", "California"))))
            .build();
    final var exception =
        assertThrows(BiosClientException.class, () -> testSession.execute(statement));
    assertThat(exception.getCode(), is(BiosClientError.BAD_INPUT));
  }

  @Test
  public void extractContextEntriesKeysTooDeeplyNested() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Bios.isql()
                .select()
                .fromContext(CONTEXT_NAME)
                .where(keys().in(List.of(List.of(10L), List.of(20L), List.of(30L))))
                .build());
  }

  @Test
  public void extractContextEntriesWithKeysEq() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where(keys().eq(CompositeKey.of("Japan", "Kanagawa", "Yokohama")))
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(1));
    assertThat(records.get(0).getAttribute("country").asString(), is("Japan"));
    assertThat(records.get(0).getAttribute("state").asString(), is("Kanagawa"));
    assertThat(records.get(0).getAttribute("city").asString(), is("Yokohama"));
    assertThat(records.get(0).getAttribute("value").asLong(), is(10L));
  }

  @Test
  public void extractContextEntriesWhereExactMatch() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where("country = 'United States' AND state = 'California' AND city = 'Redwood City'")
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(1));
    assertThat(records.get(0).getAttribute("country").asString(), is("United States"));
    assertThat(records.get(0).getAttribute("state").asString(), is("California"));
    assertThat(records.get(0).getAttribute("city").asString(), is("Redwood City"));
    assertThat(records.get(0).getAttribute("value").asLong(), is(40L));
  }

  @Test
  public void extractContextEntriesInOperatorForThirdDimension() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where("country = 'Japan' AND state = 'Kanagawa' AND city in ('Yokohama', 'Kawasaki')")
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(2));
    final var locations =
        records.stream()
            .map(
                (record) ->
                    List.of(
                        record.getAttribute("country").asObject(),
                        record.getAttribute("state").asObject(),
                        record.getAttribute("city").asObject()))
            .collect(Collectors.toSet());
    assertThat(
        locations,
        equalTo(
            Set.of(
                List.of("Japan", "Kanagawa", "Yokohama"),
                List.of("Japan", "Kanagawa", "Kawasaki"))));
  }

  @Test
  public void extractContextEntriesInOperatorForSecondDimension() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where("country = 'Japan' AND state in ('Kanagawa', 'Aomori') AND city = 'Yokohama')")
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(2));
    final var locations =
        records.stream()
            .map(
                (record) ->
                    List.of(
                        record.getAttribute("country").asObject(),
                        record.getAttribute("state").asObject(),
                        record.getAttribute("city").asObject()))
            .collect(Collectors.toSet());
    assertThat(
        locations,
        equalTo(
            Set.of(
                List.of("Japan", "Kanagawa", "Yokohama"), List.of("Japan", "Aomori", "Yokohama"))));
  }

  @Test
  public void extractContextEntriesSecondDimensionMissing() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where("country = 'Japan' AND city = 'Yokohama')")
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(2));
    final var locations =
        records.stream()
            .map(
                (record) ->
                    List.of(
                        record.getAttribute("country").asObject(),
                        record.getAttribute("state").asObject(),
                        record.getAttribute("city").asObject()))
            .collect(Collectors.toSet());
    assertThat(
        locations,
        equalTo(
            Set.of(
                List.of("Japan", "Kanagawa", "Yokohama"), List.of("Japan", "Aomori", "Yokohama"))));
  }

  @Test
  public void extractContextEntriesWhereOneFirstDimensionalKey() throws Exception {
    final var statement =
        Bios.isql().select().fromContext(CONTEXT_NAME).where("country = 'Japan'").build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(4));
    final Set<List<Object>> locations =
        records.stream()
            .map(
                (record) ->
                    List.of(
                        record.getAttribute("country").asObject(),
                        record.getAttribute("state").asObject(),
                        record.getAttribute("city").asObject()))
            .collect(Collectors.toSet());
    final Set<List<Object>> expected =
        Set.of(
            List.of("Japan", "Kanagawa", "Yokohama"),
            List.of("Japan", "Kanagawa", "Kawasaki"),
            List.of("Japan", "Miyagi", "Sendai"),
            List.of("Japan", "Aomori", "Yokohama"));
    assertThat(locations, equalTo(expected));
  }

  @Test
  public void extractContextEntriesWhereMultipleFirstDimensionKeys() throws Exception {
    final var statement =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME)
            .where("country in ('Japan', 'United States')")
            .build();
    final var response = testSession.execute(statement);
    assertThat(response.getRecords(), notNullValue());
    final var records = response.getRecords();
    assertThat(records.size(), is(6));
    final var locations =
        records.stream()
            .map(
                (record) ->
                    List.of(
                        record.getAttribute("country").asObject(),
                        record.getAttribute("state").asObject(),
                        record.getAttribute("city").asObject()))
            .collect(Collectors.toSet());
    assertThat(
        locations,
        equalTo(
            Set.of(
                List.of("Japan", "Kanagawa", "Yokohama"),
                List.of("Japan", "Kanagawa", "Kawasaki"),
                List.of("Japan", "Miyagi", "Sendai"),
                List.of("Japan", "Aomori", "Yokohama"),
                List.of("United States", "Colorado", "Denver"),
                List.of("United States", "California", "Redwood City"))));
  }
}
