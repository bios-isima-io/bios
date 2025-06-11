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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.exceptions.InsertBulkFailedException;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * IT testcase for testing enrich policy.
 *
 * <p>Setup: ${ROOT}/it/java-qa-bios-setup/setup_fundamental.py
 *
 * <p>Notes:
 */
public class EnrichIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String CUSTOMER_CONTEXT_NAME = "customer_context";
  private static final String COUNTRY_CONTEXT_NAME = "country_context";
  private static final String SIGNAL_ENRICH_GLOBAL_MVP_NAME = "enrich_global_mvp_policy";
  private static final String SIGNAL_ENRICH_LOCAL_MVP_NAME = "enrich_local_mvp_policy";
  private static final String SIGNAL_ENRICH_MULTI_MVP_NAME = "enrich_multi_mvp_policy";

  private static final Integer[] CUSTOMER_IDS = {1, 2, 3, 4, 5, 6};
  private static final List<String> CUSTOMER_CONTEXT_DATA =
      Arrays.asList("1,A,MALE", "2,B,FEMALE", "3,C,FEMALE", "4,D,FEMALE", "5,E,FEMALE", "6,F,MALE");
  private static final String FIRST_CUSTOMER = "A";
  private static final String DEFAULT_CUSTOMER = "empty";
  private static final String CUSTOMER_KEY_ATTR = "customer_id";
  private static final String CUSTOMER_NAME_IN_SIGNAL = "customer_name";
  private static final String CUSTOMER_GENDER = "gender";
  private static final String FIRST_GENDER = "MALE";
  private static final String UNKNOWN_GENDER = "UNKNOWN";

  private static final Integer[] COUNTRY_IDS = {1000, 1001, 1002};
  private static final List<String> COUNTRY_CONTEXT_DATA =
      Arrays.asList("1000,USA,AZ,Tempe", "1001,USA,FL,Gainsville", "1002,India,KA,Bangalore");
  private static final String COUNTRY_COUNTRY_ATTR = "country";
  private static final String COUNTRY_STATE_ATTR = "state";
  private static final String COUNTRY_CITY_ATTR = "city";

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
    deletePreLoaded();
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

  @Test
  public void testEnrichMvpLocal() throws BiosClientException {
    var stmt = Bios.isql().insert().into(SIGNAL_ENRICH_LOCAL_MVP_NAME).csv("1,London").build();
    final var resp = testSession.execute(stmt);

    final var select =
        Bios.isql()
            .select()
            .fromSignal(SIGNAL_ENRICH_LOCAL_MVP_NAME)
            .timeRange(resp.getRecords().get(0).getTimestamp(), Duration.ofHours(1))
            .build();

    final var selectResponses = testSession.multiExecute(select);

    assertThat(selectResponses.size(), is(1));
    DataWindow result = selectResponses.get(0).getDataWindows().get(0);
    Record record = result.getRecords().get(0);
    assertThat(record.getAttribute(CUSTOMER_NAME_IN_SIGNAL).asString(), is(FIRST_CUSTOMER));
    assertThat(record.getAttribute(CUSTOMER_KEY_ATTR).asLong(), is(1L));
    assertThat(record.getAttribute("travel_destination").asString(), is("London"));
  }

  @Test
  public void testEnrichMvpLocalMissing() {
    try {
      var stmt = Bios.isql().insert().into(SIGNAL_ENRICH_LOCAL_MVP_NAME).csv("8,NewYork").build();
      testSession.execute(stmt);
      fail("Insert should not succeed as customer id is missing in context");
    } catch (BiosClientException e) {
      assertThat(e.getCode(), is(BiosClientError.BAD_INPUT));
      assertThat(e.getMessage(), containsString("Missed"));
    }
  }

  @Test
  public void testEnrichMvpGlobal() throws BiosClientException {
    var stmt =
        Bios.isql()
            .insert()
            .into(SIGNAL_ENRICH_GLOBAL_MVP_NAME)
            .csvBulk("1,Delhi", "10,Chennai")
            .build();
    final var resp = testSession.execute(stmt);

    final var select =
        Bios.isql()
            .select()
            .fromSignal(SIGNAL_ENRICH_GLOBAL_MVP_NAME)
            .timeRange(resp.getRecords().get(0).getTimestamp(), Duration.ofMinutes(1))
            .build();

    final var selectResponses = testSession.multiExecute(select);

    DataWindow result = selectResponses.get(0).getDataWindows().get(0);
    List<Record> records = result.getRecords();
    for (var rec : records) {
      switch ((int) rec.getAttribute(CUSTOMER_KEY_ATTR).asLong()) {
        case 1:
          assertThat(rec.getAttribute(CUSTOMER_NAME_IN_SIGNAL).asString(), is(FIRST_CUSTOMER));
          assertThat(rec.getAttribute("travel_destination").asString(), is("Delhi"));
          break;

        case 10:
          assertThat(rec.getAttribute(CUSTOMER_NAME_IN_SIGNAL).asString(), is(DEFAULT_CUSTOMER));
          assertThat(rec.getAttribute("travel_destination").asString(), is("Chennai"));
          break;

        default:
          fail("Unexpected customer ID");
      }
    }
  }

  @Test
  public void testEnrichMvpMulti() throws BiosClientException {
    var start = System.currentTimeMillis();
    try {
      final var stmt =
          Bios.isql()
              .insert()
              .into(SIGNAL_ENRICH_MULTI_MVP_NAME)
              .csvBulk("1,1000,Beer", "10,1002,Whisky", "15,1020,Rum")
              .build();
      testSession.execute(stmt);
      fail("Must partially fail based on policy");
    } catch (InsertBulkFailedException e) {
      assertThat(e.getReasons().size(), is(1));
      assertThat(e.getReasons().get(2).getCode(), is(BiosClientError.BAD_INPUT));
      assertThat(e.getReasons().get(2).getMessage(), containsString("Missed"));
      assertThat(e.getResponses().size(), is(2));
      start = e.getResponses().get(0).getTimestamp();
    }

    final var select =
        Bios.isql()
            .select()
            .fromSignal(SIGNAL_ENRICH_MULTI_MVP_NAME)
            .timeRange(start, Duration.ofMinutes(1))
            .build();

    final var selectResponses = testSession.multiExecute(select);

    final DataWindow result = selectResponses.get(0).getDataWindows().get(0);
    final List<Record> records = result.getRecords();
    for (var rec : records) {
      switch ((int) rec.getAttribute(CUSTOMER_KEY_ATTR).asLong()) {
        case 1:
          assertThat(rec.getAttribute(CUSTOMER_NAME_IN_SIGNAL).asString(), is(FIRST_CUSTOMER));
          assertThat(rec.getAttribute(CUSTOMER_GENDER).asString(), is(FIRST_GENDER));
          assertThat(rec.getAttribute("product").asString(), is("Beer"));
          assertThat(rec.getAttribute(COUNTRY_COUNTRY_ATTR).asString(), is("USA"));
          break;

        case 10:
          assertThat(rec.getAttribute(CUSTOMER_NAME_IN_SIGNAL).asString(), is(DEFAULT_CUSTOMER));
          assertThat(rec.getAttribute("product").asString(), is("Whisky"));
          assertThat(rec.getAttribute(CUSTOMER_GENDER).asString(), is(UNKNOWN_GENDER));
          assertThat(rec.getAttribute(COUNTRY_STATE_ATTR).asString(), is("KA"));
          assertThat(rec.getAttribute(COUNTRY_CITY_ATTR).asString(), is("Bangalore"));
          break;

        default:
          fail("Unexpected customer ID");
      }
    }
  }

  private static void preLoadContextEntries() throws BiosClientException {
    var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
    var upsert1 =
        Bios.isql()
            .upsert()
            .intoContext(CUSTOMER_CONTEXT_NAME)
            .csvBulk(CUSTOMER_CONTEXT_DATA)
            .build();
    var upsert2 =
        Bios.isql()
            .upsert()
            .intoContext(COUNTRY_CONTEXT_NAME)
            .csvBulk(COUNTRY_CONTEXT_DATA)
            .build();
    session.execute(upsert1);
    session.execute(upsert2);
  }

  private static void deletePreLoaded() throws BiosClientException {
    var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
    var delete1 =
        Bios.isql()
            .delete()
            .fromContext(CUSTOMER_CONTEXT_NAME)
            .where(keys().in(CUSTOMER_IDS))
            .build();
    var delete2 =
        Bios.isql()
            .delete()
            .fromContext(COUNTRY_CONTEXT_NAME)
            .where(keys().in(COUNTRY_IDS))
            .build();
    session.execute(delete1);
    session.execute(delete2);
  }
}
