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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
public class ContextDataAccessIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  // for create
  private static final String CONTEXT_NAME_1 = "simpleContext";
  // for only updates and get
  private static final String CONTEXT_NAME_2 = "storeContext";

  private static final Integer[] LOAD_CONTEXT_1_KEY = {1, 2, 3, 4, 5, 6};
  private static final List<String> LOAD_CONTEXT_1_DATA =
      Arrays.asList("1,a", "2,b", "3,c", "4,d", "5,e", "6,f");
  private static final String CONTEXT_1_VALUE_ATTR = "the_value";

  private static final Integer[] LOAD_CONTEXT_2_KEY = {1000, 1001, 1002};
  private static final List<String> LOAD_CONTEXT_2_DATA =
      Arrays.asList("1000,600100,Thrissur", "1001,560102,Bengaluru", "1002,560103,Bangalore");
  private static final String CONTEXT_2_KEY_ATTR = "storeId";
  private static final String CONTEXT_2_VALUE_ATTR_1 = "zipCode";
  private static final String CONTEXT_2_VALUE_ATTR_2 = "address";

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
  public void testGetAllKeysFromContext() throws BiosClientException {
    final var stmt = Bios.isql().select(CONTEXT_2_KEY_ATTR).fromContext(CONTEXT_NAME_2).build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(LOAD_CONTEXT_2_KEY.length));
    Set<Long> expectedKeySet =
        Arrays.stream(LOAD_CONTEXT_2_KEY).map(Integer::longValue).collect(Collectors.toSet());
    for (var record : response.getRecords()) {
      assertTrue(expectedKeySet.remove(record.getAttribute(CONTEXT_2_KEY_ATTR).asLong()));
    }
    assertTrue(expectedKeySet.isEmpty());
  }

  @Test
  public void testGetAllKeysCountFromContext() throws BiosClientException {
    final var stmt = Bios.isql().select("count()").fromContext("contextWithFeature").build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(1));
    final var record = response.getRecords().get(0);
    assertThat(record.getAttribute("count()").asLong(), is(7L));
  }

  @Test
  public void testContextFeatureSelectInvalidAttribute() throws BiosClientException {
    final var stmt =
        Bios.isql()
            .select("invalidAttribute")
            .fromContext(CONTEXT_NAME_1)
            .where(keys().in(1, 2))
            .build();
    final var exception =
        assertThrows(
            BiosClientException.class,
            () -> {
              final var response = testSession.execute(stmt);
              System.out.println(response);
            });
    assertThat(exception.getCode(), is(BiosClientError.BAD_INPUT));
  }

  @Test
  public void testContextFeatureSelect() throws BiosClientException {
    final var stmt =
        Bios.isql()
            .select("zipCode", "sum(numProducts)", "count()")
            .fromContext("contextWithFeature")
            .groupBy("zipCode")
            .build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(4));
    for (var record : response.getRecords()) {
      if (record.getAttribute("zipCode").asLong() == 94064) {
        assertThat(record.getAttribute("count()").asLong(), is(2L));
      }
    }
  }

  @Test
  public void testContextSketchSelect() throws BiosClientException {
    final var stmt =
        Bios.isql()
            .select("median(numProducts)", "distinctcount(zipCode)")
            .fromContext("contextWithFeature")
            .build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(1));
    assertThat(
        response.getRecords().get(0).getAttribute("distinctcount(zipCode)").asLong(), is(4L));
  }

  @Test
  public void testContextIndexSelect() throws BiosClientException {
    final var stmt =
        Bios.isql()
            .select("numProducts", "zipCode", "storeId")
            .fromContext("contextWithFeature")
            .where("zipCode = 94061")
            .orderBy("numProducts")
            .build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(3));
    assertThat(response.getRecords().get(0).getAttribute("numProducts").asLong(), is(12L));
    assertThat(response.getRecords().get(1).getAttribute("numProducts").asLong(), is(20L));
    assertThat(response.getRecords().get(1).getAttribute("storeId").asLong(), is(10001L));
  }

  @Test
  public void testContextIndexSelectOrderAndLimit() throws BiosClientException {
    final var stmt =
        Bios.isql()
            .select("numProducts", "zipCode", "storeId")
            .fromContext("contextWithFeature")
            .where("zipCode = 94061")
            .orderBy("numProducts")
            .limit(2)
            .build();
    final var response = testSession.execute(stmt);
    assertThat(response.getRecords().size(), is(2));
    assertThat(response.getRecords().get(0).getAttribute("numProducts").asLong(), is(12L));
    assertThat(response.getRecords().get(1).getAttribute("numProducts").asLong(), is(20L));
    assertThat(response.getRecords().get(1).getAttribute("storeId").asLong(), is(10001L));
  }

  @Test
  public void testUpdateContextEntries() throws BiosClientException {
    Map<String, Object> updateMap = new HashMap<>();
    updateMap.put(CONTEXT_2_VALUE_ATTR_1, 560204);
    updateMap.put(CONTEXT_2_VALUE_ATTR_2, "Bangalore");
    var statement =
        Bios.isql()
            .update(CONTEXT_NAME_2)
            .set(updateMap)
            .where(keys().in(LOAD_CONTEXT_2_KEY[0]))
            .build();

    // multi-execute cannot handle this statement (yet)
    final var exception =
        assertThrows(BiosClientException.class, () -> testSession.multiExecute(statement));
    assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));

    testSession.execute(statement);

    final var contextSelect =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME_2)
            .where(keys().in(LOAD_CONTEXT_2_KEY[0]))
            .build();

    final var resp = testSession.execute(contextSelect);
    assertThat(resp.getRecords().size(), is(1));
    assertThat(
        resp.getRecords().get(0).getAttribute(CONTEXT_2_VALUE_ATTR_1).asString(), is("560204"));
    assertThat(
        resp.getRecords().get(0).getAttribute(CONTEXT_2_VALUE_ATTR_2).asString(), is("Bangalore"));
  }

  @Test
  public void testCreateContextEntries() throws BiosClientException {
    var statement =
        Bios.isql()
            .upsert()
            .intoContext(CONTEXT_NAME_1)
            .csvBulk(Arrays.asList("7,g", "8,h", "9,i", "10,j"))
            .build();

    // multi-execute cannot handle this statement (yet)
    final var exception =
        assertThrows(BiosClientException.class, () -> testSession.multiExecute(statement));
    assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));

    testSession.execute(statement);

    final var contextSelect =
        Bios.isql().select().fromContext(CONTEXT_NAME_1).where(keys().in(7, 8, 9, 10)).build();

    final var resp = testSession.execute(contextSelect);
    assertThat(resp.getRecords().size(), is(4));
    assertThat(resp.getRecords().get(0).getAttribute(CONTEXT_1_VALUE_ATTR).asString(), is("g"));
    assertThat(resp.getRecords().get(3).getAttribute(CONTEXT_1_VALUE_ATTR).asString(), is("j"));

    final var contextSelect2 =
        Bios.isql()
            .select()
            .fromContext(CONTEXT_NAME_1)
            .where(keys().in(List.of(7L, 8L, 9L, 10L)))
            .build();

    final var resp2 = testSession.execute(contextSelect2);
    assertThat(resp2.getRecords().size(), is(4));
    assertThat(resp2.getRecords().get(0).getAttribute(CONTEXT_1_VALUE_ATTR).asString(), is("g"));
    assertThat(resp2.getRecords().get(3).getAttribute(CONTEXT_1_VALUE_ATTR).asString(), is("j"));
  }

  @Test
  public void testDeleteContextEntriesByMultiExecute() throws BiosClientException {
    var statement =
        Bios.isql().delete().fromContext(CONTEXT_NAME_1).where(keys().in(7L, 8L)).build();
    final var exception =
        assertThrows(BiosClientException.class, () -> testSession.multiExecute(statement));
    assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
  }

  @Test
  public void testEmptyMultiExecute() throws BiosClientException {
    final var exception = assertThrows(BiosClientException.class, () -> testSession.multiExecute());
    assertThat(exception.getCode(), is(BiosClientError.INVALID_REQUEST));
  }

  private static void preLoadContextEntries() throws BiosClientException {
    var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
    var upsert1 =
        Bios.isql().upsert().intoContext(CONTEXT_NAME_1).csvBulk(LOAD_CONTEXT_1_DATA).build();
    var upsert2 =
        Bios.isql().upsert().intoContext(CONTEXT_NAME_2).csvBulk(LOAD_CONTEXT_2_DATA).build();
    session.execute(upsert1);
    session.execute(upsert2);
  }

  private static void deletePreLoaded() throws BiosClientException {
    var session =
        Bios.newSession(host).port(port).user("admin@" + TENANT_NAME).password("admin").connect();
    var delete1 =
        Bios.isql()
            .delete()
            .fromContext(CONTEXT_NAME_1)
            .where(keys().in(LOAD_CONTEXT_1_KEY))
            .build();
    var delete2 =
        Bios.isql()
            .delete()
            .fromContext(CONTEXT_NAME_2)
            .where(keys().in(LOAD_CONTEXT_2_KEY))
            .build();
    session.execute(delete1);
    session.execute(delete2);
  }

  @Test
  public void testMultiContextSelect() throws BiosClientException {
    final var random = new Random();
    final var keysStore = new ArrayList<>();
    try {
      // Populate to context 1
      final int numEntriesSimple = 4;
      final long offsetSimple = random.nextLong();
      {
        final var simpleEvents = new ArrayList<String>();
        for (int i = 0; i < numEntriesSimple; ++i) {
          simpleEvents.add(String.format("%d,%d", i + offsetSimple, i));
        }
        var statementSimple =
            Bios.isql().upsert().intoContext(CONTEXT_NAME_1).csvBulk(simpleEvents).build();
        testSession.execute(statementSimple);
      }

      // Populate to context 2
      final int numEntriesStore = 5;
      final long offsetStore = random.nextLong();
      {
        final var storeEvents = new ArrayList<String>();
        for (int i = 0; i < numEntriesStore; ++i) {
          storeEvents.add(String.format("%d,%d,%d", i + offsetStore, i * 10, i));
        }
        var statementStore =
            Bios.isql().upsert().intoContext(CONTEXT_NAME_2).csvBulk(storeEvents).build();
        testSession.execute(statementStore);
      }

      // Try multi execute
      final Statement contextSelectSimple;
      {
        final var keysSimple = new ArrayList<>();
        for (int i = 0; i < numEntriesSimple; ++i) {
          keysSimple.add(i + offsetSimple);
        }
        contextSelectSimple =
            Bios.isql().select().fromContext(CONTEXT_NAME_1).where(keys().in(keysSimple)).build();
      }
      final Statement contextSelectStore;
      {
        for (int i = 0; i < numEntriesStore; ++i) {
          keysStore.add(i + offsetStore);
        }
        contextSelectStore =
            Bios.isql().select().fromContext(CONTEXT_NAME_2).where(keys().in(keysStore)).build();
      }

      // Run multiple statements with multiExecute
      {
        final var resp = testSession.multiExecute(contextSelectSimple, contextSelectStore);
        assertThat(resp.size(), is(2));
        final var recordsSimple = resp.get(0).getRecords();
        assertThat(recordsSimple.size(), is(numEntriesSimple));
        for (int i = 0; i < numEntriesSimple; ++i) {
          assertThat(
              recordsSimple.get(i).getAttribute("the_value").asString(), is(Integer.toString(i)));
        }

        final var recordsStore = resp.get(1).getRecords();
        assertThat(recordsStore.size(), is(numEntriesStore));
        for (int i = 0; i < numEntriesStore; ++i) {
          final var record = recordsStore.get(i);
          assertThat(record.getAttribute("zipCode").asLong(), is(Long.valueOf(i * 10)));
          assertThat(record.getAttribute("address").asString(), is(Integer.toString(i)));
        }
      }
      // Run single statement with multiExecute
      {
        final var resp = testSession.multiExecute(contextSelectSimple);
        assertThat(resp.size(), is(1));
        final var recordsSimple = resp.get(0).getRecords();
        assertThat(recordsSimple.size(), is(numEntriesSimple));
        for (int i = 0; i < numEntriesSimple; ++i) {
          assertThat(
              recordsSimple.get(i).getAttribute("the_value").asString(), is(Integer.toString(i)));
        }
      }
    } finally {
      if (!keysStore.isEmpty()) {
        testSession.execute(
            Bios.isql().delete().fromContext(CONTEXT_NAME_2).where(keys().in(keysStore)).build());
      }
    }
  }

  @Test
  public void selectAllShouldBeRejected() throws Exception {
    final var statement = Bios.isql().select().fromContext(CONTEXT_NAME_1).build();
    final var exception =
        assertThrows(
            BiosClientException.class,
            () -> {
              final var response = testSession.execute(statement);
              System.out.println(response);
            });
    assertThat(exception.getCode(), is(BiosClientError.BAD_INPUT));
  }
}
