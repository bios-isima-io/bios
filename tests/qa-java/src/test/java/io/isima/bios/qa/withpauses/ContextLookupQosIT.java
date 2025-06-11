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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.common.BiosConstants;
import io.isima.bios.models.AppType;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.metrics.Metrics;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
public class ContextLookupQosIT {
  private static final String TENANT_NAME = "biosContextLookupQosJava";
  private static final String CONTEXT_NAME = "lookupQosContext";

  static {
    System.setProperty(Metrics.CLIENT_METRICS_ENABLED, "true");
  }

  private static String host;
  private static int port;

  private Session session;

  private static boolean isMultiAzTest() {
    return "yes".equals(System.getenv("MULTI_AZ_DEPLOYMENT"));
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    assumeTrue(isMultiAzTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Before
  public void open() throws Exception {
    Thread.sleep(10000);
    session =
        Bios.newSession(host)
            .port(port)
            .user("admin@" + TENANT_NAME)
            .password("admin")
            .appName("contextLookupQosTest")
            .appType(AppType.REALTIME)
            .connect();
  }

  @After
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  private void upsertEntries(List<String> entries) throws BiosClientException {
    final var statement = Bios.isql().upsert().intoContext(CONTEXT_NAME).csvBulk(entries).build();
    session.execute(statement);
  }

  @Test
  public void test() throws Exception {
    final long t1 = System.currentTimeMillis();
    upsertEntries(List.of("100,10,rcb", "101,11,vk"));

    final var selectRequests =
        new Statement[] {
          Bios.isql().select().fromContext(CONTEXT_NAME).where(keys().eq(100L)).build(),
          Bios.isql().select().fromContext(CONTEXT_NAME).where(keys().eq(101L)).build()
        };

    long sleepTime = 1000;

    List<ISqlResponse> reply = null;
    long elapsed = 0;
    for (int i = 0; i < 20; ++i) {
      final long start = System.currentTimeMillis();
      reply = session.multiExecute(selectRequests);
      elapsed = System.currentTimeMillis() - start;
      System.out.println("Elapsed = " + elapsed);
      if (elapsed >= 200 && elapsed < 1000) {
        break;
      }
      Thread.sleep(sleepTime);
    }
    System.out.println("multi execute got completed in " + elapsed);

    // context select requests are delayed by 1 second in analysis node.
    // c-sdk should forward this request to another node, and
    // it should complete before analysis node returns.
    assertThat(elapsed, greaterThanOrEqualTo(200L));
    assertThat(elapsed, lessThan(1000L));

    assertThat(reply.size(), is(2));
    assertThat(reply.get(0).getRecords().size(), is(1));
    assertThat(reply.get(1).getRecords().size(), is(1));

    Thread.sleep(60000);

    try (var sadmin =
        session =
            Bios.newSession(host).port(port).user("superadmin").password("superadmin").connect()) {

      selectTest(
          sadmin,
          // query
          () ->
              Bios.isql()
                  .select()
                  .fromSignal(BiosConstants.STREAM_ALL_CLIENT_METRICS)
                  .where(String.format("tenant = '%s'", TENANT_NAME))
                  .timeRange(t1, 60000)
                  .build(),
          // result
          (records) -> {
            long numQosRetryConsidered = 0;
            long numQosRetrySent = 0;
            long numQosRetryResponseUsed = 0;
            for (var record : records) {
              numQosRetryConsidered += record.getAttribute("numQosRetryConsidered").asLong();
              numQosRetrySent += record.getAttribute("numQosRetrySent").asLong();
              numQosRetryResponseUsed += record.getAttribute("numQosRetryResponseUsed").asLong();
            }
            assertThat(numQosRetryConsidered, greaterThan(0L));
            assertThat(numQosRetrySent, greaterThan(0L));
            assertThat(numQosRetryResponseUsed, greaterThan(0L));
          });
    }
  }

  private void selectTest(
      Session extractSession,
      Supplier<Statement> querySupplier,
      Consumer<List<Record>> respConsumer)
      throws Exception {
    final var queryResponse = extractSession.execute(querySupplier.get());
    final var data = queryResponse.getDataWindows();
    assertThat(data.size(), is(1));
    DataWindow dataWindow = data.get(0);
    List<Record> recList = dataWindow.getRecords();
    respConsumer.accept(recList);
  }
}
