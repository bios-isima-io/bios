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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.qautils.TestUtils;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.metrics.Metrics;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.time.Duration;
import org.junit.BeforeClass;
import org.junit.Test;

public class SummarizeIT {
  private static final String TENANT_NAME = "biosClientE2eTest";
  private static final String SIGNAL_NAME = "rollupTwoValues";

  static {
    System.setProperty(Metrics.CLIENT_METRICS_ENABLED, "true");
  }

  private static String host;
  private static int port;
  private static long startingWindow;
  private static long startTime;
  private static long originTime;
  private static long secondInsertTime;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
    // TODO: Move the test initialization to the setup script so that the test can re-run easily
    Session.Starter starter =
        Bios.newSession(host).port(port).user("ingest@" + TENANT_NAME).password("ingest");
    try (final Session session = starter.connect()) {
      // Using 60 second intervals for some of the queries here. To ensure we get events ingested
      // in exactly successive time windows, sleep until 2 seconds after the next window,
      // and then do the ingestion.
      long currentTime = System.currentTimeMillis();
      startingWindow = Utils.ceiling(currentTime, 60000);
      startTime = startingWindow + 2000;
      TestUtils.sleepUntil(startTime);
      final var statement =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  "US,California,3,52.4",
                  "Japan,Tokyo,7,12.6",
                  "US,Utah,33,71.1",
                  "Japan,Osaka,11,8.4",
                  "US,California,22,31.6",
                  "US,Oregon,5,83.3",
                  "US,Utah,91,12.4",
                  "Australia,Queensland,10,4.2",
                  "India,Goa,17,1.2",
                  "US,California,1,111.2")
              .build();
      final var response = session.execute(statement);
      originTime = response.getRecords().get(0).getTimestamp();
      TestUtils.sleepUntil(startTime + 60000);
      final var statement2 =
          Bios.isql()
              .insert()
              .into(SIGNAL_NAME)
              .csvBulk(
                  "Australia,New South Wales,51,38.2",
                  "Japan,Hokkaido,73,12.6",
                  "Australia,Queensland,11,18.1",
                  "US,California,95,88.0",
                  "Japan,Hokkaido,5,191.4",
                  "Australia,Queensland,14,11.2",
                  "US,California,10,70.9",
                  "US,Utah,10,11.3",
                  "Australia,Queensland,17,15.4")
              .build();
      final var response2 = session.execute(statement2);
      secondInsertTime = response2.getRecords().get(0).getTimestamp();
      TestUtils.sleepUntil(secondInsertTime + 190000);
    }
  }

  @Test
  public void testBundleTwoRollups() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session session = starter.connect()) {
      int credit = 1;
      ISqlResponse reply = null;
      do {
        final var statement =
            Bios.isql()
                .select("count()")
                .fromSignal(SIGNAL_NAME)
                .tumblingWindow(Duration.ofMinutes(3))
                .timeRange(originTime, Duration.ofMinutes(3), Duration.ofMinutes(1))
                .build();
        reply = session.execute(statement);
        System.out.printf(
            "startTime=%s, insert1=%s, insert2=%s, responseTime=%s, numWindows=%d%n",
            StringUtils.tsToIso8601Millis(startTime),
            StringUtils.tsToIso8601Millis(originTime),
            StringUtils.tsToIso8601Millis(secondInsertTime),
            reply.getDataWindows().isEmpty()
                ? "n/a"
                : StringUtils.tsToIso8601Millis(reply.getDataWindows().get(0).getWindowBeginTime()),
            reply.getDataWindows().size());
        if (reply.getDataWindows().size() > 0 || credit == 0) {
          break;
        }
        System.out.println("Empty response, will retry");
        TestUtils.sleepAtLeast(10000);
      } while (credit-- > 0);
      assertEquals(1, reply.getDataWindows().size());
      assertEquals(1, reply.getDataWindows().get(0).getRecords().size());
      if ((secondInsertTime - startingWindow) < 3 * 60000) {
        assertEquals(
            19L,
            reply.getDataWindows().get(0).getRecords().get(0).getAttribute("count()").asLong());
      } else {
        assertEquals(
            10L,
            reply.getDataWindows().get(0).getRecords().get(0).getAttribute("count()").asLong());
      }
    }
  }

  @Test
  public void testOrderBy() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session session = starter.connect()) {
      final var statement =
          Bios.isql()
              .select("state", "sum(score)")
              .fromSignal(SIGNAL_NAME)
              .groupBy("state")
              .orderBy("state")
              .tumblingWindow(Duration.ofMinutes(1))
              .timeRange(originTime, Duration.ofMinutes(1))
              .build();
      final var reply = session.execute(statement);
      assertEquals(1, reply.getDataWindows().size());
      assertTrue(reply.getDataWindows().get(0).getRecords().size() >= 7);
    }
  }

  @Test
  public void testOnTheFly() throws Exception {
    Session.Starter starter =
        Bios.newSession(host).port(port).user("extract@" + TENANT_NAME).password("extract");
    try (final Session session = starter.connect()) {
      final var statement =
          Bios.isql()
              .select("count()")
              .fromSignal(SIGNAL_NAME)
              .tumblingWindow(Duration.ofMinutes(3))
              .timeRange(originTime, Duration.ofMinutes(3), Duration.ofMinutes(1))
              .onTheFly()
              .build();
      final var reply = session.execute(statement);
      assertEquals(1, reply.getDataWindows().size());
      assertEquals(1, reply.getDataWindows().get(0).getRecords().size());
      if ((secondInsertTime - startingWindow) < 3 * 60000) {
        assertEquals(
            19L,
            reply.getDataWindows().get(0).getRecords().get(0).getAttribute("count()").asLong());
      } else {
        assertEquals(
            10L,
            reply.getDataWindows().get(0).getRecords().get(0).getAttribute("count()").asLong());
      }
    }
  }
}
