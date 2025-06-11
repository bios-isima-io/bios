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
package io.isima.bios.data;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.impl.SystemAdminConfig;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.QueryType;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.Sort;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.isql.WindowType;
import io.isima.bios.models.v1.Group;
import io.isima.bios.stats.ClockProvider;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryLoggerTest {

  private static AdminInternal admin;
  private static StreamDesc queryStream;
  private static QueryLogger queryLogger;
  private static Clock clock;

  @BeforeClass
  public static void setUpClass() throws Exception {
    admin = new AdminImpl(null, null);
    final var systemAdminConfig = new SystemAdminConfig();
    for (var tenantConfig : systemAdminConfig.getTenantConfigList()) {
      if (tenantConfig.getName().equals(BiosConstants.TENANT_SYSTEM)) {
        final long timestamp = System.currentTimeMillis();
        admin.addTenant(tenantConfig, RequestPhase.INITIAL, timestamp);
        admin.addTenant(tenantConfig, RequestPhase.FINAL, timestamp);
        queryStream = admin.getStream(tenantConfig.getName(), BiosConstants.QUERY_SIGNAL);
        break;
      }
    }
    queryLogger = new QueryLogger(null, admin);
    clock = ClockProvider.getClock();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testExtractSimple() throws InterruptedException {
    final var state = new ExtractState(null, "testTenant", "testStream", null, null);
    final var request = new ExtractRequest();
    request.setStartTime(123L);
    request.setEndTime(456L);
    state.setInput(request);
    final QueryLogger.Item item = queryLogger.newItem(state);
    final Event event = item.toEvent();

    // Check the initial state
    verifyItemParameters(event);
    assertThat(event.get("tenant"), is("testTenant"));
    assertThat(event.get("queryType"), is(QueryType.EXTRACT.ordinal()));
    assertThat(event.get("select"), is(""));
    assertThat(event.get("from"), is("testStream"));
    assertThat(event.get("where"), is(""));
    assertThat(event.get("groupBy"), is(""));
    assertThat(event.get("orderBy"), is(""));
    assertThat(event.get("limit"), is(0L));
    assertThat(event.get("windowType"), is(WindowType.GLOBAL_WINDOW.ordinal()));
    assertThat(event.get("startTime"), is(123L));
    assertThat(event.get("endTime"), is(456L));
    assertThat(event.get("interval"), is(0L));
    assertThat(event.get("timezoneOffset"), is(0L));

    // Simulate the query process
    final var start = clock.instant();
    item.setRequestReceivedTime(start);
    final int numDbQueries = 13;
    for (int i = 0; i < numDbQueries; ++i) {
      item.startDb(i);
      Thread.sleep(10);
      item.endDb(i, i * 10);
    }
    Thread.sleep(10);
    item.addPostDb(10000, 2, 100);
    final var end = clock.instant();
    item.responseSent("Ok", end);

    // Check the final state
    verifyItemParameters(event);
    assertThat(event.get("requestReceivedTime"), is(start.toEpochMilli()));
    assertThat((Long) event.get("responseSentTime"), greaterThan(start.toEpochMilli()));
    assertThat((Long) event.get("queryLatency"), is(Duration.between(start, end).toNanos() / 1000));
    assertThat((Long) event.get("numDbQueries"), is(Long.valueOf(numDbQueries)));
    assertThat(
        (Long) event.get("numDbRowsRead"), is((long) ((numDbQueries - 1) * 5) * numDbQueries));
    assertThat((Long) event.get("dbQueryAvgLatency"), greaterThan(0L));
    assertThat((Long) event.get("dbTotalLatency"), greaterThan(0L));
    assertThat((Long) event.get("postDbLatency"), greaterThan(0L));
    assertThat((Long) event.get("numRecordsReturned"), is(100L));
    assertThat((Long) event.get("numTimeWindowsReturned"), is(2L));
  }

  @Test
  public void testExtractGrouping() throws InterruptedException {
    final var state = new ExtractState(null, "testTenant", "testStream", null, null);
    final var request = new ExtractRequest();
    request.setStartTime(123L);
    request.setEndTime(456L);
    request.setAggregates(List.of(new Sum("sales")));
    request.setViews(List.of(new Group("dimension1"), new Sort("month")));
    state.setInput(request);
    final QueryLogger.Item item = queryLogger.newItem(state);
    final Event event = item.toEvent();

    // Check the initial state
    verifyItemParameters(event);
    assertThat(event.get("tenant"), is("testTenant"));
    assertThat(event.get("queryType"), is(QueryType.EXTRACT.ordinal()));
    assertThat(event.get("select"), is("[sum(sales)]"));
    assertThat(event.get("from"), is("testStream"));
    assertThat(event.get("where"), is(""));
    assertThat(event.get("groupBy"), is("[dimension1]"));
    assertThat(event.get("orderBy"), is("month"));
    assertThat(event.get("limit"), is(0L));
    assertThat(event.get("windowType"), is(WindowType.GLOBAL_WINDOW.ordinal()));
    assertThat(event.get("startTime"), is(123L));
    assertThat(event.get("endTime"), is(456L));
    assertThat(event.get("interval"), is(0L));
    assertThat(event.get("timezoneOffset"), is(0L));

    // Simulate the query process
    final var start = clock.instant();
    item.setRequestReceivedTime(start);
    final int numDbQueries = 13;
    for (int i = 0; i < numDbQueries; ++i) {
      item.startDb(i);
      Thread.sleep(10);
      item.endDb(i, i * 10);
    }
    Thread.sleep(10);
    item.addPostDb(10000, 2, 100);
    final var end = clock.instant();
    item.responseSent("Ok", end);

    // Check the final state
    verifyItemParameters(event);
    assertThat(event.get("requestReceivedTime"), is(start.toEpochMilli()));
    assertThat((Long) event.get("responseSentTime"), greaterThan(start.toEpochMilli()));
    assertThat((Long) event.get("queryLatency"), is(Duration.between(start, end).toNanos() / 1000));
    assertThat((Long) event.get("numDbQueries"), is(Long.valueOf(numDbQueries)));
    assertThat(
        (Long) event.get("numDbRowsRead"), is((long) ((numDbQueries - 1) * 5) * numDbQueries));
    assertThat((Long) event.get("dbQueryAvgLatency"), greaterThan(0L));
    assertThat((Long) event.get("dbTotalLatency"), greaterThan(0L));
    assertThat((Long) event.get("postDbLatency"), greaterThan(0L));
    assertThat((Long) event.get("numRecordsReturned"), is(100L));
    assertThat((Long) event.get("numTimeWindowsReturned"), is(2L));
  }

  @Test
  public void testSummarizeTumblingWindow() throws InterruptedException {
    final var state = new SummarizeState(null, "testTenant", "testStream", null);
    final var request = new SummarizeRequest();
    request.setStartTime(123L);
    request.setEndTime(456L);
    request.setInterval(300000L);
    request.setHorizon(300000L);
    request.setAggregates(List.of(new Count()));
    request.setGroup(List.of("dimension1"));
    request.setLimit(100);
    state.setInput(request);
    final QueryLogger.Item item = queryLogger.newItem(state);
    final Event event = item.toEvent();

    // Check the initial state
    verifyItemParameters(event);
    assertThat(event.get("tenant"), is("testTenant"));
    assertThat(event.get("queryType"), is(QueryType.SUMMARIZE.ordinal()));
    assertThat(event.get("select"), is("[count()]"));
    assertThat(event.get("from"), is("testStream"));
    assertThat(event.get("where"), is(""));
    assertThat(event.get("groupBy"), is("[dimension1]"));
    assertThat(event.get("orderBy"), is(""));
    assertThat(event.get("limit"), is(100L));
    assertThat(event.get("windowType"), is(WindowType.TUMBLING_WINDOW.ordinal()));
    assertThat(event.get("startTime"), is(123L));
    assertThat(event.get("endTime"), is(456L));
    assertThat(event.get("interval"), is(300000L));
    assertThat(event.get("timezoneOffset"), is(0L));

    // Simulate the query process
    final var start = clock.instant();
    item.setRequestReceivedTime(start);
    final int numDbQueries = 13;
    for (int i = 0; i < numDbQueries; ++i) {
      item.startDb(i);
      Thread.sleep(10);
      item.endDb(i, i * 10);
    }
    Thread.sleep(10);
    item.addPostDb(10000, 2, 100);
    final var end = clock.instant();
    item.responseSent("Ok", end);

    // Check the final state
    verifyItemParameters(event);
    assertThat(event.get("requestReceivedTime"), is(start.toEpochMilli()));
    assertThat((Long) event.get("responseSentTime"), greaterThan(start.toEpochMilli()));
    assertThat((Long) event.get("queryLatency"), is(Duration.between(start, end).toNanos() / 1000));
    assertThat((Long) event.get("numDbQueries"), is(Long.valueOf(numDbQueries)));
    assertThat(
        (Long) event.get("numDbRowsRead"), is((long) ((numDbQueries - 1) * 5) * numDbQueries));
    assertThat((Long) event.get("dbQueryAvgLatency"), greaterThan(0L));
    assertThat((Long) event.get("dbTotalLatency"), greaterThan(0L));
    assertThat((Long) event.get("postDbLatency"), greaterThan(0L));
    assertThat((Long) event.get("numRecordsReturned"), is(100L));
    assertThat((Long) event.get("numTimeWindowsReturned"), is(2L));
  }

  @Test
  public void testSummarizeSlidingWindow() {
    final var state = new SummarizeState(null, "testTenant", "testStream", null);
    final var summarizeRequest = new SummarizeRequest();
    summarizeRequest.setStartTime(123L);
    summarizeRequest.setEndTime(456L);
    summarizeRequest.setInterval(300000L);
    summarizeRequest.setHorizon(600000L);
    summarizeRequest.setAggregates(List.of(new Count()));
    summarizeRequest.setGroup(List.of("city"));
    summarizeRequest.setSort(new Sort("count()"));
    state.setInput(summarizeRequest);
    final QueryLogger.Item item = queryLogger.newItem(state);
    final Event event = item.toEvent();
    verifyItemParameters(event);

    assertThat(event.get("tenant"), is("testTenant"));
    assertThat(event.get("queryType"), is(QueryType.SUMMARIZE.ordinal()));
    assertThat(event.get("select"), is("[count()]"));
    assertThat(event.get("from"), is("testStream"));
    assertThat(event.get("where"), is(""));
    assertThat(event.get("groupBy"), is("[city]"));
    assertThat(event.get("orderBy"), is("count()"));
    assertThat(event.get("limit"), is(0L));
    assertThat(event.get("windowType"), is(WindowType.SLIDING_WINDOW.ordinal()));
    assertThat(event.get("startTime"), is(123L));
    assertThat(event.get("endTime"), is(456L));
    assertThat(event.get("interval"), is(300000L));
    assertThat(event.get("timezoneOffset"), is(0L));
  }

  private void verifyItemParameters(Event event) {
    for (var attributeDesc : queryStream.getAttributes()) {
      final var name = attributeDesc.getName();
      final var value = event.get(name);
      final var info = String.format("attribute=%s, value=%s (%s)", name, value, event);
      switch (attributeDesc.getAttributeType()) {
        case STRING:
          assertThat(info, value, instanceOf(String.class));
          break;
        case LONG:
          assertThat(info, value, instanceOf(Long.class));
          break;
        case BOOLEAN:
          assertThat(info, value, instanceOf(Boolean.class));
          break;
        case ENUM:
          assertThat(info, value, instanceOf(Integer.class));
          break;
        case BLOB:
          assertThat(info, value, instanceOf(ByteBuffer.class));
          break;
        default:
          fail("Implement this type: " + attributeDesc.getAttributeType());
      }
    }
  }
}
