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
package io.isima.bios.proto.isql;

import static io.isima.bios.models.isql.Metric.count;
import static io.isima.bios.models.isql.Metric.last;
import static io.isima.bios.models.isql.Metric.max;
import static io.isima.bios.models.isql.Metric.min;
import static io.isima.bios.models.isql.Metric.sum;
import static io.isima.bios.models.isql.OrderBy.by;
import static io.isima.bios.models.isql.Window.sliding;
import static io.isima.bios.models.isql.Window.tumbling;
import static java.time.OffsetDateTime.now;
import static java.time.OffsetDateTime.ofInstant;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.codec.proto.isql.ProtoBuilderProvider;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.isql.ISqlBuilderProvider;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.ISqlTimeHelper;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.WindowType;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test fluency and completeness of the Select Request builders. */
public class QueryBuilderTest {
  private static final String FILTER_CONDITION =
      "country = 'India' AND city IN ('Bangalore', 'Bengaluru')";

  private static ISqlTimeHelper time;

  @BeforeClass
  public static void beforeAll() {
    time = new ISqlTimeHelper();
  }

  @AfterClass
  public static void resetBuilderProvider() {
    ISqlBuilderProvider.reset();
  }

  /** Parameterized constructor to run both proto and json. */
  public QueryBuilderTest() {
    ProtoBuilderProvider.configureProtoBuilderProvider();
  }

  @Test
  public void testBasicSelect() {
    long startTime = time.now();

    final var test =
        ISqlStatement.select().from("ss1").timeRange(startTime, time.millis(-500)).build();

    assertThat(test.getEndTime(), is(startTime));
    assertThat(test.getStartTime(), is((startTime - 500L)));
    assertThat(test.getFrom(), is("ss1"));
    assertNull(test.getSelectAttributes());
    assertFalse(test.hasWindow());
    assertFalse(test.hasOrderBy());
  }

  @Test
  public void testBasicSelectWithAttributes() {
    final var test =
        ISqlStatement.select("a", "b", "c")
            .from("s1")
            .timeRange(time.millis(0), time.millis(200))
            .build();

    assertThat(test.getStartTime(), is(0L));
    assertThat(test.getEndTime(), is(200L));
    assertThat(test.getFrom(), is("s1"));
    assertThat(test.getSelectAttributes(), is(Arrays.asList("a", "b", "c")));
    assertFalse(test.hasWindow());
    assertFalse(test.hasOrderBy());
  }

  @Test
  public void testBasicSelectWithMetrics() {
    final var test =
        ISqlStatement.select(count().as("Count"), sum("a1"), last("status"))
            .from("s2")
            .groupBy("a1")
            .timeRange(time.now(), time.millis(-200))
            .build();

    assertThat(test.getSelectAttributes(), is(Collections.emptyList()));
    assertThat(test.getFrom(), is("s2"));
    assertThat(test.getGroupBy().size(), is(1));
    assertThat(test.getMetrics().size(), is(3));
    assertThat(test.getMetrics().get(0).getOutputAttributeName(), is("Count"));
    assertThat(test.getMetrics().get(1).getOutputAttributeName(), is("sum(a1)"));
    assertThat(test.getMetrics().get(2).getOutputAttributeName(), is("last(status)"));
    assertThat(test.getMetrics().get(0).getFunction(), is(MetricFunction.COUNT));
    assertThat(test.getMetrics().get(1).getFunction(), is(MetricFunction.SUM));
    assertThat(test.getMetrics().get(2).getFunction(), is(MetricFunction.LAST));
  }

  @Test
  public void testBasicSelectWithMixedStringAndMetrics() {
    final var test =
        ISqlStatement.select(
                "a1",
                "a2",
                min("a3").as("Min Value"),
                max("a3").as("Max Value"),
                last("phase").as("lastPhase"))
            .from("s2")
            .groupBy("a3", "a4")
            .timeRange(time.now(), time.days(-1))
            .build();

    assertThat(test.getSelectAttributes(), is(Arrays.asList("a1", "a2")));
    assertThat(test.getFrom(), is("s2"));
    assertThat(test.getGroupBy().size(), is(2));
    assertThat(test.getMetrics().size(), is(3));
    assertThat(test.getMetrics().get(0).getOutputAttributeName(), is("Min Value"));
    assertThat(test.getMetrics().get(1).getOutputAttributeName(), is("Max Value"));
    assertThat(test.getMetrics().get(2).getOutputAttributeName(), is("lastPhase"));
    assertThat(test.getMetrics().get(0).getFunction(), is(MetricFunction.MIN));
    assertThat(test.getMetrics().get(1).getFunction(), is(MetricFunction.MAX));
    assertThat(test.getMetrics().get(2).getFunction(), is(MetricFunction.LAST));
  }

  @Test
  public void testBasicSelectError() {
    try {
      ISqlStatement.select(10).from("s2").timeRange(System.currentTimeMillis(), 10).build();
      fail("Wrong type. Builder should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Unknown object"));
    }
  }

  @Test
  public void testBasicSelectTimeRangeError() {
    try {
      ISqlStatement.select("a1").from("s2").timeRange(0, -201).build();
      fail("Illegal time range. Builder should have failed");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("out of bounds"));
    }
  }

  @Test
  public void testWithFilter() {
    final var test =
        ISqlStatement.select()
            .from("s1")
            .where(FILTER_CONDITION)
            .groupBy("a1")
            .timeRange(time.now(), time.hours(-800L))
            .build();

    assertThat(test.getWhere(), is(FILTER_CONDITION));
    assertFalse(test.hasWindow());
  }

  @Test
  public void testWithNullFilter() {
    try {
      ISqlStatement.select()
          .from("s1")
          .where(null)
          .timeRange(time.now(), time.millis(-800L))
          .build();
      fail("Null Filter. Builder should fail");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("null"));
    }
  }

  @Test
  public void testTimeRangeOutOfBounds() {
    try {
      ISqlStatement.select()
          .from("s1")
          .where("abc = 'def'")
          .timeRange(time.millis(0), time.minutes(-800L))
          .build();
      fail("Null Filter And bad select type. Builder should fail");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("out of bounds"));
    }
  }

  @Test
  public void testWithTumblingWindow() {
    final var test =
        ISqlStatement.select()
            .from("s1")
            .groupBy("a1")
            .window(tumbling(100L, TimeUnit.MILLISECONDS))
            .snappedTimeRange(now(), Duration.ofMinutes(-200))
            .build();

    assertTrue(test.hasWindow());
    assertThat(test.getOneWindow().getWindow(), instanceOf(TumblingWindow.class));
    TumblingWindow window = (TumblingWindow) test.getOneWindow();
    assertThat(window.getWindowType(), is(WindowType.TUMBLING_WINDOW));
    assertThat(window.getWindowSizeMillis(), is(100L));
    assertThat(test.isOnTheFly(), is(false));
  }

  @Test
  public void testWithTumblingWindowOnTheFly() {
    final var test =
        ISqlStatement.select()
            .from("s1")
            .groupBy("a1")
            .window(tumbling(100L, TimeUnit.MILLISECONDS))
            .snappedTimeRange(now(), Duration.ofMinutes(-200))
            .onTheFly()
            .build();

    assertTrue(test.hasWindow());
    assertThat(test.getOneWindow().getWindow(), instanceOf(TumblingWindow.class));
    TumblingWindow window = (TumblingWindow) test.getOneWindow();
    assertThat(window.getWindowType(), is(WindowType.TUMBLING_WINDOW));
    assertThat(window.getWindowSizeMillis(), is(100L));
    assertThat(test.isOnTheFly(), is(true));
  }

  @Test
  public void testWithSlidingWindow() {
    final var test =
        ISqlStatement.select("a1")
            .from("s1")
            .groupBy("a1")
            .window(sliding(100L, TimeUnit.MINUTES, 5))
            .snappedTimeRange(now(), Duration.ofMillis(-500L))
            .build();

    assertTrue(test.hasWindow());
    assertThat(test.getOneWindow().getWindow(), instanceOf(SlidingWindow.class));
    SlidingWindow window = (SlidingWindow) test.getOneWindow();
    assertThat(window.getWindowType(), is(WindowType.SLIDING_WINDOW));
    assertThat(window.getSlideIntervalMillis(), is(60 * 100 * 1000L));
    assertThat(window.getNumberOfWindowSlides(), is(5));
    assertFalse(test.isDistinct());
  }

  @Test
  public void testDistinct() {
    final var test =
        ISqlStatement.select("a1")
            .from("s1")
            .distinct()
            .timeRange(time.now(), time.millis(-200L))
            .build();
    assertTrue(test.isDistinct());
    assertThat(test.getSelectAttributes().size(), is(1));
    assertThat(test.getSelectAttributes().get(0), is("a1"));
  }

  @Test
  public void testOrderByDefault() {
    final var test =
        ISqlStatement.select("a1")
            .from("s1")
            .distinct()
            .orderBy(by("a1"))
            .timeRange(time.now(), time.millis(-200L))
            .build();

    assertTrue(test.isDistinct());
    assertThat(test.getSelectAttributes().size(), is(1));
    assertThat(test.getSelectAttributes().get(0), is("a1"));
    assertTrue(test.hasOrderBy());
    assertFalse(test.getOrderBy().isCaseSensitive());
    assertFalse(test.getOrderBy().isDescending());
  }

  @Test
  public void testOrderByAscending() {
    final var test =
        ISqlStatement.select("a1")
            .from("s1")
            .orderBy(by("a1").asc().caseSensitive())
            .timeRange(time.now(), time.millis(-200L))
            .build();

    assertThat(test.getSelectAttributes().size(), is(1));
    assertThat(test.getSelectAttributes().get(0), is("a1"));
    assertTrue(test.hasOrderBy());
    assertTrue(test.getOrderBy().isCaseSensitive());
    assertFalse(test.getOrderBy().isDescending());
  }

  @Test
  public void testOrderByDescending() {
    final var test =
        ISqlStatement.select("a1")
            .from("s1")
            .orderBy(by("a1").desc())
            .timeRange(time.now(), time.millis(-200L))
            .build();

    assertThat(test.getSelectAttributes().size(), is(1));
    assertThat(test.getSelectAttributes().get(0), is("a1"));
    assertTrue(test.hasOrderBy());
    assertFalse(test.getOrderBy().isCaseSensitive());
    assertTrue(test.getOrderBy().isDescending());
  }

  @Test
  public void testBasicSelectSnapped() {
    final var now = System.currentTimeMillis();
    OffsetDateTime originTime = ofInstant(Instant.ofEpochMilli(now), ZoneOffset.UTC);
    final var test =
        ISqlStatement.select()
            .from("ss1")
            .window(sliding(5, TimeUnit.MINUTES, 5))
            .snappedTimeRange(originTime, Duration.ofMillis(-50))
            .build();

    assertThat(test.getEndTime(), is(now / 300000 * 300000));
    assertThat(test.getStartTime(), is(now / 300000 * 300000 - 50L));
    assertThat(test.getFrom(), is("ss1"));
    assertNull(test.getSelectAttributes());
    assertTrue(test.hasWindow());
    assertFalse(test.hasOrderBy());
  }

  @Test
  public void testBasicSelectSnappedWithSnapStepSize() {
    final var originTime = System.currentTimeMillis();
    final var test =
        ISqlStatement.select()
            .from("ss1")
            .window(sliding(5, TimeUnit.MINUTES, 5))
            .snappedTimeRange(originTime, -3600000, 60000)
            .build();

    assertThat(test.getEndTime(), is(originTime / 60000 * 60000));
    assertThat(test.getStartTime(), is(originTime / 60000 * 60000 - 3600000));
    assertThat(test.getFrom(), is("ss1"));
    assertNull(test.getSelectAttributes());
    assertTrue(test.hasWindow());
    assertFalse(test.hasOrderBy());
  }
}
