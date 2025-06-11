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
package io.isima.bios.data.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.maintenance.DataEngineMaintenance;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.it.tools.TestUtils;
import io.isima.bios.models.Count;
import io.isima.bios.models.Event;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Class to keep common part of the Summarize API test cases. */
public class SummarizeTestCore {

  private DataEngine dataEngine;

  private SummarizeTestSetup setup;
  private SummarizeTestSetup.Params params;
  private long firstIngest;
  private long basicInterval;
  private StreamDesc streamDesc;

  /**
   * The constructor is meant be called by setUpClass of the parant test case.
   *
   * @param clazz Parent test case class name
   * @param src Source string of the stream to be tested
   * @throws Throwable for any exceptions
   */
  public SummarizeTestCore(Class<?> clazz, String src) throws Throwable {
    Bios2TestModules.startModules(
        true,
        clazz,
        Map.of(
            DataEngineMaintenance.FEATURE_WORKER_INTERVAL_KEY,
            "5000",
            "prop.rollupDebugSignal",
            "animal_sightings"));
    dataEngine = BiosModules.getDataEngine();
    final String testTenant = clazz.getSimpleName();
    final String signalName = "animal_sightings";
    setup = new SummarizeTestSetup(testTenant);
    params = setup.setUp(src, signalName);
    firstIngest = params.firstIngest;
    basicInterval = params.basicInterval;
    streamDesc = params.streamDesc;
  }

  /** This method is meant to be called by tearDownClass of the parent test case. */
  public void tearDownAfterClass() throws Exception {
    setup.tearDown();
    Bios2TestModules.shutdown();
  }

  public SummarizeTestSetup.Params getParams() {
    return params;
  }

  /** 5th step: summarize the two chunks again after sleeping for a while. */
  public void testBasic() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(firstIngest);
    request.setEndTime(firstIngest + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

    Map<Long, List<Event>> out5th = TestUtils.executeSummarize(dataEngine, streamDesc, request);

    final long start = request.getStartTime();
    final long interval = request.getInterval();

    final List<Event> out = params.out2nd.get(Long.valueOf(params.request2nd.getStartTime()));

    assertEquals(2, out5th.size());
    final List<Event> out4 = out5th.get(Long.valueOf(start));
    assertNotNull(out4);
    List<Event> out5 = out5th.get(Long.valueOf(start + interval));
    assertNotNull(out5);

    // verify the first chunk
    assertEquals(4, out4.size());
    for (int i = 0; i < out4.size(); ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out4.get(i);
      assertEquals("index=" + i, event1.getAttributes(), event2.getAttributes());
      assertEquals("index=" + i, event1.getIngestTimestamp(), event2.getIngestTimestamp());
    }

    // verify the second chunk
    assertEquals(4, out5.size());

    assertEquals("Alameda", out5.get(0).get("county"));
    assertEquals(3L, out5.get(0).get("count()"));
    assertEquals(4L, out5.get(0).get("sum(number)"));
    assertEquals(50L, out5.get(0).get("min(distance)"));
    assertEquals(80L, out5.get(0).get("max(distance)"));

    assertEquals("Marin", out5.get(1).get("county"));
    assertEquals(2L, out5.get(1).get("count()"));
    assertEquals(6L, out5.get(1).get("sum(number)"));
    assertEquals(10L, out5.get(1).get("min(distance)"));
    assertEquals(40L, out5.get(1).get("max(distance)"));

    assertEquals("San Mateo", out5.get(2).get("county"));
    assertEquals(3L, out5.get(2).get("count()"));
    assertEquals(10L, out5.get(2).get("sum(number)"));
    assertEquals(5L, out5.get(2).get("min(distance)"));
    assertEquals(90L, out5.get(2).get("max(distance)"));

    assertEquals("Santa Clara", out5.get(3).get("county"));
    assertEquals(1L, out5.get(3).get("count()"));
    assertEquals(3L, out5.get(3).get("sum(number)"));
    assertEquals(90L, out5.get(3).get("min(distance)"));
    assertEquals(90L, out5.get(3).get("max(distance)"));
  }

  /** 6th step: summarize with longer horizon. */
  public void testLongerHorizon() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(firstIngest);
    request.setEndTime(firstIngest + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setHorizon(basicInterval * 2);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

    Map<Long, List<Event>> out6th = TestUtils.executeSummarize(dataEngine, streamDesc, request);
    // verify the results: 6th step -- summarize two event clusters with explict aggregate output
    // names
    final long start = request.getStartTime();
    final long interval = request.getInterval();
    assertEquals(2, out6th.size());
    final List<Event> out4 = out6th.get(Long.valueOf(start));
    assertNotNull(out4);
    // System.out.println("-----");
    // out4.forEach(entry -> System.out.println("entry=" + entry));
    List<Event> out6 = out6th.get(Long.valueOf(start + interval));
    assertNotNull(out6);
    // System.out.println(">>");
    // out6.forEach(entry -> System.out.println("entry=" + entry));

    assertEquals(4, out4.size());

    final List<Event> out = params.out2nd.get(Long.valueOf(firstIngest));
    for (int i = 0; i < out4.size(); ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out4.get(i);
      assertEquals("index=" + i, event1.getAttributes(), event2.getAttributes());
      assertEquals("index=" + i, event1.getIngestTimestamp(), event2.getIngestTimestamp());
    }

    assertEquals(4, out6.size());

    assertEquals("Alameda", out6.get(0).get("county"));
    assertEquals(5L, out6.get(0).get("count()"));
    assertEquals(11L, out6.get(0).get("sum(number)"));
    assertEquals(15L, out6.get(0).get("min(distance)"));
    assertEquals(80L, out6.get(0).get("max(distance)"));

    assertEquals("Marin", out6.get(1).get("county"));
    assertEquals(4L, out6.get(1).get("count()"));
    assertEquals(27L, out6.get(1).get("sum(number)"));
    assertEquals(10L, out6.get(1).get("min(distance)"));
    assertEquals(40L, out6.get(1).get("max(distance)"));

    assertEquals("San Mateo", out6.get(2).get("county"));
    assertEquals(6L, out6.get(2).get("count()"));
    assertEquals(23L, out6.get(2).get("sum(number)"));
    assertEquals(5L, out6.get(2).get("min(distance)"));
    assertEquals(90L, out6.get(2).get("max(distance)"));

    assertEquals("Santa Clara", out6.get(3).get("county"));
    assertEquals(2L, out6.get(3).get("count()"));
    assertEquals(7L, out6.get(3).get("sum(number)"));
    assertEquals(20L, out6.get(3).get("min(distance)"));
    assertEquals(90L, out6.get(3).get("max(distance)"));
  }

  /** 7th step: summarize with wider time range than available. */
  public void testWiderTimeRange() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long requestStart = firstIngest;
    request.setStartTime(requestStart - basicInterval);
    request.setEndTime(requestStart + basicInterval * 2 + basicInterval);
    request.setInterval(basicInterval);
    request.setHorizon(basicInterval);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

    final Map<Long, List<Event>> out7th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 7th step -- summarize with wider time range.
    // The result should be identical to the 5th step but there are two more
    // empty checkpoints
    final long start = request.getStartTime();
    final long interval = request.getInterval();
    assertEquals(4, out7th.size());
    final List<Event> out4 = out7th.get(Long.valueOf(start + interval));
    assertNotNull(out4);
    final List<Event> out6 = out7th.get(Long.valueOf(start + interval * 2));
    assertNotNull(out6);
    assertEquals(4, out6.size());

    // checkpoints that are out of range should be empty
    final List<Event> out7 = out7th.get(Long.valueOf(start));
    assertNotNull(out7);
    assertEquals(0, out7.size());

    final List<Event> out8 = out7th.get(Long.valueOf(start + interval * 3));
    assertNotNull(out8);
    assertEquals(0, out8.size());

    // verify the first chunk
    assertEquals(4, out4.size());
    final List<Event> out = params.out2nd.get(Long.valueOf(firstIngest));
    for (int i = 0; i < out4.size(); ++i) {
      final Event event1 = out.get(i);
      final Event event2 = out4.get(i);
      assertEquals("index=" + i, event1.getAttributes(), event2.getAttributes());
      assertEquals("index=" + i, event1.getIngestTimestamp(), event2.getIngestTimestamp());
    }

    // verify the second chunk
    assertEquals("Alameda", out6.get(0).get("county"));
    assertEquals(3L, out6.get(0).get("count()"));
    assertEquals(4L, out6.get(0).get("sum(number)"));
    assertEquals(50L, out6.get(0).get("min(distance)"));
    assertEquals(80L, out6.get(0).get("max(distance)"));

    assertEquals("Marin", out6.get(1).get("county"));
    assertEquals(2L, out6.get(1).get("count()"));
    assertEquals(6L, out6.get(1).get("sum(number)"));
    assertEquals(10L, out6.get(1).get("min(distance)"));
    assertEquals(40L, out6.get(1).get("max(distance)"));

    assertEquals("San Mateo", out6.get(2).get("county"));
    assertEquals(3L, out6.get(2).get("count()"));
    assertEquals(10L, out6.get(2).get("sum(number)"));
    assertEquals(5L, out6.get(2).get("min(distance)"));
    assertEquals(90L, out6.get(2).get("max(distance)"));

    assertEquals("Santa Clara", out6.get(3).get("county"));
    assertEquals(1L, out6.get(3).get("count()"));
    assertEquals(3L, out6.get(3).get("sum(number)"));
    assertEquals(90L, out6.get(3).get("min(distance)"));
    assertEquals(90L, out6.get(3).get("max(distance)"));
  }

  /** 8th step: summarize with wider intervals. */
  public void testWiderIntervals() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long requestStart = firstIngest;
    request.setStartTime(requestStart - basicInterval * 2);
    request.setEndTime(requestStart + basicInterval * 2 + basicInterval * 2);
    request.setInterval(basicInterval * 2);
    request.setHorizon(basicInterval * 2);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));

    final Map<Long, List<Event>> out8th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 8th step -- summarize with wider intervals
    {
      assertEquals(3, out8th.size());
      final long start = request.getStartTime();
      final long interval = request.getInterval();
      final List<Event> out7 = out8th.get(Long.valueOf(start));
      assertNotNull(out7);
      assertEquals(0, out7.size());

      // verify first checkpoint
      final List<Event> out8 = out8th.get(Long.valueOf(start + interval));
      assertNotNull(out8);
      // System.out.println("-----");
      // out8.forEach(entry -> System.out.println("entry=" + entry));
      assertEquals(4, out8.size());

      assertEquals("Alameda", out8.get(0).get("county"));
      assertEquals(5L, out8.get(0).get("count()"));
      assertEquals(11L, out8.get(0).get("sum(number)"));
      assertEquals(15L, out8.get(0).get("min(distance)"));
      assertEquals(80L, out8.get(0).get("max(distance)"));

      assertEquals("Marin", out8.get(1).get("county"));
      assertEquals(4L, out8.get(1).get("count()"));
      assertEquals(27L, out8.get(1).get("sum(number)"));
      assertEquals(10L, out8.get(1).get("min(distance)"));
      assertEquals(40L, out8.get(1).get("max(distance)"));

      assertEquals("San Mateo", out8.get(2).get("county"));
      assertEquals(6L, out8.get(2).get("count()"));
      assertEquals(23L, out8.get(2).get("sum(number)"));
      assertEquals(5L, out8.get(2).get("min(distance)"));
      assertEquals(90L, out8.get(2).get("max(distance)"));

      assertEquals("Santa Clara", out8.get(3).get("county"));
      assertEquals(2L, out8.get(3).get("count()"));
      assertEquals(7L, out8.get(3).get("sum(number)"));
      assertEquals(20L, out8.get(3).get("min(distance)"));
      assertEquals(90L, out8.get(3).get("max(distance)"));

      // verify second checkpoint
      final List<Event> out9 = out8th.get(Long.valueOf(start + interval * 2));
      assertNotNull(out9);
      // System.out.println(">>>>>");
      // out9.forEach(entry -> System.out.println("entry=" + entry));
      assertEquals(0, out9.size());
    }
  }

  /** 9th step: summarize with a filter. */
  public void testSimpleFilter() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = firstIngest;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("county = 'Alameda'");

    final Map<Long, List<Event>> out9th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 9th step -- summarize with filter by county = 'Alameda'.
    // The result should be equal to the part of 5th step where the county is 'Alameda'.
    final long interval = request.getInterval();
    assertEquals(2, out9th.size());
    final List<Event> out9 = out9th.get(Long.valueOf(start));
    assertNotNull(out9);
    List<Event> out10 = out9th.get(Long.valueOf(start + interval));
    assertNotNull(out10);
    // verify the first chunk
    assertEquals(1, out9.size());
    final Event event1 = out9.get(0);
    assertEquals("Alameda", event1.get("county"));
    assertEquals(2L, event1.get("count()"));
    assertEquals(7L, event1.get("sum(number)"));
    assertEquals(15L, event1.get("min(distance)"));
    assertEquals(60L, event1.get("max(distance)"));

    // verify the second chunk
    assertEquals(1, out10.size());
    final Event event2 = out10.get(0);
    assertEquals("Alameda", event2.get("county"));
    assertEquals(3L, event2.get("count()"));
    assertEquals(4L, event2.get("sum(number)"));
    assertEquals(50L, event2.get("min(distance)"));
    assertEquals(80L, event2.get("max(distance)"));
  }

  /** 10th step: summarize with a two-dimensional filter. */
  public void testTwoDimensionalFilter() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = firstIngest;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("county = 'San Mateo' AND city = 'Belmont'");

    final Map<Long, List<Event>> out10th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 10th step -- summarize with filter by two-dimensional filter:
    // county = 'San Mateo' AND city = 'Belmont'
    final long interval = request.getInterval();
    assertEquals(2, out10th.size());
    final List<Event> out9 = out10th.get(Long.valueOf(start));
    assertNotNull(out9);
    List<Event> out10 = out10th.get(Long.valueOf(start + interval));
    assertNotNull(out10);
    // verify the first chunk
    assertEquals(1, out9.size());
    final Event event1 = out9.get(0);
    assertEquals(2L, event1.get("count()"));
    assertEquals(12L, event1.get("sum(number)"));
    assertEquals(70L, event1.get("min(distance)"));
    assertEquals(90L, event1.get("max(distance)"));

    // verify the second chunk
    assertEquals(1, out10.size());
    final Event event2 = out10.get(0);
    assertEquals(1L, event2.get("count()"));
    assertEquals(5L, event2.get("sum(number)"));
    assertEquals(90L, event2.get("min(distance)"));
    assertEquals(90L, event2.get("max(distance)"));
  }

  /** 11th step: summarize with a two-dimensional filter, including zero output. */
  public void testTwoDimensionalFilterForZeroOutput() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = firstIngest;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("county = 'San Mateo' AND city = 'Redwood City'");

    Map<Long, List<Event>> out11th = TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 11th step -- summarize with filter by two-dimensional filter:
    // county = 'San Mateo' AND city = 'Redwood City'
    final long interval = request.getInterval();
    final StringBuilder sb =
        new StringBuilder("start=")
            .append(request.getStartTime())
            .append(", end=")
            .append(request.getEndTime())
            .append("\n");
    sb.append("out11th=").append(out11th);
    assertEquals(sb.toString(), 2, out11th.size());
    final List<Event> out1 = out11th.get(Long.valueOf(start));
    assertNotNull(out1);
    List<Event> out2 = out11th.get(Long.valueOf(start + interval));
    assertNotNull(out2);

    // verify the first chunk
    assertEquals(0, out1.size());

    // verify the second chunk
    assertEquals(1, out2.size());
    final Event event2 = out2.get(0);
    assertEquals(2L, event2.get("count()"));
    assertEquals(5L, event2.get("sum(number)"));
    assertEquals(5L, event2.get("min(distance)"));
    assertEquals(20L, event2.get("max(distance)"));
  }

  /** 12th step: summarize with a filter for the third dimension. */
  public void testFilterThirdDimension() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(firstIngest);
    request.setEndTime(firstIngest + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("kind = 'DEER'");

    final Map<Long, List<Event>> out12th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 12th step -- summarize with filter by the third dimension dimension:
    // kind = 'DEER'
    final long start = request.getStartTime();
    final long interval = request.getInterval();
    assertEquals(2, out12th.size());
    final List<Event> out9 = out12th.get(Long.valueOf(start));
    assertNotNull(out9);
    List<Event> out10 = out12th.get(Long.valueOf(start + interval));
    assertNotNull(out10);
    // verify the first chunk
    assertEquals(1, out9.size());
    final Event event1 = out9.get(0);
    assertEquals(4L, event1.get("count()"));
    assertEquals(21L, event1.get("sum(number)"));
    assertEquals(15L, event1.get("min(distance)"));
    assertEquals(90L, event1.get("max(distance)"));

    // verify the second chunk
    assertEquals(1, out10.size());
    final Event event2 = out10.get(0);
    assertEquals(4L, event2.get("count()"));
    assertEquals(17L, event2.get("sum(number)"));
    assertEquals(10L, event2.get("min(distance)"));
    assertEquals(90L, event2.get("max(distance)"));
  }

  /** 13th step: summarize with a filter group by the third dimension. */
  public void testFilterGroupByThirdDimension() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = firstIngest;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setGroup(Arrays.asList("kind"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("county = 'San Mateo'");

    final Map<Long, List<Event>> out13th =
        TestUtils.executeSummarize(dataEngine, streamDesc, request);

    // Verify the results: 13th step -- summarize with filter group by the third dimension
    // county = 'San Mateo', groups = ['kind']
    final long interval = request.getInterval();
    assertEquals(2, out13th.size());
    final List<Event> out1 = out13th.get(Long.valueOf(start));
    assertNotNull(out1);
    List<Event> out2 = out13th.get(Long.valueOf(start + interval));
    assertNotNull(out2);
    // verify the first chunk
    assertEquals(2, out1.size());
    final Optional<Event> out10 =
        out1.stream().filter(event -> "DEER".equals(event.get("kind"))).findFirst();
    assertTrue(out10.isPresent());
    assertEquals("DEER", out10.get().get("kind"));
    assertEquals(2L, out10.get().get("count()"));
    assertEquals(12L, out10.get().get("sum(number)"));
    assertEquals(70L, out10.get().get("min(distance)"));
    assertEquals(90L, out10.get().get("max(distance)"));
    final Optional<Event> out11 =
        out1.stream().filter(event -> "COYOTE".equals(event.get("kind"))).findFirst();
    assertTrue(out11.isPresent());
    assertEquals("COYOTE", out11.get().get("kind"));
    assertEquals(1L, out11.get().get("count()"));
    assertEquals(1L, out11.get().get("sum(number)"));
    assertEquals(5L, out11.get().get("min(distance)"));
    assertEquals(5L, out11.get().get("max(distance)"));

    // verify the second chunk
    assertEquals(2, out2.size());
    final Optional<Event> out20 =
        out2.stream().filter(event -> "DEER".equals(event.get("kind"))).findFirst();
    assertTrue(out20.isPresent());
    assertEquals("DEER", out20.get().get("kind"));
    assertEquals(2L, out20.get().get("count()"));
    assertEquals(9L, out20.get().get("sum(number)"));
    assertEquals(20L, out20.get().get("min(distance)"));
    assertEquals(90L, out20.get().get("max(distance)"));
    final Optional<Event> out21 =
        out2.stream().filter(event -> "COYOTE".equals(event.get("kind"))).findFirst();
    assertTrue(out21.isPresent());
    assertEquals("COYOTE", out21.get().get("kind"));
    assertEquals(1L, out21.get().get("count()"));
    assertEquals(1L, out21.get().get("sum(number)"));
    assertEquals(5L, out21.get().get("min(distance)"));
    assertEquals(5L, out21.get().get("max(distance)"));
  }

  /** 14th step: Filter by non-indexed attribute (when view is setup). */
  public void testFilterByNonIndexedAttribute() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = params.firstIngest;
    final long basicInterval = params.basicInterval;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("distance < 40'");

    final Map<Long, List<Event>> out =
        TestUtils.executeSummarize(dataEngine, params.streamDesc, request);

    final long interval = request.getInterval();
    assertEquals(2, out.size());
    final List<Event> out1 = out.get(Long.valueOf(start));
    assertNotNull(out1);
    List<Event> out2 = out.get(Long.valueOf(start + interval));
    assertNotNull(out2);
    // verify the first chunk
    assertEquals(4, out1.size());
    final Event event10 = out1.get(0);
    assertEquals("Alameda", event10.get("county"));
    assertEquals(1L, event10.get("count()"));
    assertEquals(5L, event10.get("sum(number)"));
    assertEquals(15L, event10.get("min(distance)"));
    assertEquals(15L, event10.get("max(distance)"));
    final Event event11 = out1.get(1);
    assertEquals("Marin", event11.get("county"));
    assertEquals(1L, event11.get("count()"));
    assertEquals(9L, event11.get("sum(number)"));
    assertEquals(20L, event11.get("min(distance)"));
    assertEquals(20L, event11.get("max(distance)"));
    final Event event12 = out1.get(2);
    assertEquals("San Mateo", event12.get("county"));
    assertEquals(1L, event12.get("count()"));
    assertEquals(1L, event12.get("sum(number)"));
    assertEquals(5L, event12.get("min(distance)"));
    assertEquals(5L, event12.get("max(distance)"));
    final Event event13 = out1.get(3);
    assertEquals("Santa Clara", event13.get("county"));
    assertEquals(1L, event13.get("count()"));
    assertEquals(4L, event13.get("sum(number)"));
    assertEquals(20L, event13.get("min(distance)"));
    assertEquals(20L, event13.get("max(distance)"));

    // verify the second chunk
    assertEquals(2, out2.size());
    final Event event20 = out2.get(0);
    assertEquals("Marin", event20.get("county"));
    assertEquals(1L, event20.get("count()"));
    assertEquals(5L, event20.get("sum(number)"));
    assertEquals(10L, event20.get("min(distance)"));
    assertEquals(10L, event20.get("max(distance)"));
    final Event event21 = out2.get(1);
    assertEquals("San Mateo", event21.get("county"));
    assertEquals(2L, event21.get("count()"));
    assertEquals(5L, event21.get("sum(number)"));
    assertEquals(5L, event21.get("min(distance)"));
    assertEquals(20L, event21.get("max(distance)"));
  }

  public void testFilterWithInOperator() throws Throwable {
    final SummarizeRequest request = new SummarizeRequest();
    final long start = firstIngest;
    request.setStartTime(start);
    request.setEndTime(start + basicInterval * 2);
    request.setInterval(basicInterval);
    request.setGroup(Arrays.asList("county"));
    request.setAggregates(
        Arrays.asList(new Count(), new Sum("number"), new Min("distance"), new Max("distance")));
    request.setFilter("county IN ('San Mateo', 'Alameda')");

    final Map<Long, List<Event>> out = TestUtils.executeSummarize(dataEngine, streamDesc, request);

    System.out.println(out);
    // Verify the results: 9th step -- summarize with filter by county = 'Alameda'.
    // The result should be equal to the part of 5th step where the county is 'Alameda'.
    final long interval = request.getInterval();
    assertEquals(2, out.size());
    final List<Event> out1 = out.get(Long.valueOf(start));
    assertNotNull(out1);
    List<Event> out2 = out.get(Long.valueOf(start + interval));
    assertNotNull(out2);
    // verify the first chunk
    assertEquals(2, out1.size());
    final Event event10 = out1.get(0);
    assertEquals("Alameda", event10.get("county"));
    assertEquals(2L, event10.get("count()"));
    assertEquals(7L, event10.get("sum(number)"));
    assertEquals(15L, event10.get("min(distance)"));
    assertEquals(60L, event10.get("max(distance)"));
    final Event event11 = out1.get(1);
    assertEquals("San Mateo", event11.get("county"));
    assertEquals(3L, event11.get("count()"));
    assertEquals(13L, event11.get("sum(number)"));
    assertEquals(5L, event11.get("min(distance)"));
    assertEquals(90L, event11.get("max(distance)"));

    // verify the second chunk
    assertEquals(2, out2.size());
    final Event event20 = out2.get(0);
    assertEquals("Alameda", event20.get("county"));
    assertEquals(3L, event20.get("count()"));
    assertEquals(4L, event20.get("sum(number)"));
    assertEquals(50L, event20.get("min(distance)"));
    assertEquals(80L, event20.get("max(distance)"));
    final Event event21 = out2.get(1);
    assertEquals("San Mateo", event21.get("county"));
    assertEquals(3L, event21.get("count()"));
    assertEquals(10L, event21.get("sum(number)"));
    assertEquals(5L, event21.get("min(distance)"));
    assertEquals(90L, event21.get("max(distance)"));
  }
}
