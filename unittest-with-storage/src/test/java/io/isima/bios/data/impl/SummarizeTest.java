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

import io.isima.bios.models.Event;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SummarizeTest {

  private static SummarizeTestCore testCore;
  private static SummarizeTestSetup.Params params;

  @BeforeClass
  public static void setUpBeforeClass() throws Throwable {
    final String src =
        "{"
            + "  'name': 'animal_sightings',"
            + "  'type': 'signal',"
            + "  'missingValuePolicy': 'strict',"
            + "  'attributes': ["
            + "    {'name': 'county', 'type': 'string'},"
            + "    {'name': 'city', 'type': 'string'},"
            + "    {'name': 'kind', 'type': 'enum',"
            + "     'enum': ['BEAR', 'DEER', 'MOUNTAIN_LION', 'ELK', 'COYOTE']},"
            + "    {'name': 'number', 'type': 'long'},"
            + "    {'name': 'distance', 'type': 'long'}"
            + "  ],"
            + "  'views': [{"
            + "    'name': 'by_county_city_kind',"
            + "    'groupBy': ['county', 'city', 'kind'],"
            + "    'attributes': ['number', 'distance']"
            + "  }],"
            + "  'postprocesses': [{"
            + "    'view': 'by_county_city_kind',"
            + "    'rollups': [{"
            + "      'name': 'rollup_by_county_city_kind',"
            + "      'interval': {'value': 1, 'timeunit': 'minute'},"
            + "      'horizon': {'value': 1, 'timeunit': 'minute'}"
            + "    }]"
            + "  }]"
            + "}";
    testCore = new SummarizeTestCore(SummarizeTest.class, src);
    params = testCore.getParams();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Throwable {
    if (testCore != null) {
      testCore.tearDownAfterClass();
    }
  }

  @Test
  public void verifySummarizeResultsDuringSetup() {
    // Verify the results: 1st step -- summarize immediatly after the first chunk of ingestions.
    // If the signal has a corresponding rollup table, returned result at this point should be
    // empty.
    {
      assertEquals(1, params.out1st.size());
      final List<Event> out = params.out1st.get(Long.valueOf(params.request1st.getStartTime()));
      assertNotNull(out);
      assertEquals(0, out.size());
    }

    // Verify the results: 2nd step -- summarize the same with 1st but after sleeping for a while.
    // Sleeping gives rollup engine enough time to execute rollup the first chunk of events, thus
    // the operation returns the summary for the first chunk of events.
    final List<Event> out;
    {
      assertEquals(1, params.out2nd.size());
      out = params.out2nd.get(Long.valueOf(params.firstIngest));
      assertNotNull(out);
      final List<Event> out2 = params.out2nd.get(Long.valueOf(params.firstIngest));
      assertEquals(4, out.size());

      assertEquals("Alameda", out.get(0).get("county"));
      assertEquals(2L, out.get(0).get("count()"));
      assertEquals(7L, out.get(0).get("sum(number)"));
      assertEquals(15L, out.get(0).get("min(distance)"));
      assertEquals(60L, out.get(0).get("max(distance)"));

      assertEquals("Marin", out.get(1).get("county"));
      assertEquals(2L, out.get(1).get("count()"));
      assertEquals(21L, out.get(1).get("sum(number)"));
      assertEquals(20L, out.get(1).get("min(distance)"));
      assertEquals(40L, out.get(1).get("max(distance)"));

      assertEquals("San Mateo", out.get(2).get("county"));
      assertEquals(3L, out.get(2).get("count()"));
      assertEquals(13L, out.get(2).get("sum(number)"));
      assertEquals(5L, out.get(2).get("min(distance)"));
      assertEquals(90L, out.get(2).get("max(distance)"));

      assertEquals("Santa Clara", out.get(3).get("county"));
      assertEquals(1L, out.get(3).get("count()"));
      assertEquals(4L, out.get(3).get("sum(number)"));
      assertEquals(20L, out.get(3).get("min(distance)"));
      assertEquals(20L, out.get(3).get("max(distance)"));

      assertEquals(4, out2.size());

      for (int i = 0; i < out2.size(); ++i) {
        final Event event1 = out.get(i);
        final Event event2 = out2.get(i);
        assertEquals("index=" + i, event1.getAttributes(), event2.getAttributes());
        assertEquals("index=" + i, event1.getIngestTimestamp(), event2.getIngestTimestamp());
      }
    }

    // verify the results: 3rd step -- summarize with explict aggregate output names
    {
      assertEquals(1, params.out3rd.size());
      final List<Event> out3 = params.out3rd.get(Long.valueOf(params.request3rd.getStartTime()));
      assertNotNull(out);

      assertEquals(4, out3.size());

      for (int i = 0; i < out3.size(); ++i) {
        final Event event1 = out.get(i);
        final Event event3 = out3.get(i);
        assertEquals("index=" + i, event1.getIngestTimestamp(), event3.getIngestTimestamp());
        assertEquals("index=" + i, event1.get("count()"), event3.get("sightingsCount"));
        assertEquals("index=" + i, event1.get("sum(number)"), event3.get("animals_count"));
        assertEquals("index=" + i, event1.get("min(distance)"), event3.get("minimumDistance"));
        assertEquals("index=" + i, event1.get("max(distance)"), event3.get("maximumDistance"));
      }
    }

    // Verify the results: 4th step -- summarize immediately after the second cluster of ingestions.
    // Entries for two intervals should be returned, but only the first one should include the
    // summary because the second data have not been rolled up yet.
    {
      assertEquals(2, params.out4th.size());
      final long start = params.request4th.getStartTime();
      final long interval = params.request4th.getInterval();
      final List<Event> out4 = params.out4th.get(Long.valueOf(start));
      assertNotNull(out4);
      final List<Event> out5 = params.out4th.get(Long.valueOf(start + interval));
      assertNotNull(out5);
      assertEquals(0, out5.size());

      // verify the fist chunk
      assertEquals(4, out4.size());
      for (int i = 0; i < out4.size(); ++i) {
        final Event event1 = out.get(i);
        final Event event2 = out4.get(i);
        assertEquals("index=" + i, event1.getAttributes(), event2.getAttributes());
        assertEquals("index=" + i, event1.getIngestTimestamp(), event2.getIngestTimestamp());
      }
    }
  }

  @Test
  public void testBasic() throws Throwable {
    testCore.testBasic();
  }

  @Test
  public void testLongerHorizon() throws Throwable {
    testCore.testLongerHorizon();
  }

  @Test
  public void testWiderTimeRange() throws Throwable {
    testCore.testWiderTimeRange();
  }

  @Test
  public void testWiderIntervals() throws Throwable {
    testCore.testWiderIntervals();
  }

  @Test
  public void testSimpleFilter() throws Throwable {
    testCore.testSimpleFilter();
  }

  @Test
  public void testTwoDimensionalFilter() throws Throwable {
    testCore.testTwoDimensionalFilter();
  }

  @Test
  public void testTwoDimensionalFilterForZeroOutput() throws Throwable {
    testCore.testTwoDimensionalFilterForZeroOutput();
  }

  @Test
  public void testFilterThirdDimension() throws Throwable {
    testCore.testFilterThirdDimension();
  }

  @Test
  public void testFilterGroupByThirdDimension() throws Throwable {
    testCore.testFilterGroupByThirdDimension();
  }

  @Test
  public void testFilterWithInOperator() throws Throwable {
    testCore.testFilterWithInOperator();
  }
}
