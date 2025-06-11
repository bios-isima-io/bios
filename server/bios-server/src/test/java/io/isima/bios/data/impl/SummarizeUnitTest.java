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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.CassTenant;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.data.impl.storage.StorageDataUtils;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SummarizeUnitTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mapper = TfosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  private TenantDesc tenantDesc;
  private StreamDesc signalDesc;
  private CassTenant cassTenant;
  private CassStream cassStream;
  private List<Event> events;

  @Before
  public void setUp() throws Exception {
    tenantDesc = new TenantDesc("mytest", System.currentTimeMillis(), Boolean.FALSE);
    events = new ArrayList<>();
    signalDesc = new StreamDesc("mysignal", System.currentTimeMillis(), Boolean.FALSE);
    signalDesc.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("value", InternalAttributeType.INT));
    tenantDesc.addStream(signalDesc);
    cassTenant = new CassTenant(tenantDesc);
    cassStream = new SignalCassStream(cassTenant, signalDesc);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testSearchEvent() throws Exception {
    String[] sources = {
      "{'eventId':'4216ed5e-1f3a-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1, 'value': 150}", // 0
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a2686a',"
          + " 'ingestTimestamp': 3, 'value': 150}", // 1
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26866',"
          + " 'ingestTimestamp': 7, 'value': 100}", // 2
      "{'eventId': '447930a2-1f3d-11e9-9945-080027a26865',"
          + " 'ingestTimestamp': 9, 'value': 200}", // 3
      "{'eventId': '49095a78-1f3d-11e9-acad-080027a26865',"
          + " 'ingestTimestamp': 13, 'value': 300}", // 4
      "{'eventId': '4cf1239a-1f3d-11e9-84f3-080027a26865',"
          + " 'ingestTimestamp': 17, 'value': 400}", // 5
      "{'eventId': '50ba6dda-1f3d-11e9-a463-080027a26865',"
          + " 'ingestTimestamp': 21, 'value': 500}", // 6
    };
    for (String entry : sources) {
      events.add(mapper.readValue(entry.replaceAll("'", "\""), EventJson.class));
    }
    assertEquals(0, StorageDataUtils.search(events, 0L));
    assertEquals(0, StorageDataUtils.search(events, 1L));
    assertEquals(1, StorageDataUtils.search(events, 2L));
    assertEquals(1, StorageDataUtils.search(events, 3L));
    assertEquals(2, StorageDataUtils.search(events, 6L));
    assertEquals(2, StorageDataUtils.search(events, 7L));
    assertEquals(3, StorageDataUtils.search(events, 9L));
    assertEquals(4, StorageDataUtils.search(events, 13L));
    assertEquals(5, StorageDataUtils.search(events, 17L));
    assertEquals(6, StorageDataUtils.search(events, 21L));
    assertEquals(7, StorageDataUtils.search(events, 22L));
  }

  @Test
  public void testNoGroupSingleInterval() throws Exception {
    String[] sources = {
      "{'eventId':'4216ed5e-1f3a-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1548268337377,"
          + " 'value': 150}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a2686a',"
          + " 'ingestTimestamp': 1548268347261,"
          + " 'value': 150}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26866',"
          + " 'ingestTimestamp': 1548268347377,"
          + " 'value': 100}",
      "{'eventId': '447930a2-1f3d-11e9-9945-080027a26865',"
          + " 'ingestTimestamp': 1548268361376,"
          + " 'value': 200}",
      "{'eventId': '49095a78-1f3d-11e9-acad-080027a26865',"
          + " 'ingestTimestamp': 1548268369032,"
          + " 'value': 300}",
      "{'eventId': '4cf1239a-1f3d-11e9-84f3-080027a26865',"
          + " 'ingestTimestamp': 1548268375584,"
          + " 'value': 400}",
      "{'eventId': '50ba6dda-1f3d-11e9-a463-080027a26865',"
          + " 'ingestTimestamp': 1548268381937,"
          + " 'value': 500}",
    };
    for (String entry : sources) {
      events.add(mapper.readValue(entry.replaceAll("'", "\""), EventJson.class));
    }

    {
      final SummarizeRequest request = new SummarizeRequest();
      request.setStartTime(1548268200000L);
      request.setEndTime(1548268400001L);
      long interval = 100000L;
      request.setInterval(interval);
      request.setAggregates(
          Arrays.asList(
              new Aggregate(MetricFunction.SUM, "value", null),
              new Aggregate(MetricFunction.COUNT, null, null)));
      Map<Long, List<Event>> out =
          cassStream.summarizeEvents(
              events, request, null, cassStream, new EventFactory() {}, false);
      // System.out.println(out);
      assertEquals(3, out.size());
      final List<Event> events0 = out.get(Long.valueOf(1548268200000L));
      assertNotNull(events0);
      assertEquals(0, events0.size());

      final List<Event> events1 = out.get(Long.valueOf(1548268300000L));
      assertNotNull(events1);
      assertEquals(1, events1.size());
      assertEquals(Integer.valueOf(1800), events1.get(0).get("sum(value)"));
      assertEquals(Long.valueOf(7), events1.get(0).get("count()"));

      final List<Event> events2 = out.get(Long.valueOf(1548268400000L));
      assertNotNull(events2);
      assertEquals(0, events2.size());
    }
  }

  @Test
  public void testNoGroupTwoIntervals() throws Exception {
    String[] sources = {
      "{'eventId':'4216ed5e-1f3a-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1548268337377,"
          + " 'value': 150}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a2686a',"
          + " 'ingestTimestamp': 1548268347261,"
          + " 'value': 150}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26866',"
          + " 'ingestTimestamp': 1548268347377,"
          + " 'value': 100}",
      "{'eventId': '447930a2-1f3d-11e9-9945-080027a26865',"
          + " 'ingestTimestamp': 1548268361376,"
          + " 'value': 200}",
      "{'eventId': '49095a78-1f3d-11e9-acad-080027a26865',"
          + " 'ingestTimestamp': 1548268369032,"
          + " 'value': 300}",
      "{'eventId': '4cf1239a-1f3d-11e9-84f3-080027a26865',"
          + " 'ingestTimestamp': 1548268375584,"
          + " 'value': 400}",
      "{'eventId': '50ba6dda-1f3d-11e9-a463-080027a26865',"
          + " 'ingestTimestamp': 1548268381937,"
          + " 'value': 500}",
    };
    for (String entry : sources) {
      events.add(mapper.readValue(entry.replaceAll("'", "\""), EventJson.class));
    }

    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(1548268250000L);
    request.setEndTime(1548268400001L);
    long interval = 50000L;
    request.setInterval(interval);
    request.setAggregates(
        Arrays.asList(
            new Aggregate(MetricFunction.SUM, "value", null),
            new Aggregate(MetricFunction.COUNT, null, null)));
    Map<Long, List<Event>> out =
        cassStream.summarizeEvents(events, request, null, cassStream, new EventFactory() {}, false);
    // System.out.println(out);
    assertEquals(4, out.size());

    final List<Event> events0 = out.get(Long.valueOf(1548268250000L));
    assertNotNull(events0);
    assertEquals(0, events0.size());

    final List<Event> events1 = out.get(Long.valueOf(1548268300000L));
    assertNotNull(events1);
    assertEquals(Integer.valueOf(400), events1.get(0).get("sum(value)"));
    assertEquals(Long.valueOf(3L), events1.get(0).get("count()"));
    assertEquals(1, events1.size());
    assertFalse(events1.get(0).getAttributes().containsKey("city"));
    assertFalse(events1.get(0).getAttributes().containsKey("state"));
    assertFalse(events1.get(0).getAttributes().containsKey("value"));

    final List<Event> events2 = out.get(Long.valueOf(1548268350000L));
    assertNotNull(events2);
    assertEquals(1, events2.size());
    assertEquals(Integer.valueOf(1400), events2.get(0).get("sum(value)"));
    assertEquals(Long.valueOf(4L), events2.get(0).get("count()"));
    assertFalse(events2.get(0).getAttributes().containsKey("city"));
    assertFalse(events2.get(0).getAttributes().containsKey("state"));
    assertFalse(events2.get(0).getAttributes().containsKey("value"));

    final List<Event> events3 = out.get(Long.valueOf(1548268400000L));
    assertNotNull(events3);
    assertEquals(0, events3.size());
  }

  @Test
  public void testTwoDimGroupsSingleInterval() throws Exception {
    signalDesc = new StreamDesc("mysignal", System.currentTimeMillis(), Boolean.FALSE);
    signalDesc.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("value", InternalAttributeType.INT));
    tenantDesc.addStream(signalDesc);
    cassTenant = new CassTenant(tenantDesc);
    cassStream = new SignalCassStream(cassTenant, signalDesc);
    String[] sources = {
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1548268320589,"
          + " 'value': 100,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1548268332334,"
          + " 'value': 100,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 1548268337377,"
          + " 'value': 100,"
          + " 'city': 'redwood city',"
          + " 'state': 'california'}",
      "{'eventId': '447930a2-1f3d-11e9-9945-080027a26865',"
          + " 'ingestTimestamp': 1548268341376,"
          + " 'value': 200,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId': '49095a78-1f3d-11e9-acad-080027a26865',"
          + " 'ingestTimestamp': 1548268369032,"
          + " 'value': 300,"
          + " 'city': 'phoenix',"
          + " 'state': 'arizona'}",
      "{'eventId': '4cf1239a-1f3d-11e9-84f3-080027a26865',"
          + " 'ingestTimestamp': 1548268375584,"
          + " 'value': 400,"
          + " 'city': 'portland',"
          + " 'state': 'oregon'}",
      "{'eventId': '50ba6dda-1f3d-11e9-a463-080027a26865',"
          + " 'ingestTimestamp': 1548268381937,"
          + " 'value': 500,"
          + " 'city': 'redwood city',"
          + " 'state': 'california'}",
    };
    for (String entry : sources) {
      events.add(mapper.readValue(entry.replaceAll("'", "\""), EventJson.class));
    }

    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(1548268300000L);
    request.setEndTime(1548268350001L);
    request.setInterval(50000L);
    request.setAggregates(
        Arrays.asList(
            new Aggregate(MetricFunction.SUM, "value", null),
            new Aggregate(MetricFunction.COUNT, null, null)));
    request.setGroup(Arrays.asList("state", "city"));
    Map<Long, List<Event>> out =
        cassStream.summarizeEvents(events, request, null, cassStream, new EventFactory() {}, false);
    System.out.println(out);
    assertEquals(2, out.size());

    final List<Event> events1 = out.get(Long.valueOf(1548268300000L));
    final List<Event> events2 = out.get(Long.valueOf(1548268350000L));

    assertNotNull(events1);
    assertEquals(2, events1.size());

    assertNotNull(events2);
    assertEquals(3, events2.size());
  }

  @Test(expected = ConstraintViolationException.class)
  public void testTimeRangeEdges() throws Exception {
    signalDesc = new StreamDesc("mysignal", System.currentTimeMillis(), Boolean.FALSE);
    signalDesc.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signalDesc.addAttribute(new AttributeDesc("value", InternalAttributeType.INT));
    tenantDesc.addStream(signalDesc);
    cassTenant = new CassTenant(tenantDesc);
    cassStream = new SignalCassStream(cassTenant, signalDesc);
    String[] sources = {
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 20589,"
          + " 'value': 100,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 32334,"
          + " 'value': 100,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId':'4216ed5e-1f3d-11e9-b5a7-080027a26865',"
          + " 'ingestTimestamp': 37377,"
          + " 'value': 100,"
          + " 'city': 'redwood city',"
          + " 'state': 'california'}",
      "{'eventId': '447930a2-1f3d-11e9-9945-080027a26865',"
          + " 'ingestTimestamp': 41376,"
          + " 'value': 200,"
          + " 'city': 'san jose',"
          + " 'state': 'california'}",
      "{'eventId': '49095a78-1f3d-11e9-acad-080027a26865',"
          + " 'ingestTimestamp': 69032,"
          + " 'value': 300,"
          + " 'city': 'phoenix',"
          + " 'state': 'arizona'}",
      "{'eventId': '4cf1239a-1f3d-11e9-84f3-080027a26865',"
          + " 'ingestTimestamp': 75584,"
          + " 'value': 400,"
          + " 'city': 'portland',"
          + " 'state': 'oregon'}",
      "{'eventId': '50ba6dda-1f3d-11e9-a463-080027a26865',"
          + " 'ingestTimestamp': 81937,"
          + " 'value': 500,"
          + " 'city': 'redwood city',"
          + " 'state': 'california'}",
    };
    for (String entry : sources) {
      events.add(mapper.readValue(entry.replaceAll("'", "\""), EventJson.class));
    }

    final SummarizeRequest request = new SummarizeRequest();
    request.setStartTime(0L);
    request.setEndTime(1000L);
    request.setInterval(50000L);
    request.setHorizon(100000L);
    request.setAggregates(
        Arrays.asList(
            new Aggregate(MetricFunction.SUM, "value", null),
            new Aggregate(MetricFunction.COUNT, null, null)));
    request.setGroup(Arrays.asList("state", "city"));
    cassStream.summarizeEvents(events, request, null, cassStream, new EventFactory() {}, false);
  }
}
