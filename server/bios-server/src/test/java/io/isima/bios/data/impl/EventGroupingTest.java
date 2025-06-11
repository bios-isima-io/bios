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
import static org.junit.Assert.assertNull;

import com.google.common.net.InetAddresses;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.ExtractState;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.CassTenant;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Avg;
import io.isima.bios.models.Count;
import io.isima.bios.models.DistinctCount;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Sum;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventGroupingTest {

  private static CassTenant cassTenant;
  private static CassStream cassStream;

  private List<Event> input;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TenantDesc tenant = new TenantDesc("testConfig", System.currentTimeMillis(), false);
    StreamDesc signal = new StreamDesc("testSignal", System.currentTimeMillis());
    signal.addAttribute(new AttributeDesc("country", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("state", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("city", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("value", InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc("inet", InternalAttributeType.INET));
    signal.setParent(tenant);
    cassTenant = new CassTenant(tenant);
    cassStream = new SignalCassStream(cassTenant, signal);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {
    input = new ArrayList<>();
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "California");
      event.set("city", "Richmond");
      event.set("value", Long.valueOf(10));
      event.set("inet", InetAddresses.forString("123.45.67.78"));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "California");
      event.set("city", "Palo Alto");
      event.set("value", Long.valueOf(10));
      event.set("inet", InetAddresses.forString("123.45.67.34"));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "California");
      event.set("city", "Richmond");
      event.set("value", Long.valueOf(7));
      event.set("inet", InetAddresses.forString("123.45.67.56"));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "Virginia");
      event.set("city", "Richmond");
      event.set("value", Long.valueOf(13));
      event.set("inet", InetAddresses.forString("123.45.67.13"));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "Japan");
      event.set("state", "Aomori");
      event.set("city", "Yokohama");
      event.set("value", Long.valueOf(21));
      event.set("inet", InetAddresses.forString("223.45.67.56"));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "Japan");
      event.set("state", "Kanagawa");
      event.set("city", "Yokohama");
      event.set("value", Long.valueOf(31));
      event.set("inet", InetAddresses.forString("223.45.67.89"));
      input.add(event);
    }
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGroupByCountryStateCity() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value").as("total");
    final var count = new Count().as("count");
    final var min = new Min("value").as("minimum");
    final var max = new Max("value").as("maximum");
    final var avg = new Avg("value").as("average");
    request.setAggregates(List.of(sum, count, min, max, avg));
    final var group = new Group(List.of("country", "state", "city"));
    request.setView(group);
    request.setAttributes(List.of());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(5, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals("Aomori", out.get(0).getAttributes().get("state"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("city"));
    assertEquals(21L, out.get(0).getAttributes().get("total"));
    assertEquals(1L, out.get(0).getAttributes().get("count"));
    assertEquals(21L, out.get(0).getAttributes().get("minimum"));
    assertEquals(21L, out.get(0).getAttributes().get("maximum"));
    assertEquals(Double.valueOf(21), (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("Japan", out.get(1).getAttributes().get("country"));
    assertEquals("Kanagawa", out.get(1).getAttributes().get("state"));
    assertEquals("Yokohama", out.get(1).getAttributes().get("city"));
    assertEquals(31L, out.get(1).getAttributes().get("total"));
    assertEquals(1L, out.get(1).getAttributes().get("count"));
    assertEquals(31L, out.get(1).getAttributes().get("minimum"));
    assertEquals(31L, out.get(1).getAttributes().get("maximum"));
    assertEquals(Double.valueOf(31), (Double) out.get(1).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(2).getAttributes().get("country"));
    assertEquals("California", out.get(2).getAttributes().get("state"));
    assertEquals("Palo Alto", out.get(2).getAttributes().get("city"));
    assertEquals(10L, out.get(2).getAttributes().get("total"));
    assertEquals(1L, out.get(2).getAttributes().get("count"));
    assertEquals(10L, out.get(2).getAttributes().get("minimum"));
    assertEquals(10L, out.get(2).getAttributes().get("maximum"));
    assertEquals(Double.valueOf(10), (Double) out.get(2).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(3).getAttributes().get("country"));
    assertEquals("California", out.get(3).getAttributes().get("state"));
    assertEquals("Richmond", out.get(3).getAttributes().get("city"));
    assertEquals(17L, out.get(3).getAttributes().get("total"));
    assertEquals(2L, out.get(3).getAttributes().get("count"));
    assertEquals(7L, out.get(3).getAttributes().get("minimum"));
    assertEquals(10L, out.get(3).getAttributes().get("maximum"));
    assertEquals(Double.valueOf(8.5), (Double) out.get(3).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(4).getAttributes().get("country"));
    assertEquals("Virginia", out.get(4).getAttributes().get("state"));
    assertEquals("Richmond", out.get(4).getAttributes().get("city"));
    assertEquals(13L, out.get(4).getAttributes().get("total"));
    assertEquals(1L, out.get(4).getAttributes().get("count"));
    assertEquals(13L, out.get(4).getAttributes().get("minimum"));
    assertEquals(13L, out.get(4).getAttributes().get("maximum"));
    assertEquals(Double.valueOf(13), (Double) out.get(4).getAttributes().get("average"), 1.e-5);
  }

  @Test
  public void testGroupByCountryCity() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value").as("total");
    final var count = new Count().as("count");
    final var min = new Min("value").as("minimum");
    final var max = new Max("value").as("maximum");
    final var avg = new Avg("value").as("average");
    request.setAggregates(List.of(sum, count, min, max, avg));
    final var group = new Group(List.of("country", "city"));
    request.setView(group);
    request.setAttributes(new ArrayList<>());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(3, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals("Yokohama", out.get(0).getAttributes().get("city"));
    assertEquals(52L, out.get(0).getAttributes().get("total"));
    assertEquals(2L, out.get(0).getAttributes().get("count"));
    assertEquals(21L, out.get(0).getAttributes().get("minimum"));
    assertEquals(31L, out.get(0).getAttributes().get("maximum"));
    assertEquals(26.0, (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(1).getAttributes().get("country"));
    assertEquals("Palo Alto", out.get(1).getAttributes().get("city"));
    assertEquals(10L, out.get(1).getAttributes().get("total"));
    assertEquals(1L, out.get(1).getAttributes().get("count"));
    assertEquals(10L, out.get(1).getAttributes().get("minimum"));
    assertEquals(10L, out.get(1).getAttributes().get("maximum"));
    assertEquals(10.0, (Double) out.get(1).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(2).getAttributes().get("country"));
    assertEquals("Richmond", out.get(2).getAttributes().get("city"));
    assertEquals(30L, out.get(2).getAttributes().get("total"));
    assertEquals(3L, out.get(2).getAttributes().get("count"));
    assertEquals(7L, out.get(2).getAttributes().get("minimum"));
    assertEquals(13L, out.get(2).getAttributes().get("maximum"));
    assertEquals(10.0, (Double) out.get(2).getAttributes().get("average"), 1.e-5);
  }

  @Test
  public void testGroupByCity() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value").as("total");
    final var count = new Count().as("count");
    final var min = new Min("value").as("minimum");
    final var max = new Max("value").as("maximum");
    final var avg = new Avg("value").as("average");
    request.setAggregates(List.of(sum, count, min, max, avg));
    final var group = new Group("city");
    request.setView(group);
    request.setAttributes(new ArrayList<>());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(3, out.size());

    assertEquals("Palo Alto", out.get(0).getAttributes().get("city"));
    assertEquals(10L, out.get(0).getAttributes().get("total"));
    assertEquals(1L, out.get(0).getAttributes().get("count"));
    assertEquals(10L, out.get(0).getAttributes().get("minimum"));
    assertEquals(10L, out.get(0).getAttributes().get("maximum"));
    assertEquals(10.0, (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("Richmond", out.get(1).getAttributes().get("city"));
    assertEquals(30L, out.get(1).getAttributes().get("total"));
    assertEquals(3L, out.get(1).getAttributes().get("count"));
    assertEquals(7L, out.get(1).getAttributes().get("minimum"));
    assertEquals(13L, out.get(1).getAttributes().get("maximum"));
    assertEquals(10.0, (Double) out.get(1).getAttributes().get("average"), 1.e-5);

    assertEquals("Yokohama", out.get(2).getAttributes().get("city"));
    assertEquals(52L, out.get(2).getAttributes().get("total"));
    assertEquals(2L, out.get(2).getAttributes().get("count"));
    assertEquals(21L, out.get(2).getAttributes().get("minimum"));
    assertEquals(31L, out.get(2).getAttributes().get("maximum"));
    assertEquals(26.0, (Double) out.get(2).getAttributes().get("average"), 1.e-5);
  }

  @Test
  public void testGroupByCountry() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var distinctCount = new DistinctCount("city").as("uniqueCities");
    final var avg = new Avg("value").as("average");
    request.setAggregates(List.of(distinctCount, avg));
    final View group = new Group("country");
    request.setView(group);
    request.setAttributes(List.of());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(2, out.size());

    assertEquals("Japan", out.get(0).getAttributes().get("country"));
    assertEquals(1L, out.get(0).getAttributes().get("uniqueCities"));
    assertEquals(26, (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(1).getAttributes().get("country"));
    assertEquals(2L, out.get(1).getAttributes().get("uniqueCities"));
    assertEquals(10, (Double) out.get(1).getAttributes().get("average"), 1.e-5);
  }

  @Test
  public void testDistinctCountGroupByState() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var distinctCount = new DistinctCount("city").as("uniqueCities");
    request.setAggregates(List.of(distinctCount));
    final View group = new Group("state");
    request.setView(group);
    request.setAttributes(List.of());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(4, out.size());

    assertEquals("Aomori", out.get(0).getAttributes().get("state"));
    assertEquals(1L, out.get(0).getAttributes().get("uniqueCities"));

    assertEquals("California", out.get(1).getAttributes().get("state"));
    assertEquals(2L, out.get(1).getAttributes().get("uniqueCities"));

    assertEquals("Kanagawa", out.get(2).getAttributes().get("state"));
    assertEquals(1L, out.get(2).getAttributes().get("uniqueCities"));

    assertEquals("Virginia", out.get(3).getAttributes().get("state"));
    assertEquals(1L, out.get(3).getAttributes().get("uniqueCities"));
  }

  @Test
  public void testZeroDimensionGrouping() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value").as("total");
    final var count = new Count().as("count");
    final var min = new Min("value").as("minimum");
    final var max = new Max("value").as("maximum");
    request.setAggregates(List.of(sum, count, min, max));
    final View group = new Group();
    request.setView(group);
    request.setAttributes(List.of());
    final ExtractState state =
        new ExtractState(null, cassStream.getTenantName(), cassStream.getStreamName(), null);
    state.setInput(request);
    String err = SignalExtractor.validateExtractRequest(state, cassStream);
    assertNull(err);
    Function<List<Event>, List<Event>> viewFunc =
        ExtractView.createView(
            request.getView(),
            request.getAttributes(),
            request.getAggregates(),
            cassStream,
            EventJson::new);

    List<Event> out = viewFunc.apply(input);
    assertEquals(1, out.size());

    assertEquals(92L, out.get(0).getAttributes().get("total"));
    assertEquals(6L, out.get(0).getAttributes().get("count"));
    assertEquals(7L, out.get(0).getAttributes().get("minimum"));
    assertEquals(31L, out.get(0).getAttributes().get("maximum"));
  }
}
