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
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ExtractRequest;
import io.isima.bios.models.Sum;
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

public class RollupCollectionTest {

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
    signal.addAttribute(new AttributeDesc("count", InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc("value_sum", InternalAttributeType.LONG));
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
      event.set("count", Long.valueOf(3));
      event.set("value_sum", Long.valueOf(10));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "California");
      event.set("city", "Palo Alto");
      event.set("count", Long.valueOf(5));
      event.set("value_sum", Long.valueOf(53));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "California");
      event.set("city", "Richmond");
      event.set("count", Long.valueOf(2));
      event.set("value_sum", Long.valueOf(7));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "United States");
      event.set("state", "Virginia");
      event.set("city", "Richmond");
      event.set("count", Long.valueOf(4));
      event.set("value_sum", Long.valueOf(19));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "Japan");
      event.set("state", "Kanagawa");
      event.set("city", "Yokohama");
      event.set("count", Long.valueOf(6));
      event.set("value_sum", Long.valueOf(24));
      input.add(event);
    }
    {
      final Event event = new EventJson();
      event.set("country", "Japan");
      event.set("state", "Aomori");
      event.set("city", "Yokohama");
      event.set("count", Long.valueOf(2));
      event.set("value_sum", Long.valueOf(11));
      input.add(event);
    }
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGroupByCountryStateCity() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value_sum").as("sum(value)");
    final var count = new Sum("count").as("count()");
    final var avg = new Avg("value_sum").as("average").countAttribute("count");
    request.setAggregates(List.of(sum, count, avg));
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
    assertEquals(11L, out.get(0).getAttributes().get("sum(value)"));
    assertEquals(2L, out.get(0).getAttributes().get("count()"));
    assertEquals(5.5, (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("Japan", out.get(1).getAttributes().get("country"));
    assertEquals("Kanagawa", out.get(1).getAttributes().get("state"));
    assertEquals("Yokohama", out.get(1).getAttributes().get("city"));
    assertEquals(24L, out.get(1).getAttributes().get("sum(value)"));
    assertEquals(6L, out.get(1).getAttributes().get("count()"));
    assertEquals(4.0, (Double) out.get(1).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(2).getAttributes().get("country"));
    assertEquals("California", out.get(2).getAttributes().get("state"));
    assertEquals("Palo Alto", out.get(2).getAttributes().get("city"));
    assertEquals(53L, out.get(2).getAttributes().get("sum(value)"));
    assertEquals(5L, out.get(2).getAttributes().get("count()"));
    assertEquals(10.6, (Double) out.get(2).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(3).getAttributes().get("country"));
    assertEquals("California", out.get(3).getAttributes().get("state"));
    assertEquals("Richmond", out.get(3).getAttributes().get("city"));
    assertEquals(17L, out.get(3).getAttributes().get("sum(value)"));
    assertEquals(5L, out.get(3).getAttributes().get("count()"));
    assertEquals(3.4, (Double) out.get(3).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(4).getAttributes().get("country"));
    assertEquals("Virginia", out.get(4).getAttributes().get("state"));
    assertEquals("Richmond", out.get(4).getAttributes().get("city"));
    assertEquals(19L, out.get(4).getAttributes().get("sum(value)"));
    assertEquals(4L, out.get(4).getAttributes().get("count()"));
    assertEquals(4.75, (Double) out.get(4).getAttributes().get("average"), 1.e-5);
  }

  @Test
  public void testGroupByCountry() throws TfosException, ApplicationException {
    final ExtractRequest request = new ExtractRequest();
    final var sum = new Sum("value_sum").as("sum(value)");
    final var count = new Sum("count").as("count()");
    final var avg = new Avg("value_sum").as("average").countAttribute("count");
    request.setAggregates(List.of(sum, count, avg));
    final var group = new Group(List.of("country"));
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
    assertEquals(35L, out.get(0).getAttributes().get("sum(value)"));
    assertEquals(8L, out.get(0).getAttributes().get("count()"));
    assertEquals(4.375, (Double) out.get(0).getAttributes().get("average"), 1.e-5);

    assertEquals("United States", out.get(1).getAttributes().get("country"));
    assertEquals(89L, out.get(1).getAttributes().get("sum(value)"));
    assertEquals(14L, out.get(1).getAttributes().get("count()"));
    assertEquals(89.0 / 14, (Double) out.get(1).getAttributes().get("average"), 1.e-5);
  }
}
