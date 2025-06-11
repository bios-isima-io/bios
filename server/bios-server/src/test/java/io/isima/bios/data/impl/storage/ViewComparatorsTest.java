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
package io.isima.bios.data.impl.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.net.InetAddresses;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.View;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.Arrays;
import java.util.Comparator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewComparatorsTest {

  private static CassTenant cassTenant;
  private static CassStream cassStream;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TenantDesc tenant = new TenantDesc("testConfig", System.currentTimeMillis(), false);
    StreamDesc signal = new StreamDesc("testSignal", System.currentTimeMillis());
    signal.addAttribute(new AttributeDesc("stringKey", InternalAttributeType.STRING));
    signal.addAttribute(new AttributeDesc("longKey", InternalAttributeType.LONG));
    signal.addAttribute(new AttributeDesc("inetKey", InternalAttributeType.INET));
    signal.setParent(tenant);
    cassTenant = new CassTenant(tenant);
    cassStream = new SignalCassStream(cassTenant, signal);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGenerateStringComparator() {
    final View view = new View();
    view.setFunction(ViewFunction.SORT);
    view.setBy("stringKey");
    view.setCaseSensitive(Boolean.TRUE);
    Comparator<Event> comparator = ExtractView.generateComparator(view, cassStream);
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "abcde");
      assertEquals(0, comparator.compare(e1, e2));
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "abcdd");
      assertTrue(comparator.compare(e1, e2) > 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "abcdf");
      assertTrue(comparator.compare(e1, e2) < 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      assertTrue(comparator.compare(e1, e2) > 0);
    }
  }

  @Test
  public void testGenerateStringCaseInsensitiveComparator() {
    final View view = new View();
    view.setFunction(ViewFunction.SORT);
    view.setBy("stringKey");
    view.setCaseSensitive(Boolean.FALSE);
    Comparator<Event> comparator = ExtractView.generateComparator(view, cassStream);
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      assertEquals(0, comparator.compare(e1, e2));
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDD");
      assertTrue(comparator.compare(e1, e2) > 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDF");
      assertTrue(comparator.compare(e1, e2) < 0);
    }
  }

  @Test
  public void testGenerateInetComparator() {
    final View view = new View();
    view.setFunction(ViewFunction.SORT);
    view.setBy("inetKey");
    view.setReverse(Boolean.TRUE);
    Comparator<Event> comparator = ExtractView.generateComparator(view, cassStream);
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555555));
      assertEquals(0, comparator.compare(e1, e2));
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555554));
      assertTrue(comparator.compare(e1, e2) < 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555556));
      assertTrue(comparator.compare(e1, e2) > 0);
    }
  }

  @Test
  public void testMultiDimensionalComparator() {
    final View view = new View();
    view.setFunction(ViewFunction.SORT);
    view.setDimensions(Arrays.asList("stringKey", "longKey", "inetKey"));
    Comparator<Event> comparator = ExtractView.generateComparator(view, cassStream);
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12345));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555555));
      assertEquals(0, comparator.compare(e1, e2));
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDD");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12346));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555556));
      assertTrue(comparator.compare(e1, e2) > 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12344));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555556));
      assertTrue(comparator.compare(e1, e2) > 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12345));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555554));
      assertTrue(comparator.compare(e1, e2) > 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDF");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12344));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555554));
      assertTrue(comparator.compare(e1, e2) < 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12346));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555554));
      assertTrue(comparator.compare(e1, e2) < 0);
    }
    {
      final Event e1 = new EventJson();
      final Event e2 = new EventJson();
      e1.set("stringKey", "abcde");
      e2.set("stringKey", "ABCDE");
      e1.set("longKey", Long.valueOf(12345));
      e2.set("longKey", Long.valueOf(12345));
      e1.set("inetKey", InetAddresses.fromInteger(0x555555));
      e2.set("inetKey", InetAddresses.fromInteger(0x555556));
      assertTrue(comparator.compare(e1, e2) < 0);
    }
  }
}
