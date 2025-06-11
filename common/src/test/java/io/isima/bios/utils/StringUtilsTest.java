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
package io.isima.bios.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.isima.bios.exceptions.ApplicationException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringUtilsTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testShortReadableDuration() throws ApplicationException {
    final long[] durations = {
      0,
      1,
      20,
      999,
      1000,
      2000,
      2001,
      58888,
      60000,
      120000,
      121000,
      121001,
      3590000,
      3600000,
      7200000,
      7260000,
      7261000,
      7261001,
      86399999,
      86400000,
      172800000,
      176400000,
      176460000,
      176461000,
      176461001,
      17280000000L
    };
    final String[] readableDurations = {
      "0",
      "1 ms",
      "20 ms",
      "999 ms",
      "1 secs",
      "2 secs",
      "2.001 secs",
      "58.888 secs",
      "1 mins",
      "2 mins",
      "2:01 mins",
      "2:01.001 mins",
      "59:50 mins",
      "1 hrs",
      "2 hrs",
      "2:01 hrs",
      "2:01:01 hrs",
      "2:01:01.001 hrs",
      "23:59:59.999 hrs",
      "1 days",
      "2 days",
      "2:01 days",
      "2:01:01 days",
      "2:01:01:01 days",
      "2:01:01:01.001 days",
      "200 days",
    };
    long durationInMs;
    String readableDuration;

    for (int i = 0; i < durations.length; i++) {
      durationInMs = durations[i];
      readableDuration = StringUtils.shortReadableDuration(durationInMs);
      // System.out.printf("%d: {%s}\n", durationInMs, readableDuration);
      assertEquals(readableDurations[i], readableDuration);
    }
  }

  @Test
  public void testShortReadableDurationNanos() throws ApplicationException {
    final long[] durations = {
      0,
      1,
      20,
      999,
      1000,
      2000,
      2001,
      58888,
      999999,
      1000000,
      2000000,
      2001000,
      2001001,
      999000000,
      1000000000L,
      2000000000L,
      2001000000L,
      58888000000L,
      58888001000L,
      58888001001L,
      60000000000L,
      120000000000L,
      121000000000L,
      121001000000L,
      3590000000000L,
      3600000000000L,
      7200000000000L,
      7260000000000L,
      7261000000000L,
      7261001000000L,
      86399999000000L,
      86400000000000L,
      172800000000000L,
      176400000000000L,
      176460000000000L,
      176461000000000L,
      176461001000000L,
      17280000000000000L,
      17280000000000005L
    };
    final String[] readableDurations = {
      "0",
      "1 ns",
      "20 ns",
      "999 ns",
      "1 micros",
      "2 micros",
      "2.001 micros",
      "58.888 micros",
      "999.999 micros",
      "1 ms",
      "2 ms",
      "2.001 ms",
      "2.001.001 ms",
      "999 ms",
      "1 secs",
      "2 secs",
      "2.001 secs",
      "58.888 secs",
      "58.888.001 secs",
      "58.888.001.001 secs",
      "1 mins",
      "2 mins",
      "2:01 mins",
      "2:01.001 mins",
      "59:50 mins",
      "1 hrs",
      "2 hrs",
      "2:01 hrs",
      "2:01:01 hrs",
      "2:01:01.001 hrs",
      "23:59:59.999 hrs",
      "1 days",
      "2 days",
      "2:01 days",
      "2:01:01 days",
      "2:01:01:01 days",
      "2:01:01:01.001 days",
      "200 days",
      "200:00:00:00.000.000.005 days",
    };
    long durationInNanos;
    String readableDuration;

    for (int i = 0; i < durations.length; i++) {
      durationInNanos = durations[i];
      readableDuration = StringUtils.shortReadableDurationNanos(durationInNanos);
      // System.out.printf("%d: {%s}\n", durationInNanos, readableDuration);
      assertEquals(readableDurations[i], readableDuration);
    }
  }

  @Test
  public void testSplitIntoWords() {
    testSplitIntoWordsCore("abc", List.of("abc"));
    testSplitIntoWordsCore("camelCase", List.of("camel", "case"));
    testSplitIntoWordsCore("PascalCase", List.of("pascal", "case"));
    testSplitIntoWordsCore("snake_case", List.of("snake", "case"));
    testSplitIntoWordsCore("with1number", List.of("with", "number"));
    testSplitIntoWordsCore("with2numbers56here", List.of("with", "numbers", "here"));
    testSplitIntoWordsCore("with3numbers567here", List.of("with", "numbers", "here"));
    testSplitIntoWordsCore(
        "mix_underscore_2Number_And_cases", List.of("mix", "underscore", "number", "and", "cases"));
  }

  public void testSplitIntoWordsCore(String input, List<String> expectedOutput) {
    final List<String> output = StringUtils.splitIntoWords(input);
    System.out.println(output);
    assertEquals(expectedOutput, output);
  }

  @Test
  public void testPrefixToCamelCase() {
    assertThat(StringUtils.prefixToCamelCase("audit", "productCatalog"), is("auditProductCatalog"));
    assertThat(StringUtils.prefixToCamelCase("prev", "productId"), is("prevProductId"));
    assertThat(StringUtils.prefixToCamelCase("prev", "gtin"), is("prevGtin"));

    assertThrows(IllegalArgumentException.class, () -> StringUtils.prefixToCamelCase("abc", ""));
    assertThrows(IllegalArgumentException.class, () -> StringUtils.prefixToCamelCase("", "abc"));
    assertThrows(NullPointerException.class, () -> StringUtils.prefixToCamelCase(null, "abc"));
    assertThrows(NullPointerException.class, () -> StringUtils.prefixToCamelCase("abc", null));
  }

  @Test
  public void testGeneratePath() {
    assertThat(
        StringUtils.generatePath(
            "/bios/v1/tenants/{tenantName}/signals/{signalName}", "testTenant", "testSignal"),
        is("/bios/v1/tenants/testTenant/signals/testSignal"));

    // The method would return reply in best-effort manner.
    assertThat(
        StringUtils.generatePath(
            "/bios/v1/tenants/{tenantName}/signals/{signalName}", "testTenant"),
        is("/bios/v1/tenants/testTenant/signals/{signalName}"));
    assertThat(
        StringUtils.generatePath(
            "/bios/v1/tenants/{tenantName}/signals/{signalName", "testTenant", "abc"),
        is("/bios/v1/tenants/testTenant/signals/abc"));
    assertThat(
        StringUtils.generatePath(
            "/bios/v1/tenants/{tenantName/signals/{signalName}", "testTenant", "abc"),
        is("/bios/v1/tenants/testTenant"));
  }
}
