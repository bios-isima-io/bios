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
package io.isima.bios.preprocess;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParseCsvPerformanceTest {

  private static List<AttributeDesc> attributes;
  private static List<String> sources;

  private static int numEvents;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    attributes = new ArrayList<>();
    AttributeDesc desc = new AttributeDesc("integer", InternalAttributeType.LONG);
    attributes.add(desc);
    desc = new AttributeDesc("decimal", InternalAttributeType.DOUBLE);
    attributes.add(desc);
    desc = new AttributeDesc("string", InternalAttributeType.STRING);
    attributes.add(desc);
    desc = new AttributeDesc("boolean", InternalAttributeType.BOOLEAN);
    attributes.add(desc);
    desc = new AttributeDesc("blob", InternalAttributeType.BLOB);
    attributes.add(desc);

    sources = new ArrayList<>();
    final var random = new Random();
    numEvents = 100000;
    for (int i = 0; i < numEvents; ++i) {
      final var sb = new StringBuilder();
      sb.append(random.nextLong()).append(",").append(random.nextDouble() * 1.e5).append(",");
      for (int j = 0; j < 32; ++j) {
        sb.append((char) (random.nextDouble() * 26) + 'a');
      }
      sb.append(",").append(random.nextBoolean());
      byte[] bytes = new byte[16];
      random.nextBytes(bytes);
      sb.append(",").append(new String(Base64.getEncoder().encode(bytes)));
      final String src = sb.toString();
      sources.add(src);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testDefault() throws TfosException {
    runParser(
        "Default",
        (src) -> {
          return new DefaultEventCsvParser(src, attributes, false);
        });
  }

  @Test
  public void testOpenCsv() throws TfosException {
    runParser(
        "OpenCSV",
        (src) -> {
          try {
            return new OpenCsvEventCsvParser(src, attributes, false);
          } catch (TfosException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testSuperCsv() throws TfosException {
    runParser(
        "SuperCSV",
        (src) -> {
          try {
            return new SuperCsvEventCsvParser(src, attributes, false);
          } catch (TfosException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testApache() throws TfosException {
    runParser(
        "Apache",
        (src) -> {
          try {
            return new ApacheEventCsvParser(src, attributes, false);
          } catch (TfosException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private List<Map<String, Object>> runParser(
      String parserName, Function<String, EventCsvParser> getParser) throws TfosException {
    final var out = new ArrayList<Map<String, Object>>();
    final var start = System.nanoTime();
    for (String src : sources) {
      final var parser = getParser.apply(src);
      out.add(parser.parse());
    }
    final var end = System.nanoTime();
    System.out.println(
        String.format("Parser: %s, elapsed: %f msec", parserName, (end - start) / 1000000.0));
    return out;
  }
}
