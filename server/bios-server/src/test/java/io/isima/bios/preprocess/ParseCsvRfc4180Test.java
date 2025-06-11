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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParseCsvRfc4180Test {

  private static List<AttributeDesc> attributes;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    attributes = new ArrayList<>();
    AttributeDesc desc = new AttributeDesc("integer", InternalAttributeType.LONG);
    attributes.add(desc);
    desc = new AttributeDesc("decimal", InternalAttributeType.DOUBLE);
    attributes.add(desc);
    desc = new AttributeDesc("string1", InternalAttributeType.STRING);
    attributes.add(desc);
    desc = new AttributeDesc("string2", InternalAttributeType.STRING);
    attributes.add(desc);
    desc = new AttributeDesc("boolean", InternalAttributeType.BOOLEAN);
    attributes.add(desc);
    // TODO: Run blob test, too
    // desc = new AttributeDesc("blob", InternalAttributeType.BLOB);
    // attributes.add(desc);
  }

  @Test
  public void testFundamental() throws TfosException {
    final String src = "123,1.23,hello world,abcde,true";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyFundamental(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyFundamental(parserApache);
  }

  private void verifyFundamental(EventCsvParser parser) throws TfosException {
    final var attributes = parser.parse();
    assertEquals(Long.valueOf(123L), attributes.get("integer"));
    assertEquals(Double.valueOf(1.23), attributes.get("decimal"));
    assertEquals("hello world", attributes.get("string1"));
    assertEquals("abcde", attributes.get("string2"));
    assertEquals(Boolean.TRUE, attributes.get("boolean"));
  }

  @Test
  public void testDquoteSimple() throws TfosException {
    final String src = "\"123\",\"1.23\",\"hello,world\",\" s p a c e s \",\"true\"";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyDquoteSimple(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyDquoteSimple(parserApache);
  }

  private void verifyDquoteSimple(EventCsvParser parser) throws TfosException {
    final var attributes = parser.parse();
    assertEquals(Long.valueOf(123L), attributes.get("integer"));
    assertEquals(Double.valueOf(1.23), attributes.get("decimal"));
    assertEquals("hello,world", attributes.get("string1"));
    assertEquals(" s p a c e s ", attributes.get("string2"));
    assertEquals(Boolean.TRUE, attributes.get("boolean"));
  }

  @Test
  public void testDquoteNewLines() throws TfosException {
    final String src = "\"123\",\"1.23\",\"hello,\r\n world\",\"\",\"true\"";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyDquoteNewLines(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyDquoteNewLines(parserApache);
  }

  private void verifyDquoteNewLines(EventCsvParser parser) throws TfosException {
    final var attributes = parser.parse();
    assertEquals(Long.valueOf(123L), attributes.get("integer"));
    assertEquals(Double.valueOf(1.23), attributes.get("decimal"));
    assertEquals("hello,\r\n world", attributes.get("string1"));
    assertEquals("", attributes.get("string2"));
    assertEquals(Boolean.TRUE, attributes.get("boolean"));
  }

  @Test
  public void testEscapeDquotes() throws TfosException {
    final String src = "123,1.23,\"Julian \"\"Cannonball\"\" Adderley\",\"another\",true";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyEscapeDquotes(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyEscapeDquotes(parserApache);
  }

  private void verifyEscapeDquotes(EventCsvParser parser) throws TfosException {
    final var attributes = parser.parse();
    assertEquals(Long.valueOf(123L), attributes.get("integer"));
    assertEquals(Double.valueOf(1.23), attributes.get("decimal"));
    assertEquals("Julian \"Cannonball\" Adderley", attributes.get("string1"));
    assertEquals("another", attributes.get("string2"));
    assertEquals(Boolean.TRUE, attributes.get("boolean"));
  }

  @Test
  public void testDquotesInMiddle() throws TfosException {
    final String src = "123,1.23,Julian \"Cannonball\" Adderley, \"another\" ,true";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyDquotesInMiddle(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyDquotesInMiddle(parserApache);
  }

  private void verifyDquotesInMiddle(EventCsvParser parser) throws TfosException {
    final var attributes = parser.parse();
    assertEquals(Long.valueOf(123L), attributes.get("integer"));
    assertEquals(Double.valueOf(1.23), attributes.get("decimal"));
    assertEquals("Julian \"Cannonball\" Adderley", attributes.get("string1"));
    assertEquals(" \"another\" ", attributes.get("string2"));
    assertEquals(Boolean.TRUE, attributes.get("boolean"));
  }

  @Test
  public void testDquotesNotClose() throws TfosException {
    final String src = "123,1.23,Julian \"Cannonball\" Adderley,\"another,true";
    final var parserDefault = new DefaultEventCsvParser(src, attributes, false);
    verifyDquotesNotClosing(parserDefault);
    final var parserApache = new ApacheEventCsvParser(src, attributes, false);
    verifyDquotesNotClosing(parserApache);
  }

  private void verifyDquotesNotClosing(EventCsvParser parser) throws TfosException {
    try {
      parser.parse();
      fail("Exception is expected");
    } catch (TfosException e) {
      assertEquals(EventIngestError.CSV_SYNTAX_ERROR.getErrorCode(), e.getErrorCode());
    }
  }
}
