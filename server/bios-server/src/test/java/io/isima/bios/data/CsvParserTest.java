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
package io.isima.bios.data;

import static io.isima.bios.common.TestUtils.SIMPLE_ALL_TYPES_SIGNAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.TestUtils;
import io.isima.bios.data.payload.CsvParser;
import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CsvParserTest {

  private static StreamDesc testSignal;
  private static Map<String, ColumnDefinition> definitions;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testSignal = TestUtils.loadStreamJson(SIMPLE_ALL_TYPES_SIGNAL);
    definitions =
        new ColumnDefinitionsBuilder().addAttributes(testSignal.getAllBiosAttributes()).build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testFundamentalCsvParser() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
    assertThat(record.attributes().size(), is(12));
    assertThat(record.getAttribute("stringAttribute").asString(), is("hello"));
    assertThat(record.getAttribute("integerAttribute").asLong(), is(6507226550L));
    assertThat(record.getAttribute("decimalAttribute").asDouble(), is(2.3));
    assertThat(record.getAttribute("booleanAttribute").asBoolean(), is(true));
    assertThat(new String(record.getAttribute("blobAttribute").asByteArray()), is("Isima biOS 2"));
    assertThat(record.getAttribute("enumAttribute").asString(), is("TWO"));

    assertThat(record.getAttribute("stringAttributeWithDefault").asString(), is("world"));
    assertThat(record.getAttribute("integerAttributeWithDefault").asLong(), is(2L));
    assertThat(record.getAttribute("decimalAttributeWithDefault").asDouble(), is(3.4));
    assertThat(record.getAttribute("booleanAttributeWithDefault").asBoolean(), is(false));
    assertThat(
        new String(record.getAttribute("blobAttributeWithDefault").asByteArray()),
        is("A long time ago in a galaxy far, far away"));
    assertThat(record.getAttribute("enumAttributeWithDefault").asString(), is("FOUR"));
  }

  @Test
  public void testCsvParserDefaultValues() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src = "hello,1,2.3,true,SXNpbWEgYmlPUyAy,TWO," + ",,,,,";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
    assertThat(record.attributes().size(), is(8));
    assertThat(record.getAttribute("stringAttribute").asString(), is("hello"));
    assertThat(record.getAttribute("integerAttribute").asLong(), is(1L));
    assertThat(record.getAttribute("decimalAttribute").asDouble(), is(2.3));
    assertThat(record.getAttribute("booleanAttribute").asBoolean(), is(true));
    assertThat(new String(record.getAttribute("blobAttribute").asByteArray()), is("Isima biOS 2"));
    assertThat(record.getAttribute("enumAttribute").asString(), is("TWO"));

    // you can't set default string or blob by CSV
    assertThat(record.getAttribute("stringAttributeWithDefault").asString(), is(""));
    assertThat(new String(record.getAttribute("blobAttributeWithDefault").asByteArray()), is(""));

    // default values will be set in later stage by data engine
    assertNull(record.getAttribute("integerAttributeWithDefault"));
    assertNull(record.getAttribute("decimalAttributeWithDefault"));
    assertNull(record.getAttribute("booleanAttributeWithDefault"));
    assertNull(record.getAttribute("enumAttributeWithDefault"));
  }

  @Test
  public void testCsvParserTooFewAttributes() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,1,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=";
    try {
      CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
      fail("exception is expected");
    } catch (TfosException e) {
      assertThat(e.getErrorCode(), is(EventIngestError.TOO_FEW_VALUES.getErrorCode()));
    }
  }

  @Test
  public void testCsvParserTooManyAttributes() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,1,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR,FIVE";
    try {
      CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
      fail("exception is expected");
    } catch (TfosException e) {
      assertThat(e.getErrorCode(), is(EventIngestError.TOO_MANY_VALUES.getErrorCode()));
    }
  }

  @Test
  public void testCsvParserRfc4180Escapes() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "\"double\"\"quote\",1,2.3,true,SXNpbWEgYmlPUyAy,\"FIVE\","
            + "quotes\"in\"the\"middle,2,3.4,false,"
            + "QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
    assertThat(record.attributes().size(), is(12));
    assertThat(record.getAttribute("stringAttribute").asString(), is("double\"quote"));
    assertThat(record.getAttribute("enumAttribute").asString(), is("FIVE"));
    assertThat(
        record.getAttribute("stringAttributeWithDefault").asString(),
        is("quotes\"in\"the\"middle"));
  }

  @Test
  public void testCsvParserInvalidInteger() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,world,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    assertThrows(
        InvalidValueSyntaxException.class,
        () -> {
          CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
        });
  }

  @Test
  public void testCsvParserInvalidDecimal() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,10,invalid,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    assertThrows(
        InvalidValueSyntaxException.class,
        () -> {
          CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
        });
  }
}
