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
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.TestUtils;
import io.isima.bios.data.payload.CsvParser;
import io.isima.bios.models.AttributeValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ServerRecordTest {

  private static StreamDesc testSignal;
  private static Map<String, ColumnDefinition> definitions;

  /** Setup test signal and build the column definitions for it. */
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
  public void testEquals() throws Exception {
    ServerRecord record1 = new ServerRecord(definitions);
    ServerRecord record2 = new ServerRecord(definitions);
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record1.asEvent(true));
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record2.asEvent(true));

    assertTrue(
        record1.getAttribute("stringAttribute").equals(record2.getAttribute("stringAttribute")));
    assertTrue(
        record1.getAttribute("integerAttribute").equals(record2.getAttribute("integerAttribute")));
    assertTrue(
        record1.getAttribute("decimalAttribute").equals(record2.getAttribute("decimalAttribute")));
    assertTrue(
        record1.getAttribute("booleanAttribute").equals(record2.getAttribute("booleanAttribute")));
    assertTrue(record1.getAttribute("blobAttribute").equals(record2.getAttribute("blobAttribute")));
  }

  @Test
  public void testNotEquals() throws Exception {
    ServerRecord record1 = new ServerRecord(definitions);
    ServerRecord record2 = new ServerRecord(definitions);
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record1.asEvent(true));
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record2.asEvent(true));

    assertFalse(
        record1
            .getAttribute("stringAttribute")
            .equals(record2.getAttribute("stringAttributeWithDefault")));
    assertFalse(
        record1
            .getAttribute("integerAttribute")
            .equals(record2.getAttribute("integerAttributeWithDefault")));
    assertFalse(
        record1
            .getAttribute("decimalAttribute")
            .equals(record2.getAttribute("decimalAttributeWithDefault")));
    assertFalse(
        record1
            .getAttribute("booleanAttribute")
            .equals(record2.getAttribute("booleanAttributeWithDefault")));
    assertFalse(
        record1
            .getAttribute("blobAttribute")
            .equals(record2.getAttribute("blobAttributeWithDefault")));

    assertFalse(
        record1.getAttribute("stringAttribute").equals(record2.getAttribute("integerAttribute")));
  }

  @Test
  public void testHashEquals() throws Exception {
    ServerRecord record1 = new ServerRecord(definitions);
    ServerRecord record2 = new ServerRecord(definitions);
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record1.asEvent(true));
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record2.asEvent(true));

    assertThat(
        record1.getAttribute("stringAttribute").hashCode(),
        is(record2.getAttribute("stringAttribute").hashCode()));
    assertThat(
        record1.getAttribute("integerAttribute").hashCode(),
        is(record2.getAttribute("integerAttribute").hashCode()));
    assertThat(
        record1.getAttribute("integerAttribute").hashCode(),
        is(record2.getAttribute("integerAttribute").hashCode()));
    assertThat(
        record1.getAttribute("decimalAttribute").hashCode(),
        is(record2.getAttribute("decimalAttribute").hashCode()));
    assertThat(
        record1.getAttribute("booleanAttribute").hashCode(),
        is(record2.getAttribute("booleanAttribute").hashCode()));
  }

  @Test
  public void testHashNotEquals() throws Exception {
    ServerRecord record1 = new ServerRecord(definitions);
    ServerRecord record2 = new ServerRecord(definitions);
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record1.asEvent(true));
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record2.asEvent(true));

    assertThat(
        record1.getAttribute("stringAttribute").hashCode(),
        is(not(record2.getAttribute("stringAttributeWithDefault").hashCode())));
    assertThat(
        record1.getAttribute("integerAttribute").hashCode(),
        is(not(record2.getAttribute("integerAttributeWithDefault").hashCode())));
    assertThat(
        record1.getAttribute("integerAttribute").hashCode(),
        is(not(record2.getAttribute("integerAttributeWithDefault").hashCode())));
    assertThat(
        record1.getAttribute("decimalAttribute").hashCode(),
        is(not(record2.getAttribute("decimalAttributeWithDefault").hashCode())));
    assertThat(
        record1.getAttribute("booleanAttribute").hashCode(),
        is(not(record2.getAttribute("booleanAttributeWithDefault").hashCode())));
  }

  @Test
  public void mapKeyTest() throws Exception {
    final Map<List<AttributeValue>, String> lookup = new HashMap<>();

    ServerRecord record1 = new ServerRecord(definitions);
    ServerRecord record2 = new ServerRecord(definitions);
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record1.asEvent(true));
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record2.asEvent(true));

    // populate the lookup table by using the record1 for keys.
    lookup.put(
        List.of(record1.getAttribute("stringAttribute"), record1.getAttribute("integerAttribute")),
        "stringAndInteger");
    lookup.put(
        List.of(record1.getAttribute("decimalAttribute"), record1.getAttribute("booleanAttribute")),
        "decimalAndBoolean");
    lookup.put(List.of(record1.getAttribute("stringAttribute")), "stringOnly");

    // try getting the entries by using the record2 for keys
    assertThat(
        lookup.get(
            List.of(
                record2.getAttribute("stringAttribute"), record2.getAttribute("integerAttribute"))),
        is("stringAndInteger"));

    assertThat(
        lookup.get(
            List.of(
                record2.getAttribute("decimalAttribute"),
                record2.getAttribute("booleanAttribute"))),
        is("decimalAndBoolean"));

    assertThat(lookup.get(List.of(record2.getAttribute("stringAttribute"))), is("stringOnly"));

    assertNull(
        lookup.get(
            List.of(
                record2.getAttribute("decimalAttributeWithDefault"),
                record2.getAttribute("booleanAttribute"))));
  }
}
