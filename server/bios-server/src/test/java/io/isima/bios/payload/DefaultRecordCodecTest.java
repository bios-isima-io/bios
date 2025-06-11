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
package io.isima.bios.payload;

import static io.isima.bios.common.TestUtils.SIMPLE_ALL_TYPES_SIGNAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.TestUtils;
import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.ColumnDefinitionsBuilder;
import io.isima.bios.data.ServerRecord;
import io.isima.bios.data.payload.CsvParser;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DefaultRecordCodecTest {

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
  public void testFundamental() throws Exception {
    ServerRecord record = new ServerRecord(definitions);
    assertThat(record.attributes().size(), is(0));
    assertTrue(record.attributes().isEmpty());
    final String src =
        "hello,6507226550,2.3,true,SXNpbWEgYmlPUyAy,TWO,"
            + "world,2,3.4,false,QSBsb25nIHRpbWUgYWdvIGluIGEgZ2FsYXh5IGZhciwgZmFyIGF3YXk=,FOUR";
    CsvParser.parse(src, testSignal.getAllBiosAttributes(), record.asEvent(true));
    final var srcEventId = UUID.randomUUID();
    final var timestamp = System.currentTimeMillis();
    record.setEventId(srcEventId);
    record.setTimestamp(timestamp);

    assertThat(record.getAttribute("stringAttribute").asString(), is("hello"));
    assertThat(record.getAttribute("integerAttribute").asLong(), is(6507226550L));
    assertThat(record.getAttribute("decimalAttribute").asDouble(), is(2.3));
    assertThat(record.getAttribute("booleanAttribute").asBoolean(), is(true));
    assertThat(record.getAttribute("blobAttribute").asByteArray(), is("Isima biOS 2".getBytes()));
    assertThat(record.getAttribute("enumAttribute").asString(), is("TWO"));
  }
}
