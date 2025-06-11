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
package io.isima.bios.proto.isql;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.codec.proto.isql.ProtoBuilderProvider;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.isql.ISqlBuilderProvider;
import io.isima.bios.models.isql.ISqlStatement;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Test;

public class InsertBuilderTest {
  @AfterClass
  public static void resetBuilderProvider() {
    ISqlBuilderProvider.reset();
  }

  /** Parameterized constructor to run both proto and json. */
  public InsertBuilderTest() {
    ProtoBuilderProvider.configureProtoBuilderProvider();
  }

  @Test
  public void testBasicInsert() {
    var insertStmt = ISqlStatement.insert().into("s1").csv("123").build();
    assertFalse(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s1"));
    assertThat(insertStmt.getCsv(), is("123"));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsvBulk());
  }

  @Test
  public void testBasicInsertBulk() {
    var insertStmt =
        ISqlStatement.insert().into("s2").csvBulk(Arrays.asList("123", "456", "789")).build();
    assertTrue(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s2"));
    assertThat(insertStmt.getCsvBulk(), is(Arrays.asList("123", "456", "789")));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsv());
  }

  @Test
  public void testBasicInsertBulkArray() {
    var insertStmt = ISqlStatement.insert().into("s3").csvBulk("12,3", "45,6", "78,9").build();
    assertTrue(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s3"));
    assertThat(insertStmt.getCsvBulk(), is(Arrays.asList("12,3", "45,6", "78,9")));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsv());
  }

  @Test
  public void testValuesInsert() {
    var insertStmt =
        ISqlStatement.insert().into("s1").values(123L, "Hello, world!", "You said\"Hi\"").build();
    assertFalse(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s1"));
    assertThat(insertStmt.getCsv(), is("123,\"Hello, world!\",\"You said\"\"Hi\"\"\""));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsvBulk());
  }

  @Test
  public void testValuesInsert2() {
    final Object[] values = new Object[] {123L, "Hello, world!", "You said\"Hi\""};
    var insertStmt = ISqlStatement.insert().into("s1").values(values).build();
    assertFalse(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s1"));
    assertThat(insertStmt.getCsv(), is("123,\"Hello, world!\",\"You said\"\"Hi\"\"\""));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsvBulk());
  }

  @Test
  public void testValuesListInsert() {
    final List<Object> values1 = List.of(123L, "Hello, world!", "You said\"Hi\"");
    final List<Object> values2 = List.of(Boolean.TRUE, 1.23, "Abc");
    var insertStmt =
        ISqlStatement.insert().into("s1").valuesBulk(List.of(values1, values2)).build();
    assertTrue(insertStmt.isBulk());
    assertThat(insertStmt.getSignalName(), is("s1"));
    assertThat(insertStmt.getCsvBulk().get(0), is("123,\"Hello, world!\",\"You said\"\"Hi\"\"\""));
    assertThat(insertStmt.getCsvBulk().get(1), is("true,1.23,Abc"));
    assertThat(insertStmt.getContentRepresentation(), is(ContentRepresentation.CSV));
    assertNull(insertStmt.getCsv());
  }

  @Test
  public void testBasicInsertMissingTarget() {
    try {
      ISqlStatement.insert().into("").csv("hello,world").build();
      fail("Not a buildable statement");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("into"));
    }
  }

  @Test
  public void testBasicInsertMissingValue() {
    try {
      ISqlStatement.insert().into("aSignal").csv(null).build();
      fail("Not a buildable statement");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("csv"));
    }
  }

  @Test
  public void testBasicInsertBulkMissingTarget() {
    try {
      ISqlStatement.insert().into(null).csvBulk(Arrays.asList("123", null, "789")).build();
      fail("Not a buildable statement");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("into"));
    }
  }

  @Test
  public void testBasicInsertBulkMissingCsvEntry() {
    try {
      ISqlStatement.insert().into("whatever").csvBulk(Arrays.asList("123", null, "789")).build();
      fail("Not a buildable statement");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("csvBulk[1]"));
    }
  }
}
