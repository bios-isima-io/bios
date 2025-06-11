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
package io.isima.bios.bi.teachbios;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.isima.bios.dto.teachbios.LearningData;
import io.isima.bios.errors.TeachBiosError;
import io.isima.bios.errors.exception.TfosException;
import java.util.List;
import org.hamcrest.junit.ExpectedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TeachBiosCsvParserTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private TeachBios handler;

  @Before
  public void setUp() throws Exception {
    handler = new TeachBios(null, null);
  }

  @Test
  public void testFundamental() throws TfosException {
    final String src = "abc,123,true,1.23\ndef,456,false,4.56\nghi,789,true,7.89";
    final var out = handler.parseInput(new LearningData(src));
    assertEquals(
        List.of(
            List.of("abc", "def", "ghi"),
            List.of("123", "456", "789"),
            List.of("true", "false", "true"),
            List.of("1.23", "4.56", "7.89")),
        out);
  }

  @Test
  public void testRfc4180() throws TfosException {
    final String src = "\"hello, world\",123\n\"abc \"\"def\"\" ghi\",456";
    final var out = handler.parseInput(new LearningData(src));
    assertEquals(List.of(List.of("hello, world", "abc \"def\" ghi"), List.of("123", "456")), out);
  }

  @Test
  public void testSyntaxError() throws TfosException {
    final String src = "\"hello, world\",123\n\"abc \"\"def\"\" ghi,456";
    try {
      handler.parseInput(new LearningData(src));
      fail("Exception is expected");
    } catch (TfosException e) {
      assertEquals(TeachBiosError.INVALID_INPUT.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void inconsistentNumColumns() throws TfosException {
    final String src = "abc,123,true,1.23\ndef,456,false\nghi,789,true,7.89";
    try {
      handler.parseInput(new LearningData(src));
      fail("Exception is expected");
    } catch (TfosException e) {
      assertEquals(TeachBiosError.INVALID_INPUT.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void emptyInput() throws TfosException {
    final String src = "";
    try {
      handler.parseInput(new LearningData(src));
      fail("Exception is expected");
    } catch (TfosException e) {
      assertEquals(TeachBiosError.EMPTY_INPUT_NOW_ALLOWED.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void emptyColumns() throws TfosException {
    final String src = "abc,123,,1.23\ndef,456,false,4.56\nghi,789,true,7.89";
    try {
      handler.parseInput(new LearningData(src));
      fail("Exception is expected");
    } catch (TfosException e) {
      assertEquals(TeachBiosError.EMPTY_INPUT_NOW_ALLOWED.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void tooManyRows() throws TfosException {
    final var sb = new StringBuilder("0");
    for (int i = 1; i < 1000; ++i) {
      sb.append("\n").append(Integer.toString(i));
    }
    // OK
    handler.parseInput(new LearningData(sb.toString()));

    sb.append("\n1001");
    try {
      handler.parseInput(new LearningData(sb.toString()));
    } catch (TfosException e) {
      assertEquals(TeachBiosError.MAX_ROW_LIMIT_EXCEEDED.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void tooManyColumns() throws TfosException {
    final var sb = new StringBuilder("0");
    for (int i = 1; i < 1000; ++i) {
      sb.append(",").append(Integer.toString(i));
    }
    // OK
    handler.parseInput(new LearningData(sb.toString()));

    sb.append(",1001");
    try {
      handler.parseInput(new LearningData(sb.toString()));
    } catch (TfosException e) {
      assertEquals(TeachBiosError.MAX_COLUMN_LIMIT_EXCEEDED.getErrorCode(), e.getErrorCode());
    }
  }
}
