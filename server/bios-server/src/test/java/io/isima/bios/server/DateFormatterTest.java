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
package io.isima.bios.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.LocalDateTime;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateFormatterTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testFormatter() {
    final String out = Http2Utils.toRfc1123Date(LocalDateTime.of(2020, 12, 3, 5, 6, 7));
    assertThat(out, is("Thu, 03 Dec 2020 05:06:07 GMT"));
  }

  @Test
  public void testFormattingCurrentTime() {
    final String out = Http2Utils.toRfc1123Date(LocalDateTime.now());
    Pattern p = Pattern.compile("^\\D{3}, \\d{2} \\D{3} \\d{4} \\d{2}:\\d{2}:\\d{2} GMT$");
    // assertThat(out, matchesPattern(p));
  }
}
