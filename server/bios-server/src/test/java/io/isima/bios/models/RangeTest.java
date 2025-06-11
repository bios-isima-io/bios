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
package io.isima.bios.models;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RangeTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testNonOverlap() {
    final var range1 = new Range(10L, 20L);
    final var range2 = new Range(15L, 18L);
    assertTrue(range1.includes(range2));
    assertFalse(range2.includes(range1));
    assertFalse(range1.overlaps(range2));
    assertFalse(range2.overlaps(range1));
  }

  @Test
  public void testCompleteMatch() {
    final var range1 = new Range(10L, 20L);
    final var range2 = new Range(10L, 20L);
    assertTrue(range1.includes(range2));
    assertTrue(range2.includes(range1));
    assertFalse(range1.overlaps(range2));
    assertFalse(range2.overlaps(range1));
  }

  @Test
  public void testOverlap() {
    final var range1 = new Range(10L, 20L);
    final var range2 = new Range(15L, 25L);
    assertFalse(range1.includes(range2));
    assertFalse(range2.includes(range1));
    assertTrue(range1.overlaps(range2));
    assertTrue(range2.overlaps(range1));
  }

  @Test
  public void testInstantIncludeCheck() {
    final var range = new Range(1609268753799L, 1609268796135L);
    assertFalse(range.includes(160926875369L));
    assertFalse(range.includes(1609268753798L));
    assertTrue(range.includes(1609268753799L));
    assertTrue(range.includes(1609268796134L));
    assertFalse(range.includes(1609268796135L));
    assertFalse(range.includes(1609268828693L));
  }
}
