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
package io.isima.bios.service;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CorsOriginWhiteListTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testAllowAll() {
    final var whiteList = new CorsOriginWhiteList(new String[] {"*"});
    assertTrue(whiteList.isAllowed("any"));
    assertTrue(whiteList.isAllowed("Thig"));
    assertTrue(whiteList.isAllowed("would be allowed"));
    assertTrue(whiteList.isAllowed("EveN"));
    assertTrue(whiteList.isAllowed(null));
  }

  @Test
  public void testAllowAll2() {
    final var whiteList = new CorsOriginWhiteList(new String[] {"hello", "*", "world"});
    assertTrue(whiteList.isAllowed("any"));
    assertTrue(whiteList.isAllowed("Thig"));
    assertTrue(whiteList.isAllowed("would.be.allowed"));
    assertTrue(whiteList.isAllowed("EveN"));
    assertTrue(whiteList.isAllowed(null));
  }

  @Test
  public void testAllowSpecificSites() {
    final var whiteList = new CorsOriginWhiteList(new String[] {"site.a", "dev.isima.io"});
    assertTrue(whiteList.isAllowed("site.a"));
    assertTrue(whiteList.isAllowed("dev.isima.io"));

    assertFalse(whiteList.isAllowed("site.b"));
    assertFalse(whiteList.isAllowed("isima.io"));
    assertFalse(whiteList.isAllowed("dev.isima.io.x"));
    assertFalse(whiteList.isAllowed(null));
  }
}
