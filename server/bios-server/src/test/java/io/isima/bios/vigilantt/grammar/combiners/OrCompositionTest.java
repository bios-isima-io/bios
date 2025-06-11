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
package io.isima.bios.vigilantt.grammar.combiners;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrCompositionTest {
  private OrComposition orComposition;

  @Before
  public void setUp() {
    orComposition = new OrComposition();
  }

  @After
  public void tearDown() {}

  @Test
  public void testOrCompositionBothTrue() throws Exception {
    Object result = orComposition.apply(Boolean.TRUE, Boolean.TRUE);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testOrCompositionFirstTrue() throws Exception {
    Object result = orComposition.apply(Boolean.TRUE, Boolean.FALSE);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testOrCompositionSecondTrue() throws Exception {
    Object result = orComposition.apply(Boolean.FALSE, Boolean.TRUE);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testOrCompositionBothFalse() throws Exception {
    Object result = orComposition.apply(Boolean.FALSE, Boolean.FALSE);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertFalse((Boolean) result);
  }
}
