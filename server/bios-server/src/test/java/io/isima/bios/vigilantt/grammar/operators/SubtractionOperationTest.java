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
package io.isima.bios.vigilantt.grammar.operators;

import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SubtractionOperationTest {
  private SubtractionOperation subtractionOperation;

  @Before
  public void setUp() {
    subtractionOperation = new SubtractionOperation();
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidSubtractionOperation() throws Exception {
    Double respectForAmericanLeader = 100.0;
    Double respectSubtraction = 200.0;
    Object result = subtractionOperation.apply(respectForAmericanLeader, respectSubtraction);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(respectForAmericanLeader - respectSubtraction, result);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionOnInvalidAdditionOperation() throws Exception {
    String supremeLeader = "kimjongun";
    Double respectSubtraction = -0.001;
    Object result = subtractionOperation.apply(supremeLeader, respectSubtraction);
  }
}
