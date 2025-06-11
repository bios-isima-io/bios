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

public class MultiplicationOperationTest {
  private MultiplicationOperation multiplicationOperation;

  @Before
  public void setUp() {
    multiplicationOperation = new MultiplicationOperation();
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidMultiplicationOperation() throws Exception {
    Double respectForSupremeLeader = 100.0;
    Double respectIncreaseFactor = 2.0;
    Object result = multiplicationOperation.apply(respectForSupremeLeader, respectIncreaseFactor);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(respectForSupremeLeader * respectIncreaseFactor, result);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionOnInvalidMultiplicationOperation() throws Exception {
    String americanLeader = "donaldtrump";
    Double respectIncreaseFactor = 2.0;
    Object result = multiplicationOperation.apply(americanLeader, respectIncreaseFactor);
  }
}
