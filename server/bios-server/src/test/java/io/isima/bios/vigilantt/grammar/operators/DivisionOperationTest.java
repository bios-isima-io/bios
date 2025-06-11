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

public class DivisionOperationTest {
  private DivisionOperation divisionOperation;

  @Before
  public void setUp() {
    divisionOperation = new DivisionOperation();
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidDivisionOperation() throws Exception {
    Double respectForAmericanLeader = 100.0;
    Double respectDivisionFactor = 2.0;
    Object result = divisionOperation.apply(respectForAmericanLeader, respectDivisionFactor);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(respectForAmericanLeader / respectDivisionFactor, result);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionOnInvalidDivisionOperation() throws Exception {
    String supremeLeader = "kimjongun";
    Double respectDivisionFactor = 2.0;
    divisionOperation.apply(supremeLeader, respectDivisionFactor);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionOnDivisionByZero() throws Exception {
    Double positiveNumerator = 10.0;
    Double zero = 0.0;
    divisionOperation.apply(positiveNumerator, zero);

    Double negativeNumerator = -10.0;
    divisionOperation.apply(negativeNumerator, zero);
  }
}
