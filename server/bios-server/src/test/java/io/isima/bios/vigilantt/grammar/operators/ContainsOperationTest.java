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

public class ContainsOperationTest {
  private final ContainsOperation containsOperation = new ContainsOperation();

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testContainsOperationPositiveCases() throws Exception {
    String[] positiveStrings =
        new String[] {
          "kimjongun is god",
          "is god kimjongun?",
          "god is kimjongun",
          "kimjongun is god of the whole fricking world",
          "all hail kimjongun - the undisputed god"
        };
    String god = "god";
    for (String positiveString : positiveStrings) {
      Object result = containsOperation.apply(positiveString, god);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof Boolean);
      Assert.assertEquals(Boolean.TRUE, result);
    }
  }

  @Test
  public void testContainsOperationNegativeCases() throws Exception {
    String[] negativeStrings =
        new String[] {
          "donaldtrump is normal person",
          "north korea is paradise",
          "apples are indeed oranges",
          "missiles over food",
        };
    String god = "god";
    for (String negativeString : negativeStrings) {
      Object result = containsOperation.apply(negativeString, god);
      Assert.assertNotNull(result);
      Assert.assertTrue(result instanceof Boolean);
      Assert.assertEquals(Boolean.FALSE, result);
    }
  }

  @Test(expected = UnexpectedValueException.class)
  public void testContainsOperationInvalidOperands() throws Exception {
    Object[] invalidOperands = new Object[] {100, new Object(), Double.parseDouble("120.10")};
    String validValue = "dummy";
    for (Object invalidValue : invalidOperands) {
      containsOperation.apply(validValue, invalidValue);
      containsOperation.apply(invalidValue, validValue);
    }
  }
}
