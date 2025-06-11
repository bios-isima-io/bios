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

public class EqualOperationTest {
  private final EqualOperation equalOperation = new EqualOperation();

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testEqualOperationNumericOperandsPositive() throws Exception {
    Double first = 100.0;
    Double second = 100.0;
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.TRUE, result);
  }

  @Test
  public void testEqualOperationNumericOperandsNegative() throws Exception {
    Double first = 100.0;
    Double second = 200.0;
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.FALSE, result);
  }

  @Test
  public void testEqualOperationBooleanOperandsPositive() throws Exception {
    Boolean first = true;
    Boolean second = Boolean.TRUE;
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.TRUE, result);
  }

  @Test
  public void testEqualOperationBooleanOperandsNegative() throws Exception {
    Boolean first = true;
    Boolean second = Boolean.FALSE;
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.FALSE, result);
  }

  @Test
  public void testEqualOperationStringOperandsPositive() throws Exception {
    String first = "dummy";
    String second = "dummy";
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.TRUE, result);
  }

  @Test
  public void testEqualOperationStringOperandsNegative() throws Exception {
    String first = "dummy";
    String second = "ymmud";
    Object result = equalOperation.apply(first, second);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Boolean);
    Assert.assertEquals(Boolean.FALSE, result);
  }

  @Test
  public void testEqualOperationException() throws Exception {
    String first = "dummy";
    Object[] invalidValues =
        new Object[] {
          Double.parseDouble("100.0"),
          Integer.parseInt("100"),
          100.0,
          100,
          new Object(),
          true,
          Boolean.FALSE
        };
    for (Object invalidValue : invalidValues) {
      try {
        equalOperation.apply(first, invalidValue);
        Assert.fail("UnexpectedValueException was expected for: " + invalidValue);
      } catch (UnexpectedValueException e) {
        // exception is expected
      }
    }
  }
}
