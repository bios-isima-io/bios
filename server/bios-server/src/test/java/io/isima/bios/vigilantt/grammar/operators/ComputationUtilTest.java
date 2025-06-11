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
import io.isima.bios.vigilantt.grammar.combiners.AndComposition;
import io.isima.bios.vigilantt.grammar.utils.ComputationUtil;
import org.junit.Assert;
import org.junit.Test;

public class ComputationUtilTest {

  @Test
  public void testGetValidValueAsDouble() throws Exception {
    Object[] validValues = new Object[] {12.32, 34.21};
    for (Object validValue : validValues) {
      Double convertedValue = ComputationUtil.getValueAsDouble(validValue);
      Assert.assertNotNull(convertedValue);
    }
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValueAsDouble() throws Exception {
    Object[] invalidValues =
        new Object[] {"jsndfj", new AndComposition(), OperationFactory.getOperation("==")};
    for (Object invalidValue : invalidValues) {
      ComputationUtil.getValueAsDouble(invalidValue);
    }
  }

  @Test
  public void testGetValidValueAsBooleanTest() throws Exception {
    Object[] validValues =
        new Object[] {false, true, "False", "True", "FALSE", "TRUE", "false", "true"};
    Boolean[] expectedBooleans = new Boolean[] {false, true, false, true, false, true, false, true};
    for (int i = 0; i < validValues.length; i++) {
      Boolean convertedValue = ComputationUtil.getValueAsBoolean(validValues[i]);
      Assert.assertEquals(expectedBooleans[i], convertedValue);
    }
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValueAsBooleanTest1() throws Exception {
    ComputationUtil.getValueAsBoolean("randomString");
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValueAsBooleanTest2() throws Exception {
    ComputationUtil.getValueAsBoolean(1);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValueAsBooleanTest3() throws Exception {
    ComputationUtil.getValueAsBoolean(23.43);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValueAsBooleanTest4() throws Exception {
    ComputationUtil.getValueAsBoolean(new Object());
  }

  @Test
  public void testGetValidValuesAsString() throws Exception {
    Object[] validValues = new Object[] {"hello", "world"};
    for (Object testValue : validValues) {
      String convertedValue = ComputationUtil.getValueAsString(testValue);
      Assert.assertNotNull(convertedValue);
    }
  }

  @Test(expected = UnexpectedValueException.class)
  public void testGetInvalidValuesAsString() throws Exception {
    Object[] invalidValues = new Object[] {100, new Object()};
    for (Object testValue : invalidValues) {
      ComputationUtil.getValueAsString(testValue);
    }
  }

  @Test
  public void isConvertibleToDouble() throws Exception {
    Object[] validDoubles = new Object[] {23, 32.12};
    for (Object validValue : validDoubles) {
      Assert.assertTrue(ComputationUtil.isConvertibleToDouble(validValue));
    }

    Object[] invalidDoubles = new Object[] {"hello", new Object()};
    for (Object invalidValue : invalidDoubles) {
      Assert.assertFalse(ComputationUtil.isConvertibleToDouble(invalidValue));
    }

    String[] validDoubleStrings = new String[] {"12.32", "34"};
    for (String validValue : validDoubleStrings) {
      Assert.assertTrue(ComputationUtil.isConvertibleToDouble(validValue));
    }

    String[] invalidDoubleStrings = new String[] {"hello", "34%"};
    for (String invalidValue : invalidDoubleStrings) {
      Assert.assertFalse(ComputationUtil.isConvertibleToDouble(invalidValue));
    }
  }

  @Test
  public void isConvertibleToBoolean() throws Exception {
    Object[] validBooleans = new Object[] {Boolean.TRUE, Boolean.FALSE, true, false};
    for (Object validValue : validBooleans) {
      Assert.assertTrue(validValue.toString(), ComputationUtil.isConvertibleToBoolean(validValue));
    }

    Object[] invalidBooleans = new Object[] {"hello", new Object(), "true", "_false", "yes", 1, 25};
    for (Object invalidValue : invalidBooleans) {
      Assert.assertFalse(
          invalidValue.toString(), ComputationUtil.isConvertibleToBoolean(invalidValue));
    }

    String[] validBooleanStrings = new String[] {"true", "false", "True", "False", "TRUE", "FALSE"};
    for (String validValue : validBooleanStrings) {
      Assert.assertTrue(validValue, ComputationUtil.isConvertibleToBoolean(validValue));
    }

    String[] invalidBooleanStrings = new String[] {"hello", "34%", "true1", "_false", "yes", "1"};
    for (String invalidValue : invalidBooleanStrings) {
      Assert.assertFalse(invalidValue, ComputationUtil.isConvertibleToBoolean(invalidValue));
    }
  }
}
