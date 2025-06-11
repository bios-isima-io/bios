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

public class AdditionOperationTest {
  private AdditionOperation additionOperation;

  @Before
  public void setUp() {
    additionOperation = new AdditionOperation();
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidAdditionOperation() throws Exception {
    Double respectForSupremeLeader = 100.0;
    Double respectAddition = 200.0;
    Object result = additionOperation.apply(respectForSupremeLeader, respectAddition);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof Double);
    Assert.assertEquals(respectForSupremeLeader + respectAddition, result);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionOnInvalidAdditionOperation() throws Exception {
    String americanLeader = "donaldtrump";
    Double respectAddition = 0.001;
    Object result = additionOperation.apply(americanLeader, respectAddition);
  }
}
