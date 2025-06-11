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
package io.isima.bios.vigilantt.validators;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExampleRuleValidatorTest {
  private final RuleValidator ruleValidator = new ExampleRuleValidator();

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testSimpleValidRule() {
    String[] validRules = new String[] {"cpu_usage > 40", "disk_usage >20", "latency> 10"};
    for (String rule : validRules) {
      boolean isValid = ruleValidator.isValid(rule);
      Assert.assertTrue(isValid);
    }
  }

  @Test
  public void testInvalidRule() {
    String[] invalidRules =
        new String[] {
          "cpu_usage = 40", "something_is_not_right_here", "somemetric <> 32", "apples = oranges"
        };
    for (String rule : invalidRules) {
      boolean isValid = ruleValidator.isValid(rule);
      Assert.assertFalse(isValid);
    }
  }
}
