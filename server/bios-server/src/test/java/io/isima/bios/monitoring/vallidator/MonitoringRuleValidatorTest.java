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
package io.isima.bios.monitoring.vallidator;

import io.isima.bios.monitoring.validator.MonitoringRuleValidator;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringRuleValidatorTest {
  private MonitoringRuleValidator monitoringRuleValidator;

  @Before
  public void setUp() {
    ExpressionParser parser = new ExpressionParser();
    monitoringRuleValidator = new MonitoringRuleValidator(parser);
  }

  @After
  public void tearDown() {}

  @Test
  public void testValidRulesAreValidatedSuccessfully() throws Exception {
    String[] validRules =
        new String[] {
          "(cpu_usage > 80)",
          "(memory_usage > 90)",
          "(exception == 'NPE')",
          "((exception == 'NPE') OR (exception contains OOM))"
        };
    for (String validRule : validRules) {
      boolean validationResult = monitoringRuleValidator.isValid(validRule);
      Assert.assertTrue(validationResult);
    }
  }

  @Test
  public void testInvalidRulesAreValidatedSuccessfully() {
    String[] invalidRules =
        new String[] {
          "(memory_usage === 90)",
          "(memory_usage <> 90)",
          "(memory_usage > 90",
          "memory_usage > 90)",
          "%memory_usage > 90)",
          "(memory_usage > 90))",
          "(memory_usage => 90)",
          "(memory_usage != 90)",
          "(memory_usage &= 90)",
          "(memory_usage *= 90)",
          "))))((((",
          ")",
          "())"
        };
    for (String invalidRule : invalidRules) {
      boolean validationResult = monitoringRuleValidator.isValid(invalidRule);
      Assert.assertFalse(validationResult);
    }
  }
}
