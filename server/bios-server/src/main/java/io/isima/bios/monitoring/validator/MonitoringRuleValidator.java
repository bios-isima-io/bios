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
package io.isima.bios.monitoring.validator;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.validators.RuleValidator;
import java.util.EmptyStackException;

/**
 * Implementation of rule validator for monitoring rules. Uses the grammar implementation in
 * vigilantt to determine whether the rule is valid
 */
public class MonitoringRuleValidator implements RuleValidator {

  private final ExpressionParser parser;

  public MonitoringRuleValidator(ExpressionParser parser) {
    this.parser = parser;
  }

  public boolean isValid(String rule) {
    boolean valid = false;
    try {
      ExpressionTreeNode node = parser.processExpression(rule);
      valid = true;
    } catch (InvalidRuleException | EmptyStackException e) {
      // Nothing to do since this function will return false
    }
    return valid;
  }
}
