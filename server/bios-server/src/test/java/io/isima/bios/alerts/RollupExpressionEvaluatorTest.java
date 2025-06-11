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
package io.isima.bios.alerts;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.evaluator.ExpressionEvaluator;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollupExpressionEvaluatorTest {
  private ExpressionParser expressionParser;
  private Event event;

  @Before
  public void setUp() {
    expressionParser = new ExpressionParser();
    event = new EventJson();
  }

  @After
  public void tearDown() {}

  @Test
  public void testEvaluatorForSimpleExpression()
      throws UnexpectedValueException, InvalidRuleException {
    String alertCondition = "true";
    ExpressionTreeNode expressionRoot = expressionParser.processExpression(alertCondition);
    ExpressionEvaluator.evaluateExpression(expressionRoot, event);
    Boolean result = (Boolean) expressionRoot.getResult();
    Assert.assertTrue(result);
  }

  @Test
  public void testEvaluatorForComplexExpression()
      throws UnexpectedValueException, InvalidRuleException {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("revenue", 1000);
    attributes.put("discount", 100);
    attributes.put("quantity_sold", 50);
    event.setAttributes(attributes);
    String alertCondition =
        "(((revenue <= 1000) OR " + "(discount == 1000)) AND (quantity_sold <= 100))";
    ExpressionTreeNode expressionRoot = expressionParser.processExpression(alertCondition);
    ExpressionEvaluator.evaluateExpression(expressionRoot, event);
    Boolean result = (Boolean) expressionRoot.getResult();
    Assert.assertTrue(result);
  }

  @Test
  public void testEvaluatorForExpressionWithFormulas()
      throws InvalidRuleException, UnexpectedValueException {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sum(item_price)", 20561);
    attributes.put("count()", 370);
    event.setAttributes(attributes);
    String alertCondition = "((sum(item_price) / count()) > 50)";
    ExpressionTreeNode expressionTreeNode = expressionParser.processExpression(alertCondition);
    ExpressionEvaluator.evaluateExpression(expressionTreeNode, event);
    Assert.assertTrue((Boolean) expressionTreeNode.getResult());
  }
}
