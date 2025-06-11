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

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.AttributeNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.node.OperatorNode;
import io.isima.bios.vigilantt.grammar.node.ScalarDoubleNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.EmptyStackException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollupExpressionParserTest {
  private ExpressionParser expressionParser;

  @Before
  public void setUp() {
    expressionParser = new ExpressionParser();
  }

  @After
  public void tearDown() {}

  @Test
  public void testSimpleValidExpression() throws InvalidRuleException {
    String alertCondition = "(revenue >= 100)";
    ExpressionTreeNode expressionRoot = expressionParser.processExpression(alertCondition);
    Assert.assertTrue(expressionRoot instanceof OperatorNode);
    OperatorNode rootNode = (OperatorNode) expressionRoot;
    Token token = rootNode.getToken();

    Assert.assertEquals(token.getType(), TokenType.OPERATOR);
    Assert.assertEquals(token.getContent(), ">=");

    ExpressionTreeNode leftChild = rootNode.getLeft();
    Token leftChildToken = leftChild.getToken();

    Assert.assertTrue(leftChild instanceof AttributeNode);
    Assert.assertEquals(leftChildToken.getType(), TokenType.STRING);
    Assert.assertEquals(leftChildToken.getContent(), "revenue");

    ExpressionTreeNode rightChild = rootNode.getRight();
    Token rightChildToken = rightChild.getToken();

    Assert.assertTrue(rightChild instanceof ScalarDoubleNode);
    Assert.assertEquals(rightChildToken.getType(), TokenType.STRING);
    Assert.assertEquals(rightChildToken.getContent(), "100");
  }

  @Test
  public void testSimpleCompositeExpression() throws InvalidRuleException {
    String alertCondition = "((revenue >= 100) AND (quantity_sold < 100))";
    ExpressionTreeNode expressionRoot = expressionParser.processExpression(alertCondition);
    Assert.assertEquals(expressionRoot.getHeight(), 3);
  }

  @Test
  public void testComplexCompositeExpressions() throws InvalidRuleException {
    String[] alertConditions =
        new String[] {
          "(((revenue >= 100) OR (discount == 10)) AND (quantity_sold < 100))",
          "((revenue >= 100) AND ((discount == 10) OR (quantity_sold < 100)))"
        };

    for (String alertCondition : alertConditions) {
      ExpressionTreeNode expressionRoot = expressionParser.processExpression(alertCondition);
      Assert.assertEquals(expressionRoot.getHeight(), 4);
    }
  }

  @Test
  public void testValidExpressionList() throws InvalidRuleException {
    String[] validAlertConditions =
        new String[] {
          "(product_id_1 == product_id_2)",
          "(item_price * quantity_sold)",
          "(selling_price - cost_price)",
          "(revenue / item_price)",
          "(electronics_revenue + grocery_revenue)"
        };
    for (String validAlertCondition : validAlertConditions) {
      ExpressionTreeNode expressionTreeNode =
          expressionParser.processExpression(validAlertCondition);
      Assert.assertEquals(2, expressionTreeNode.getHeight());
    }
  }

  @Test
  public void testInvalidExpressionList() throws Exception {
    String[] invalidAlertConditions =
        new String[] {
          "(AND 1)",
          "(item_price + )",
          "(10 + * 20)",
          "(revenue && 100)",
          "(item_price + (quantity_sold))",
          "(item_price + abc(quantity_sold))",
          "(item_price + (quantity_sold)",
          "(item_price + (quantity_sold",
          "(item_price + min(quantity_sold",
          "(item_price + min(quantity_sold)",
          "(item_price + min(quantity_sold)))",
          ")(",
          "(item_price))",
          "( + item_price )",
          "(item_price quantity_sold)",
          "(item_price & quantity_sold)",
          "(=item_price)",
          "(==item_price)",
          "(selling_price===cost_price)",
          "(item_price &| quantity_sold)",
          "(item_price &&& quantity_sold)",
          "(item_price | quantity_sold)",
          "(+ - 20)",
          "(+ - *)",
          "(20 + *)",
          "(+ + + +)",
          "(-)",
          "(revenue)",
          "a b",
          "a b c",
          "a < b c d"
        };
    for (String alertCondition : invalidAlertConditions) {
      try {
        expressionParser.processExpression(alertCondition);
        Assert.fail("Exception InvalidAlertCondition was expected for condition " + alertCondition);
      } catch (InvalidRuleException | EmptyStackException e) {
        System.out.println(alertCondition);
        System.out.println(e.getMessage());
      }
    }
  }

  @Test
  public void testNoOpExpressions() throws Exception {
    String[] validNoopAlertConditions = new String[] {"(20)", "20", "a", "(43.12)"};
    for (String validNoopCondition : validNoopAlertConditions) {
      ExpressionTreeNode treeNode = expressionParser.processExpression(validNoopCondition);
      Assert.assertNotNull(treeNode);
    }
  }

  @Test
  public void testValidExpressions() throws Exception {
    String[] validExpressionList =
        new String[] {
          "(a > b)",
          "(a   > b)",
          "(a >       b)",
          "(a> b)",
          "(a >b)",
          "(a>b)",
          "(a < b)",
          "(a >= b)",
          "(a <= b)",
          "(a == b)",
          "(a + b)",
          "(a - b)",
          "(a * b)",
          "(a / b)",
          "(a         / b)",
          "(a /    b)",
          "(a/ b)",
          "(a /b)",
          "(a/b)",
          "(a and b)",
          "(a AND b)",
          "(a And b)",
          "(a or b)",
          "(a OR b)",
          "(a Or b)",
          "(a + 1)",
          "(a * 1)",
          "(a - 1.1)",
          "(a / 1.1)",
          "(1 - a)",
          "(1 / a)",
          "(1.2 + a)",
          "(1.2 * a)",
          "true",
          "TRUE",
          "True",
          "false",
          "FALSE",
          "False",
          "(true and false)",
          "(true == a)",
          "(false == (a < b))",
          "((a + b) / c)",
          "((((a + b)/c) > d) == false)",
          "(count() == 0)",
          "(count() < 100)",
          "(count() > 10000)",
          "(sum(xyz) > 200)",
          "(sum(xyz) > 200)",
          "(max(xyz) > 200)",
          "(max(xyz) < 200)",
          "(min(xyz) > 200)",
          "(min(xyz) < 200)",
          "((sum(abc)/count()) >= 100)",
          "((sum(abc)/count()) < 100)",
          "(last(attrib) == 'literal')",
          "(dimension == 'literal2')",
          "(dimension > 100)",
          "(dimension <= 100)",
          "((sum(abc)/count()) >= expectedAverage)"
        };
    for (String validExpression : validExpressionList) {
      try {
        ExpressionTreeNode treeNode = expressionParser.processExpression(validExpression);
        // System.out.println(validExpression);
        // System.out.println(treeNode.printTree());
        Assert.assertNotNull(treeNode);
      } catch (Throwable e) {
        Assert.fail(
            "Testing expression {"
                + validExpression
                + "}, got unexpected exception: "
                + e.toString());
      }
    }
  }

  @Test
  public void testSampleAlertConditionsOnDemoExample() throws Exception {
    String[] conditions =
        new String[] {
          "(sum(quantity_sold) < 5000)",
          "(max(item_price) > 80)",
          "((max(item_price) * 2) > (sum(item_price) / count()))",
          "((sum(item_price) / count()) > 50)",
          "((sum(quantity_sold) < 5000) AND ((sum(item_price) / count()) > 50))"
        };
    for (String condition : conditions) {
      ExpressionTreeNode node = expressionParser.processExpression(condition);
      Assert.assertNotNull(node);
      Assert.assertNotNull(node.getLeft());
      Assert.assertNotNull(node.getRight());
    }
  }
}
