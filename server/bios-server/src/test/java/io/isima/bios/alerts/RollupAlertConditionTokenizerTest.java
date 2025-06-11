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
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import io.isima.bios.vigilantt.grammar.tokenizer.Tokenizer;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollupAlertConditionTokenizerTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testValidExpressionSimple() throws InvalidRuleException {
    String verySimpleExpression = "(100 + 20)";
    List<Token> tokenList = Tokenizer.tokenize(verySimpleExpression);
    Assert.assertEquals(tokenList.size(), 5);
    Token firstToken = tokenList.get(0);
    Token secondToken = tokenList.get(1);
    Token thirdToken = tokenList.get(2);

    Assert.assertEquals(firstToken.getType(), TokenType.PARENTHESIS_OPEN);
    Assert.assertEquals(firstToken.getContent(), "(");
    Assert.assertEquals(secondToken.getType(), TokenType.STRING);
    Assert.assertEquals(secondToken.getContent(), "100");
    Assert.assertEquals(thirdToken.getType(), TokenType.OPERATOR);
    Assert.assertEquals(thirdToken.getContent(), "+");
  }

  @Test
  public void testValidExpressionWithExpressionCombiners() throws InvalidRuleException {
    String compositeExpression = "((revenue >= 100) AND (quantity <= 2))";
    List<Token> tokenList = Tokenizer.tokenize(compositeExpression);
    Assert.assertEquals(tokenList.size(), 13);
    Token thirdToken = tokenList.get(2);
    Token fourthToken = tokenList.get(3);
    Token seventhToken = tokenList.get(6);

    Assert.assertEquals(thirdToken.getType(), TokenType.STRING);
    Assert.assertEquals(thirdToken.getContent(), "revenue");
    Assert.assertEquals(fourthToken.getType(), TokenType.OPERATOR);
    Assert.assertEquals(fourthToken.getContent(), ">=");
    Assert.assertEquals(seventhToken.getType(), TokenType.COMPOSITE);
    Assert.assertEquals(seventhToken.getContent(), "AND");
  }

  @Test
  public void testInvalidExpressions() throws Exception {
    String[] invalidExpressionList =
        new String[] {
          "(a >> b)",
          "b ?",
          "100 @ 09",
          "(<<<<====)",
          "% a < b",
          "(a >> b)",
          "(a === b)",
          "(a % b)",
          "(a && b)",
          "a || b",
          "a & b",
          "a | b",
          "a <> b",
          "a >< b",
          "a => b",
          "a =< b",
          "a =* b",
          "a =- b",
          "a != b"
        };
    for (String invalidExpression : invalidExpressionList) {
      try {
        Tokenizer.tokenize(invalidExpression);
      } catch (InvalidRuleException e) {
        // System.out.println(e.getMessage());
        continue;
      }
      Assert.fail(
          "InvalidRuleException was expected while tokenizing expression " + invalidExpression);
    }
  }

  @Test
  public void testValidExpressionWithFunctions() throws InvalidRuleException {
    String alertCondition = "((max(item_price) * 2) > (sum(item_price) / count()))";
    Tokenizer.tokenize(alertCondition);
    ExpressionParser expressionParser = new ExpressionParser();
    ExpressionTreeNode node = expressionParser.processExpression(alertCondition);
    Assert.assertNotNull(node);
  }
}
