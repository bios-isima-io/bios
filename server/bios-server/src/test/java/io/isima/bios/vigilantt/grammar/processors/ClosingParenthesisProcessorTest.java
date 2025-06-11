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
package io.isima.bios.vigilantt.grammar.processors;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.AttributeNode;
import io.isima.bios.vigilantt.grammar.node.BooleanNode;
import io.isima.bios.vigilantt.grammar.node.CompositeExpressionNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.node.OperatorNode;
import io.isima.bios.vigilantt.grammar.node.ScalarDoubleNode;
import io.isima.bios.vigilantt.grammar.node.ScalarStringNode;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenizerUtils;
import java.util.Stack;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClosingParenthesisProcessorTest {
  private Stack<ExpressionTreeNode> expressionStack;
  private ClosingParenthesisProcessor closingParenthesisProcessor;
  private final Token closingParenthesisToken =
      new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);

  @Before
  public void setUp() {
    expressionStack = new Stack<>();
    closingParenthesisProcessor = new ClosingParenthesisProcessor(expressionStack);
  }

  @After
  public void tearDown() {}

  @Test(expected = InvalidRuleException.class)
  public void testExceptionOnInsufficientNodesOnStack() throws Exception {
    closingParenthesisProcessor.process(closingParenthesisToken);
  }

  @Test
  public void testNoopScalarExpression() throws Exception {
    Token openingParenthesisToken = new Token(TokenType.PARENTHESIS_OPEN, "(");
    ScalarStringNode scalarStringNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(scalarStringNode);

    Token scalarDoubleToken = new Token(TokenType.STRING, "100.0");
    ScalarDoubleNode scalarDoubleNode = new ScalarDoubleNode(scalarDoubleToken);
    expressionStack.push(scalarDoubleNode);

    Token closingParenthesisToken = new Token(TokenType.PARENTHESIS_CLOSE, ")");
    closingParenthesisProcessor.process(closingParenthesisToken);

    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof ScalarDoubleNode);
    Assert.assertEquals(TokenType.STRING, node.getToken().getType());
    Assert.assertEquals("100.0", node.getToken().getContent());
  }

  @Test
  public void testMathematicalExpression() throws Exception {
    final String formula = "sum";
    final String attribute = "revenue";
    final String fullFormula =
        formula
            + TokenizerUtils.OPENING_PARENTHESIS
            + attribute
            + TokenizerUtils.CLOSING_PARENTHESIS;
    Token formulaToken = new Token(TokenType.STRING, formula);
    ScalarStringNode formulaNode = new ScalarStringNode(formulaToken);
    expressionStack.push(formulaNode);

    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, attribute);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token closingParenthesisToken =
        new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);
    closingParenthesisProcessor.process(closingParenthesisToken);

    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof AttributeNode);
    Assert.assertEquals(TokenType.STRING, node.getToken().getType());
    Assert.assertEquals(fullFormula, node.getToken().getContent());
  }

  @Test
  public void testOperationExpression() throws Exception {
    final String supremeLeaderRespect = "kimJongUnRespect";
    final String supremeLeaderRespectValue = "1000000000";
    final String greaterOperator = ">";
    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, supremeLeaderRespect);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token operatorToken = new Token(TokenType.OPERATOR, greaterOperator);
    OperatorNode operatorNode = new OperatorNode(operatorToken);
    expressionStack.push(operatorNode);

    Token attributeValueToken = new Token(TokenType.STRING, supremeLeaderRespectValue);
    ScalarStringNode attributeValueNode = new ScalarStringNode(attributeValueToken);
    expressionStack.push(attributeValueNode);

    Token closingParenthesisToken =
        new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);
    closingParenthesisProcessor.process(closingParenthesisToken);

    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof OperatorNode);
    Assert.assertEquals(TokenType.OPERATOR, node.getToken().getType());
    Assert.assertEquals(greaterOperator, node.getToken().getContent());

    ExpressionTreeNode leftChild = node.getLeft();
    Assert.assertNotNull(leftChild);
    Assert.assertTrue(leftChild instanceof ScalarStringNode);
    Assert.assertEquals(TokenType.STRING, leftChild.getToken().getType());
    Assert.assertEquals(supremeLeaderRespect, leftChild.getToken().getContent());

    ExpressionTreeNode rightChild = node.getRight();
    Assert.assertNotNull(rightChild);
    Assert.assertTrue(rightChild instanceof ScalarStringNode);
    Assert.assertEquals(TokenType.STRING, rightChild.getToken().getType());
    Assert.assertEquals(supremeLeaderRespectValue, rightChild.getToken().getContent());
  }

  @Test
  public void testCompositionExpression() throws Exception {
    final String supremeLeaderDeservesRespect = "true";
    final String americanLeaderDeservesRespect = "false";
    final String orComposition = "OR";
    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token leftToken = new Token(TokenType.STRING, supremeLeaderDeservesRespect);
    BooleanNode leftNode = new BooleanNode(leftToken);
    expressionStack.push(leftNode);

    Token compositeToken = new Token(TokenType.COMPOSITE, orComposition);
    CompositeExpressionNode compositeNode = new CompositeExpressionNode(compositeToken);
    expressionStack.push(compositeNode);

    Token rightToken = new Token(TokenType.STRING, americanLeaderDeservesRespect);
    BooleanNode rightNode = new BooleanNode(rightToken);
    expressionStack.push(rightNode);

    Token closingParenthesisToken =
        new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);
    closingParenthesisProcessor.process(closingParenthesisToken);

    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof CompositeExpressionNode);
    Assert.assertEquals(TokenType.COMPOSITE, node.getToken().getType());
    Assert.assertEquals(orComposition, node.getToken().getContent());

    ExpressionTreeNode leftChild = node.getLeft();
    Assert.assertNotNull(leftChild);
    Assert.assertTrue(leftChild instanceof BooleanNode);
    Assert.assertEquals(TokenType.STRING, leftChild.getToken().getType());
    Assert.assertEquals(supremeLeaderDeservesRespect, leftChild.getToken().getContent());

    ExpressionTreeNode rightChild = node.getRight();
    Assert.assertNotNull(rightChild);
    Assert.assertTrue(rightChild instanceof BooleanNode);
    Assert.assertEquals(TokenType.STRING, rightChild.getToken().getType());
    Assert.assertEquals(americanLeaderDeservesRespect, rightChild.getToken().getContent());
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionOnInvalidFormula() throws Exception {
    final String formula = "median";
    final String attribute = "revenue";
    Token formulaToken = new Token(TokenType.STRING, formula);
    ScalarStringNode formulaNode = new ScalarStringNode(formulaToken);
    expressionStack.push(formulaNode);

    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, attribute);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token closingParenthesisToken =
        new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);
    closingParenthesisProcessor.process(closingParenthesisToken);
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionWhenNoMatchingOpeningParenthesis() throws Exception {
    final String supremeLeaderRespect = "kimJongUnRespect";
    final String supremeLeaderRespectValue = "1000000000";
    final String greaterOperator = ">";
    Token closingParenthesisToken =
        new Token(TokenType.PARENTHESIS_CLOSE, TokenizerUtils.CLOSING_PARENTHESIS);
    ScalarStringNode closingParenthesisNode = new ScalarStringNode(closingParenthesisToken);
    expressionStack.push(closingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, supremeLeaderRespect);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token operatorToken = new Token(TokenType.OPERATOR, greaterOperator);
    OperatorNode operatorNode = new OperatorNode(operatorToken);
    expressionStack.push(operatorNode);

    Token attributeValueToken = new Token(TokenType.STRING, supremeLeaderRespectValue);
    ScalarStringNode attributeValueNode = new ScalarStringNode(attributeValueToken);
    expressionStack.push(attributeValueNode);

    closingParenthesisProcessor.process(closingParenthesisToken);
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionWhenInvalidOperation() throws Exception {
    final String supremeLeaderRespect = "kimJongUnRespect";
    final String supremeLeaderRespectValue = "1000000000";
    final String greaterOperator = ">";
    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, supremeLeaderRespect);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token operatorToken = new Token(TokenType.STRING, greaterOperator);
    OperatorNode operatorNode = new OperatorNode(operatorToken);
    expressionStack.push(operatorNode);

    Token attributeValueToken = new Token(TokenType.STRING, supremeLeaderRespectValue);
    ScalarStringNode attributeValueNode = new ScalarStringNode(attributeValueToken);
    expressionStack.push(attributeValueNode);

    closingParenthesisProcessor.process(closingParenthesisToken);
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionWhenLeftChildInvalid() throws Exception {
    final String supremeLeaderRespect = "kimJongUnRespect";
    final String supremeLeaderRespectValue = "1000000000";
    final String greaterOperator = ">";
    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, supremeLeaderRespect);
    OperatorNode attributeNode = new OperatorNode(attributeToken);
    expressionStack.push(attributeNode);

    Token operatorToken = new Token(TokenType.OPERATOR, greaterOperator);
    OperatorNode operatorNode = new OperatorNode(operatorToken);
    expressionStack.push(operatorNode);

    Token attributeValueToken = new Token(TokenType.STRING, supremeLeaderRespectValue);
    ScalarStringNode attributeValueNode = new ScalarStringNode(attributeValueToken);
    expressionStack.push(attributeValueNode);

    closingParenthesisProcessor.process(closingParenthesisToken);
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionWhenRightChildInvalid() throws Exception {
    final String supremeLeaderRespect = "kimJongUnRespect";
    final String supremeLeaderRespectValue = "1000000000";
    final String greaterOperator = ">";
    Token openingParenthesisToken =
        new Token(TokenType.PARENTHESIS_OPEN, TokenizerUtils.OPENING_PARENTHESIS);
    ScalarStringNode openingParenthesisNode = new ScalarStringNode(openingParenthesisToken);
    expressionStack.push(openingParenthesisNode);

    Token attributeToken = new Token(TokenType.STRING, supremeLeaderRespect);
    ScalarStringNode attributeNode = new ScalarStringNode(attributeToken);
    expressionStack.push(attributeNode);

    Token operatorToken = new Token(TokenType.OPERATOR, greaterOperator);
    OperatorNode operatorNode = new OperatorNode(operatorToken);
    expressionStack.push(operatorNode);

    Token attributeValueToken = new Token(TokenType.STRING, supremeLeaderRespectValue);
    OperatorNode attributeValueNode = new OperatorNode(attributeValueToken);
    expressionStack.push(attributeValueNode);

    closingParenthesisProcessor.process(closingParenthesisToken);
  }
}
