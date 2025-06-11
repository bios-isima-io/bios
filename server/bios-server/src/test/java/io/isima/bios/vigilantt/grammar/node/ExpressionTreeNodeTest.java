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
package io.isima.bios.vigilantt.grammar.node;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExpressionTreeNodeTest {
  private ExpressionTreeNode rootNode;
  private Event event = new EventJson();
  private final String supremeCountry = " North Korea ";
  private final String supremeStatementLeft = " Kim Jong Un is best ";
  private final String supremeStatementRight = " Kim Jong Un is supreme leader ";

  private Token rootToken = new Token(TokenType.STRING, supremeCountry);
  private Token leftToken = new Token(TokenType.STRING, supremeStatementLeft);
  private Token rightToken = new Token(TokenType.STRING, supremeStatementRight);

  @Before
  public void setUp() throws UnexpectedValueException {
    rootNode = new ScalarStringNode(rootToken);
    rootNode.computeResult(event);
    setUpLeftChild(rootNode);
    setUpRightChild(rootNode);
  }

  @After
  public void tearDown() {}

  private void setUpLeftChild(ExpressionTreeNode rootNode) throws UnexpectedValueException {
    ExpressionTreeNode leftNode;
    Token token = new Token(TokenType.STRING, supremeStatementLeft);
    leftNode = new ScalarStringNode(token);
    leftNode.computeResult(event);
    rootNode.setLeftChild(leftNode);
  }

  private void setUpRightChild(ExpressionTreeNode rootNode) throws UnexpectedValueException {
    ExpressionTreeNode rightNode;
    Token token = new Token(TokenType.STRING, supremeStatementRight);
    rightNode = new ScalarStringNode(token);
    rightNode.computeResult(event);
    rootNode.setRightChild(rightNode);
  }

  @Test
  public void testRootNode() throws Exception {
    Token token = rootNode.getToken();
    Assert.assertNotNull(rootToken);
    Assert.assertEquals(rootToken.getType(), token.getType());
    Assert.assertEquals(rootToken.getContent(), token.getContent());
    Assert.assertEquals(rootNode.getResult(), supremeCountry);
  }

  @Test
  public void testLeftChild() {
    ExpressionTreeNode leftNode = rootNode.getLeft();
    Token token = leftNode.getToken();
    Assert.assertNotNull(leftNode);
    Assert.assertEquals(leftToken.getType(), token.getType());
    Assert.assertEquals(leftToken.getContent(), token.getContent());
    Assert.assertEquals(leftNode.getResult(), supremeStatementLeft);
  }

  @Test
  public void testRightChild() {
    ExpressionTreeNode rightNode = rootNode.getRight();
    Token token = rightNode.getToken();
    Assert.assertNotNull(rightNode);
    Assert.assertEquals(rightToken.getType(), token.getType());
    Assert.assertEquals(rightToken.getContent(), token.getContent());
    Assert.assertEquals(rightNode.getResult(), supremeStatementRight);
  }

  @Test
  public void testIsLeafNode() {
    ExpressionTreeNode leftNode = rootNode.getLeft();
    Assert.assertTrue(leftNode.isLeafNode());

    ExpressionTreeNode rightNode = rootNode.getRight();
    Assert.assertTrue(rightNode.isLeafNode());
  }

  @Test
  public void testGetHeight() {
    int rootHeight = rootNode.getHeight();
    Assert.assertEquals(2, rootHeight);

    int leftHeight = rootNode.getLeft().getHeight();
    Assert.assertEquals(1, leftHeight);

    int rightHeight = rootNode.getRight().getHeight();
    Assert.assertEquals(1, rightHeight);
  }

  @Test
  public void testFullPropagandaStatement() {
    String fullPropagandaStatement = rootNode.printTree();
    Assert.assertEquals(
        supremeStatementLeft + "." + supremeCountry + "." + supremeStatementRight,
        fullPropagandaStatement);
  }
}
