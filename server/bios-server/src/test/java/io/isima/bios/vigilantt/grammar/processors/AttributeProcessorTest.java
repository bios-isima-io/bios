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

import io.isima.bios.vigilantt.grammar.node.AttributeNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.node.ScalarDoubleNode;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.Stack;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AttributeProcessorTest {
  private Stack<ExpressionTreeNode> expressionStack;
  private StringProcessor attributeProcessor;

  @Before
  public void setUp() {
    expressionStack = new Stack<>();
    attributeProcessor = new StringProcessor(expressionStack);
  }

  @After
  public void tearDown() {}

  @Test
  public void testScalarDoubleNodeIsPushedToStack() throws Exception {
    String doubleValue = "100.0";
    Token token = new Token(TokenType.STRING, doubleValue);
    attributeProcessor.process(token);
    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof ScalarDoubleNode);
    Assert.assertEquals(TokenType.STRING, node.getToken().getType());
    Assert.assertEquals(doubleValue, node.getToken().getContent());
    Assert.assertNull(node.getLeft());
    Assert.assertNull(node.getRight());
  }

  @Test
  public void testAttributeNodeIsPushedToStack() throws Exception {
    String normalLeader = "us_president";
    Token token = new Token(TokenType.STRING, normalLeader);
    attributeProcessor.process(token);
    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof AttributeNode);
    Assert.assertEquals(TokenType.STRING, node.getToken().getType());
    Assert.assertEquals(normalLeader, node.getToken().getContent());
    Assert.assertNull(node.getLeft());
    Assert.assertNull(node.getRight());
  }
}
