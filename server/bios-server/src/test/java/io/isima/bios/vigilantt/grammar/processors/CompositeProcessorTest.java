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

import io.isima.bios.vigilantt.grammar.node.CompositeExpressionNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.Stack;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompositeProcessorTest {
  private Stack<ExpressionTreeNode> expressionStack;
  private CompositeProcessor compositeProcessor;
  private final String andComposition = "AND";
  private final Token compositeToken = new Token(TokenType.COMPOSITE, andComposition);

  @Before
  public void setUp() {
    expressionStack = new Stack<>();
    compositeProcessor = new CompositeProcessor(expressionStack);
  }

  @After
  public void tearDown() {}

  @Test
  public void testCompositeNode() throws Exception {
    compositeProcessor.process(compositeToken);
    ExpressionTreeNode node = expressionStack.peek();
    Assert.assertNotNull(node);
    Assert.assertTrue(node instanceof CompositeExpressionNode);
    Assert.assertEquals(TokenType.COMPOSITE, node.getToken().getType());
    Assert.assertEquals(andComposition, node.getToken().getContent());
  }
}
