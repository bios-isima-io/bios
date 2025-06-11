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
import io.isima.bios.vigilantt.grammar.node.BooleanNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.node.ScalarDoubleNode;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.utils.ComputationUtil;
import java.util.Stack;

public class StringProcessor implements TokenProcessor {
  private final Stack<ExpressionTreeNode> expressionStack;

  public StringProcessor(Stack<ExpressionTreeNode> expressionStack) {
    this.expressionStack = expressionStack;
  }

  public void process(Token token) {
    if (ComputationUtil.isConvertibleToDouble(token.getContent())) {
      expressionStack.push(new ScalarDoubleNode(token));
    } else if (ComputationUtil.isConvertibleToBoolean(token.getContent())) {
      expressionStack.push(new BooleanNode(token));
    } else {
      expressionStack.push(new AttributeNode(token));
    }
  }
}
