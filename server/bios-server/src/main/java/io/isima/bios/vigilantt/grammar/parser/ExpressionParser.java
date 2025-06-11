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
package io.isima.bios.vigilantt.grammar.parser;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.processors.ClosingParenthesisProcessor;
import io.isima.bios.vigilantt.grammar.processors.CompositeProcessor;
import io.isima.bios.vigilantt.grammar.processors.OpeningParenthesisProcessor;
import io.isima.bios.vigilantt.grammar.processors.OperatorProcessor;
import io.isima.bios.vigilantt.grammar.processors.QuotedStringProcessor;
import io.isima.bios.vigilantt.grammar.processors.StringProcessor;
import io.isima.bios.vigilantt.grammar.processors.TokenConsumer;
import io.isima.bios.vigilantt.grammar.processors.TokenProcessor;
import io.isima.bios.vigilantt.grammar.processors.WhitespaceProcessor;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import io.isima.bios.vigilantt.grammar.tokenizer.Tokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class ExpressionParser {

  private final Stack<ExpressionTreeNode> expressionStack;
  private final Map<TokenType, TokenProcessor> tokenProcessorMap;
  private final TokenConsumer tokenConsumer;

  public ExpressionParser() {
    expressionStack = new Stack<>();
    tokenConsumer = new TokenConsumer();
    tokenProcessorMap = new HashMap<>();
    tokenProcessorMap.put(TokenType.COMPOSITE, new CompositeProcessor(expressionStack));
    tokenProcessorMap.put(TokenType.OPERATOR, new OperatorProcessor(expressionStack));
    tokenProcessorMap.put(TokenType.SPACE, new WhitespaceProcessor(expressionStack));
    tokenProcessorMap.put(
        TokenType.PARENTHESIS_OPEN, new OpeningParenthesisProcessor(expressionStack));
    tokenProcessorMap.put(
        TokenType.PARENTHESIS_CLOSE, new ClosingParenthesisProcessor(expressionStack));
    tokenProcessorMap.put(TokenType.STRING, new StringProcessor(expressionStack));
    tokenProcessorMap.put(TokenType.QUOTED_STRING, new QuotedStringProcessor(expressionStack));
  }

  public ExpressionTreeNode processExpression(String alertCondition) throws InvalidRuleException {
    expressionStack.clear();
    List<Token> tokenList = Tokenizer.tokenize(alertCondition);
    for (Token token : tokenList) {
      TokenProcessor tokenProcessor = tokenProcessorMap.get(token.getType());
      if (tokenProcessor == null) {
        throw new InvalidRuleException("Invalid token: {" + token.getContent() + "}");
      } else {
        tokenConsumer.setTokenProcessor(tokenProcessor);
        tokenConsumer.consume(token);
      }
    }

    if (expressionStack.size() != 1) {
      throw new InvalidRuleException(
          String.format(
              "Given rule string is not valid: it contains"
                  + " %d expressions, expected 1. Enclose expressions in parentheses e.g. '(a < b)'",
              expressionStack.size()));
    }

    return expressionStack.pop();
  }
}
