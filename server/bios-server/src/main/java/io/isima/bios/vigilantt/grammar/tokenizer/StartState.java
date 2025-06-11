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
package io.isima.bios.vigilantt.grammar.tokenizer;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import java.util.List;

/**
 * This class denotes the start state in tokenizer DFA. It has below behavior 1) Transition to
 * StringState on a string type token 2) Transition to OperatorState on a operator type token 3)
 * Transition to OpeningParenthesisState on a '(' token 4) Transition to ClosingParenthesisState on
 * a ')' token 5) Transition to itself on other valid tokens - whitespaces etc 6) Throw
 * InvalidRuleException on invalid token
 */
public class StartState implements TokenizerState {
  private final List<Token> currentTokenList;

  public StartState(List<Token> tokenList) {
    this.currentTokenList = tokenList;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.QUOTED_STRING) || tokenType.equals(TokenType.ESCAPE)) {
      throw new InvalidRuleException("Given rule is not valid; invalid token: " + nextChar);
    }

    StringBuilder tokenString = new StringBuilder();
    if (tokenType.equals(TokenType.QUOTE)) {
      return new QuotedStringState(currentTokenList, tokenString);
    }

    tokenString.append(nextChar);
    if (tokenType.equals(TokenType.STRING)) {
      return new StringState(currentTokenList, tokenString);
    } else if (tokenType.equals(TokenType.OPERATOR)) {
      return new OperatorState(currentTokenList, tokenString);
    } else if (tokenType.equals(TokenType.PARENTHESIS_OPEN)) {
      return new OpeningParenthesisState(currentTokenList, tokenString);
    } else if (tokenType.equals(TokenType.PARENTHESIS_CLOSE)) {
      return new ClosingParenthesisState(currentTokenList, tokenString);
    }
    return this;
  }

  public void terminate() throws InvalidRuleException {
    // Do nothing, it is a no-op
  }
}
