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
 * This class denotes the string state in tokenizer DFA. It has below behavior 1) Transition to
 * itself when next character is string type 2) Transition to start state when next character is
 * whitespace A) If token buffer is "contains" then push operator type token to list of tokens B) If
 * token buffer is in ("AND", "OR") then push composite type token to list of tokens C) Otherwise
 * push string type token to list of tokens 1) Throw InvalidRuleException on invalid token
 */
public class StringState implements TokenizerState {
  private final List<Token> currentTokenList;
  private final StringBuilder currentTokenString;

  public StringState(List<Token> currentTokenList, StringBuilder currentTokenString) {
    this.currentTokenString = currentTokenString;
    this.currentTokenList = currentTokenList;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.INVALID)) {
      throw new InvalidRuleException("Given rule has invalid character: <" + nextChar + ">");
    } else if (tokenType.equals(TokenType.STRING)) {
      currentTokenString.append(nextChar);
      return this;
    } else {
      String tokenString = currentTokenString.toString();
      TokenType currentTokenType = TokenType.STRING;
      if (tokenString.toLowerCase().equals(TokenizerUtils.CONTAINS_PREDICATE)) {
        currentTokenType = TokenType.OPERATOR;
      } else if (TokenizerUtils.EXPRESSION_COMPOSITION_KEYWORDS_SET.contains(
          tokenString.toLowerCase())) {
        currentTokenType = TokenType.COMPOSITE;
      }
      currentTokenList.add(new Token(currentTokenType, tokenString));
      return new StartState(currentTokenList).consumeChar(nextChar);
    }
  }

  public void terminate() throws InvalidRuleException {
    currentTokenList.add(new Token(TokenType.STRING, currentTokenString.toString()));
  }
}
