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
 * This class denotes OpeningParenthesisState in tokenizer DFA It has below behavior 1) Transition
 * to StartState on any valid token 2) Throw InvalidRuleException on invalid token
 */
public class OpeningParenthesisState implements TokenizerState {
  private final List<Token> currentTokenList;
  private final StringBuilder currentTokenString;

  public OpeningParenthesisState(List<Token> tokenList, StringBuilder currentTokenString) {
    this.currentTokenList = tokenList;
    this.currentTokenString = currentTokenString;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.INVALID)) {
      throw new InvalidRuleException("Given rule is not valid; invalid token: " + nextChar);
    } else {
      currentTokenList.add(new Token(TokenType.PARENTHESIS_OPEN, currentTokenString.toString()));
      return new StartState(currentTokenList).consumeChar(nextChar);
    }
  }

  public void terminate() throws InvalidRuleException {
    currentTokenList.add(new Token(TokenType.PARENTHESIS_OPEN, currentTokenString.toString()));
  }
}
