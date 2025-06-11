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
 * This class denotes the quoted string state in tokenizer DFA. It has below behavior 1) Transition
 * to itself when next character is anything other than quote and escape 2) Transition to start
 * state when next character is quote 3) Transition to an escape state when next character is
 * backslash \
 */
public class QuotedStringState implements TokenizerState {
  private final List<Token> currentTokenList;
  private final StringBuilder currentTokenString;

  public QuotedStringState(List<Token> currentTokenList, StringBuilder currentTokenString) {
    this.currentTokenString = currentTokenString;
    this.currentTokenList = currentTokenList;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.ESCAPE)) {
      // Begin escape sequence.
      return new EscapeInQuotedStringState(currentTokenString, this);
    } else if (tokenType.equals(TokenType.QUOTE)) {
      // Ending quote - the token is complete.
      currentTokenList.add(new Token(TokenType.QUOTED_STRING, currentTokenString.toString()));
      return new StartState(currentTokenList);
    } else {
      currentTokenString.append(nextChar);
      return this;
    }
  }

  public void terminate() throws InvalidRuleException {
    currentTokenList.add(new Token(TokenType.QUOTED_STRING, currentTokenString.toString()));
  }
}
