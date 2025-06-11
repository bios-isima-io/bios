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

/**
 * This class denotes the escape state within a quoted string state in tokenizer DFA. It has below
 * behavior 1) Transition to the parent quoted string state if the next character is one that needs
 * escaping: \ ' 2) Error otherwise
 */
public class EscapeInQuotedStringState implements TokenizerState {
  private final StringBuilder currentTokenString;
  private final QuotedStringState parentState;

  public EscapeInQuotedStringState(
      StringBuilder currentTokenString, QuotedStringState parentState) {
    this.currentTokenString = currentTokenString;
    this.parentState = parentState;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.ESCAPE)) {
      currentTokenString.append(nextChar);
      return parentState;
    } else if (tokenType.equals(TokenType.QUOTE)) {
      currentTokenString.append(nextChar);
      return parentState;
    } else {
      throw new InvalidRuleException(
          "Invalid character after escape character backslash: <"
              + nextChar
              + ">. Only backslash and single quote are allowed after a backslash.");
    }
  }

  public void terminate() throws InvalidRuleException {
    throw new InvalidRuleException("Invalid termination just after escape character backslash");
  }
}
