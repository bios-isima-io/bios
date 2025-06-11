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
 * This class denotes the operator state in tokenizer DFA. It has below behavior 1) Transition to
 * itself on operator type token 2) Transition to StartState on whitespace token 3) Throw
 * InvalidRuleException when A) next token is invalid B) current token buffer does not contain valid
 * operator
 */
public class OperatorState implements TokenizerState {
  private final List<Token> tokenList;
  private final StringBuilder currentTokenString;

  public OperatorState(List<Token> tokenList, StringBuilder currentTokenString) {
    this.tokenList = tokenList;
    this.currentTokenString = currentTokenString;
  }

  public TokenizerState consumeChar(char nextChar) throws InvalidRuleException {
    TokenType tokenType = TokenizerUtils.getTokenType(nextChar);
    if (tokenType.equals(TokenType.INVALID)) {
      throw new InvalidRuleException("Given rule is not valid; invalid token: " + nextChar);
    } else if (currentTokenString.length() > 2) {
      throw new InvalidRuleException(
          "Given rule is not valid; operator longer than 2: " + currentTokenString.toString());
    } else if (tokenType.equals(TokenType.OPERATOR)) {
      currentTokenString.append(nextChar);
      return this;
    } else if ((tokenType.equals(TokenType.SPACE)) || (tokenType.equals(TokenType.STRING))) {
      String tokenString = currentTokenString.toString();
      if (TokenizerUtils.getAllowedOperators().contains(tokenString)) {
        tokenList.add(new Token(TokenType.OPERATOR, currentTokenString.toString()));
        return new StartState(tokenList).consumeChar(nextChar);
      } else {
        throw new InvalidRuleException("Given rule is not valid; invalid operator: " + tokenString);
      }
    }
    throw new InvalidRuleException(
        "Given rule is not valid; unexpected character after operator: " + nextChar);
  }

  public void terminate() throws InvalidRuleException {
    String tokenString = currentTokenString.toString();
    if (TokenizerUtils.getAllowedOperators().contains(tokenString)) {
      tokenList.add(new Token(TokenType.OPERATOR, currentTokenString.toString()));
    } else {
      throw new InvalidRuleException(
          "Given rule is not valid; operator not recognized: " + tokenString);
    }
  }
}
