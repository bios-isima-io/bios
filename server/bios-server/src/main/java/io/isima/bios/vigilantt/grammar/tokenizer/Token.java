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

/**
 * This class encapsulates the details about a token in rule string. TokenType indicates the
 * syntactic type and content has the actual value of token.
 */
public class Token {
  private final TokenType type;
  private String content;

  public Token(TokenType type, String content) {
    this.type = type;
    this.content = content;
  }

  public TokenType getType() {
    return type;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  @Override
  public String toString() {
    return "type = " + type + ", content = " + content;
  }
}
