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

public enum TokenType {
  // Denotes composition keyword between two expressions - for example "AND" in expr1 AND expr2
  COMPOSITE,

  // Denotes operator keyword between two expressions/scalars - for example ">=" 1 >= 3
  OPERATOR,

  // Denotes whitespace
  SPACE,

  // Opening Parenthesis - (
  PARENTHESIS_OPEN,

  // Closing parenthesis - )
  PARENTHESIS_CLOSE,

  // Single quote - '
  QUOTE,

  // Escape character to add quote inside quoted string - \
  // Use \' for ' and \\ for \
  ESCAPE,

  // Denotes attributes and numbers - for example - item_price, 12.34, 100
  STRING,

  // Denotes string constants
  QUOTED_STRING,

  // Denotes any invalid token
  INVALID
}
