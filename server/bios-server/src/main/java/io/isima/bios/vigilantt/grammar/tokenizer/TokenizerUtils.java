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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** This class has utility constants for tokenizer. */
public class TokenizerUtils {
  public static final TokenType[] TYPE_MAP;

  public static final String[] EXPRESSION_COMPOSITION_KEYWORDS = new String[] {"and", "or"};
  public static final String CONTAINS_PREDICATE = "contains";
  public static final String OPENING_PARENTHESIS = "(";
  public static final String CLOSING_PARENTHESIS = ")";
  public static final char[] SINGLE_CHAR_OPERATORS = new char[] {'=', '>', '<', '+', '-', '*', '/'};
  public static final String[] MULTI_CHAR_OPERATORS = new String[] {"==", ">=", "<="};
  public static final Set<String> EXPRESSION_COMPOSITION_KEYWORDS_SET =
      new HashSet<>(Arrays.asList(EXPRESSION_COMPOSITION_KEYWORDS));

  static {
    TYPE_MAP = new TokenType[128];
    Arrays.fill(TYPE_MAP, TokenType.QUOTED_STRING);
    for (char op : SINGLE_CHAR_OPERATORS) {
      TYPE_MAP[op] = TokenType.OPERATOR;
    }
    TYPE_MAP[' '] = TokenType.SPACE;
    TYPE_MAP['\t'] = TokenType.SPACE;
    TYPE_MAP['('] = TokenType.PARENTHESIS_OPEN;
    TYPE_MAP[')'] = TokenType.PARENTHESIS_CLOSE;
    Arrays.fill(TYPE_MAP, 'a', 'z' + 1, TokenType.STRING);
    Arrays.fill(TYPE_MAP, 'A', 'Z' + 1, TokenType.STRING);
    Arrays.fill(TYPE_MAP, '0', '9' + 1, TokenType.STRING);
    TYPE_MAP['.'] = TokenType.STRING;
    TYPE_MAP['_'] = TokenType.STRING;
    TYPE_MAP['\''] = TokenType.QUOTE;
    TYPE_MAP['\\'] = TokenType.ESCAPE;
  }

  public static TokenType getTokenType(char ch) {
    if (ch >= 128) {
      return TokenType.QUOTED_STRING;
    } else {
      return TYPE_MAP[ch];
    }
  }

  public static Set<String> getAllowedOperators() {
    Set<String> operators = new HashSet<>();
    for (final char singleCharOperator : SINGLE_CHAR_OPERATORS) {
      operators.add(String.valueOf(singleCharOperator));
    }
    operators.addAll(Arrays.asList(MULTI_CHAR_OPERATORS));
    return operators;
  }
}
