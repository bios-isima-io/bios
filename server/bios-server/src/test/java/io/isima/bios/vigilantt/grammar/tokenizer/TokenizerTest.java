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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TokenizerTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testTokenizationForStringsWithValidTokens() throws Exception {
    String[] validRules =
        new String[] {
          "=",
          "+",
          "-",
          "*",
          "/",
          "<",
          ">",
          "<=",
          ">=",
          "==",
          "AND",
          "OR",
          "(",
          ")",
          "()",
          "((",
          "))",
          "(((((",
          "))))",
          "()()()()",
          "(((())))",
          "(())(()))",
          "contains",
          "nukes_count",
          "kimjongun = god",
          "kimjongun >=  god",
          "kimjongun > god",
          "kimjongun <= god",
          "kimjongun < god",
          "kimjongun == god",
          "kimjongun * god",
          "kimjongun / god",
          "kimjongun + god",
          "kimjongun - god",
          "exception contains 'NoNukesFoundException'",
          "exception = 'NoNukesFoundException'",
          "(nukes_count > 100)",
          "((nukes_count > 100) AND (missile_count > 100))",
          "((nukes_count > 100) OR (missile_count > 100))",
          "((nukes_count > 100) OR ((missile_count > 100) AND (ak47_count > 10000000000000000)))",
          "((one Contains '🤓 Emoji') Or (two CONTAINS 'a \\' single quote'))",
          "((one contains 'Symbol for € Euro') or ((two == 'a \\\\ backslash') AND (one == 'abc'))"
        };
    for (String validRule : validRules) {
      List<Token> tokens = Tokenizer.tokenize(validRule);
      Assert.assertNotNull(tokens);
    }
  }

  @Test
  public void testTokenizationForValidStringAndVerifyTokens() throws Exception {
    String validString = "(nukes > 100)";
    List<Token> tokens = Tokenizer.tokenize(validString);
    Assert.assertNotNull(tokens);
    Assert.assertEquals(5, tokens.size());

    Token firstToken = tokens.get(0);
    Assert.assertEquals(TokenType.PARENTHESIS_OPEN, firstToken.getType());
    Assert.assertEquals("(", firstToken.getContent());

    Token secondToken = tokens.get(1);
    Assert.assertEquals(TokenType.STRING, secondToken.getType());
    Assert.assertEquals("nukes", secondToken.getContent());

    Token thirdToken = tokens.get(2);
    Assert.assertEquals(TokenType.OPERATOR, thirdToken.getType());
    Assert.assertEquals(">", thirdToken.getContent());

    Token fourthToken = tokens.get(3);
    Assert.assertEquals(TokenType.STRING, fourthToken.getType());
    Assert.assertEquals("100", fourthToken.getContent());

    Token fifthToken = tokens.get(4);
    Assert.assertEquals(TokenType.PARENTHESIS_CLOSE, fifthToken.getType());
    Assert.assertEquals(")", fifthToken.getContent());
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionOnTokenizationOfInvalidStrings() throws Exception {
    String[] invalidStrings =
        new String[] {
          "&",
          "|",
          "#",
          "@",
          "$",
          "%",
          "^",
          "!",
          "~",
          "`",
          "?",
          "}",
          "{",
          "[",
          "]",
          ":",
          ";",
          "++",
          "--",
          "//",
          "\\",
          "=>",
          "=<",
          ">>",
          "<<",
          ">>>",
          "<<<",
          "<>",
          "><",
          "**",
          "===",
          "cpu_usage>23",
          ")&",
          "(&",
          "=&",
          "&kimjongun is god",
          "kim&jongun is god",
          "kimjong&un is god",
          "kimjongun& is god",
          "kimjongun &is god",
          "kimjongun is& god",
          "kimjongun is & god",
          "kimjongun is g&od",
          "kimjongun is god&"
        };

    for (String invalidString : invalidStrings) {
      Tokenizer.tokenize(invalidString);
    }
  }
}
