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
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StringStateTest {
  private StringState stringState;
  private final String supremeLeader = "kimjongun";
  private List<Token> tokens;

  @Before
  public void setUp() {
    tokens = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    sb.append(supremeLeader);
    stringState = new StringState(tokens, sb);
  }

  @After
  public void tearDown() {}

  @Test(expected = InvalidRuleException.class)
  public void testInvalidRuleException() throws Exception {
    char[] invalidChars = new char[] {'@', '$', '%', '&'};
    for (char invalidChar : invalidChars) {
      stringState.consumeChar(invalidChar);
    }
  }

  @Test
  public void testTransitionToSelf() throws Exception {
    char stringChar = 'a';
    TokenizerState nextState = stringState.consumeChar(stringChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StringState);
    Assert.assertEquals(stringState, nextState);
  }

  @Test
  public void testTransitionToStartStateAfterStringToken() throws Exception {
    char whitespaceChar = ' ';
    TokenizerState nextState = stringState.consumeChar(whitespaceChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StartState);

    Token token = tokens.get(tokens.size() - 1);
    Assert.assertEquals(TokenType.STRING, token.getType());
    Assert.assertEquals(supremeLeader, token.getContent());
  }

  @Test
  public void testTransitionToStartStateAfterContainsToken() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(TokenizerUtils.CONTAINS_PREDICATE);
    stringState = new StringState(tokens, sb);

    char whitespaceChar = ' ';
    TokenizerState nextState = stringState.consumeChar(whitespaceChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StartState);

    Token token = tokens.get(tokens.size() - 1);
    Assert.assertEquals(TokenType.OPERATOR, token.getType());
    Assert.assertEquals(TokenizerUtils.CONTAINS_PREDICATE, token.getContent());
  }

  @Test
  public void testTransitionToStartStateAfterCompositeToken() throws Exception {
    for (String compositionString : TokenizerUtils.EXPRESSION_COMPOSITION_KEYWORDS) {
      StringBuilder sb = new StringBuilder();
      sb.append(compositionString);
      stringState = new StringState(tokens, sb);

      char whitespaceChar = ' ';
      TokenizerState nextState = stringState.consumeChar(whitespaceChar);
      Assert.assertNotNull(nextState);
      Assert.assertTrue(nextState instanceof StartState);

      Token token = tokens.get(tokens.size() - 1);
      Assert.assertEquals(TokenType.COMPOSITE, token.getType());
      Assert.assertEquals(compositionString, token.getContent());
    }
  }

  @Test
  public void testTransitionToCorrectStateOnNonWhitespaceCharacter() throws Exception {
    char closingParenthesisChar = ')';
    TokenizerState nextState = stringState.consumeChar(closingParenthesisChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof ClosingParenthesisState);

    nextState.terminate();
    Token token = tokens.get(tokens.size() - 1);
    Assert.assertNotNull(token);
    Assert.assertEquals(TokenType.PARENTHESIS_CLOSE, token.getType());
    Assert.assertEquals(")", token.getContent());
  }
}
