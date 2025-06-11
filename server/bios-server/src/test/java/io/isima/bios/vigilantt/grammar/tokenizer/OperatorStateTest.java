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

public class OperatorStateTest {
  private OperatorState operatorState;
  private List<Token> tokens;
  private final String equalString = "=";

  @Before
  public void setUp() {
    tokens = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    sb.append(equalString);
    operatorState = new OperatorState(tokens, sb);
  }

  @After
  public void tearDown() {}

  @Test(expected = InvalidRuleException.class)
  public void testInvalidRuleException() throws Exception {
    char[] invalidChars = new char[] {'@', '$', '%', '&'};
    for (char invalidChar : invalidChars) {
      operatorState.consumeChar(invalidChar);
    }
  }

  @Test
  public void testTransitionToSelf() throws Exception {
    char operatorChar = '=';
    TokenizerState nextState = operatorState.consumeChar(operatorChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof OperatorState);
  }

  @Test
  public void testTransitionToStartState() throws Exception {
    char whitespaceChar = ' ';
    TokenizerState nextState = operatorState.consumeChar(whitespaceChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StartState);

    Token token = tokens.get(tokens.size() - 1);
    Assert.assertEquals(TokenType.OPERATOR, token.getType());
    Assert.assertEquals(equalString, token.getContent());
  }

  @Test
  public void testTransitionToMultiCharOperatorState() throws Exception {
    char operatorChar = '=';
    TokenizerState nextState = operatorState.consumeChar(operatorChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof OperatorState);

    char whitespaceChar = ' ';
    nextState = nextState.consumeChar(whitespaceChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StartState);

    nextState.terminate();
    Token token = tokens.get(tokens.size() - 1);
    Assert.assertEquals(TokenType.OPERATOR, token.getType());
    Assert.assertEquals("==", token.getContent());
  }

  @Test(expected = InvalidRuleException.class)
  public void testExceptionOnTransitionFromInvalidMultiCharOperatorState() throws Exception {
    char operatorChar = '>';
    TokenizerState nextState = operatorState.consumeChar(operatorChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof OperatorState);

    char whitespaceChar = ' ';
    nextState = nextState.consumeChar(whitespaceChar);
  }
}
