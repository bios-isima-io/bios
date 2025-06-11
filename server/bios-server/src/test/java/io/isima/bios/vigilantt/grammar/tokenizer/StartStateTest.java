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

public class StartStateTest {
  private StartState startState;

  @Before
  public void setUp() {
    List<Token> tokens = new ArrayList<>();
    startState = new StartState(tokens);
  }

  @After
  public void tearDown() {}

  @Test(expected = InvalidRuleException.class)
  public void testInvalidRuleException() throws Exception {
    char[] invalidChars = new char[] {'@', '$', '%', '&'};
    for (char invalidChar : invalidChars) {
      startState.consumeChar(invalidChar);
    }
  }

  @Test
  public void testTransitionToStringState() throws Exception {
    char stringChar = 'a';
    TokenizerState nextState = startState.consumeChar(stringChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StringState);
  }

  @Test
  public void testTransitionToOperatorState() throws Exception {
    char operatorChar = '+';
    TokenizerState nextState = startState.consumeChar(operatorChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof OperatorState);
  }

  @Test
  public void testTransitionToOpeningParenthesisState() throws Exception {
    char openingParenthesisChar = '(';
    TokenizerState nextState = startState.consumeChar(openingParenthesisChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof OpeningParenthesisState);
  }

  @Test
  public void testTransitionToClosingParenthesisState() throws Exception {
    char closingParenthesisChar = ')';
    TokenizerState nextState = startState.consumeChar(closingParenthesisChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof ClosingParenthesisState);
  }

  @Test
  public void testTransitionToSelf() throws Exception {
    char whitespaceChar = ' ';
    TokenizerState nextState = startState.consumeChar(whitespaceChar);
    Assert.assertNotNull(nextState);
    Assert.assertTrue(nextState instanceof StartState);
  }
}
