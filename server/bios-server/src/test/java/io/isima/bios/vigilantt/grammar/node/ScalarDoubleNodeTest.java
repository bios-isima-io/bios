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
package io.isima.bios.vigilantt.grammar.node;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ScalarDoubleNodeTest {
  private final String supremeLeaderName = "kimjongun";
  private final String supremeLeader = "supremeLeader";
  private final Double numberOfNukes = 100.0;
  private ScalarDoubleNode scalarDoubleNode;
  private Event event;

  @Before
  public void setUp() {
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(supremeLeader, supremeLeaderName);

    event = new EventJson();
    event.setAttributes(attributeMap);
  }

  @After
  public void tearDown() {}

  @Test
  public void testScalarDoubleNodeEvaluation() throws Exception {
    Token token = new Token(TokenType.STRING, String.valueOf(numberOfNukes));
    ScalarDoubleNode scalarDoubleNode = new ScalarDoubleNode(token);

    scalarDoubleNode.computeResult(event);
    Assert.assertNotNull(scalarDoubleNode.getResult());
    Assert.assertEquals(numberOfNukes, scalarDoubleNode.getResult());
  }

  @Test(expected = NumberFormatException.class)
  public void testScalarDoubleNodeEvaluationException() throws Exception {
    Token token = new Token(TokenType.STRING, supremeLeaderName);
    ScalarDoubleNode scalarDoubleNode = new ScalarDoubleNode(token);

    scalarDoubleNode.computeResult(event);
  }
}
