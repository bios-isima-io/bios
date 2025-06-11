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
import io.isima.bios.vigilantt.grammar.evaluator.ExpressionEvaluator;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompositeExpressionNodeTest {
  private final String icbmAttributeName = "icbm";
  private final String nuclearWeaponAttributeName = "nukes";
  private AttributeNode icbmAttribute;
  private AttributeNode nuclearWeaponAttribute;
  private Event dailyDestructiveWeaponReport;
  private CompositeExpressionNode compositeExpressionNode;

  @Before
  public void setUp() {
    Token leftToken = new Token(TokenType.STRING, icbmAttributeName);
    icbmAttribute = new AttributeNode(leftToken);

    Token rightToken = new Token(TokenType.STRING, nuclearWeaponAttributeName);
    nuclearWeaponAttribute = new AttributeNode(rightToken);
  }

  @After
  public void tearDown() {}

  @Test
  public void testOrCompositionNodeBothTrue() throws Exception {
    setUpTreeForOrTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, true);
    attributeMap.put(nuclearWeaponAttributeName, true);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertTrue((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testOrCompositionNodeLeftTrue() throws Exception {
    setUpTreeForOrTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, true);
    attributeMap.put(nuclearWeaponAttributeName, false);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertTrue((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testOrCompositionNodeRightTrue() throws Exception {
    setUpTreeForOrTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, false);
    attributeMap.put(nuclearWeaponAttributeName, true);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertTrue((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testOrCompositionNodeBothFalse() throws Exception {
    setUpTreeForOrTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, false);
    attributeMap.put(nuclearWeaponAttributeName, false);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertFalse((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testAndCompositionNodeBothTrue() throws Exception {
    setUpTreeForAndTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, true);
    attributeMap.put(nuclearWeaponAttributeName, true);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertTrue((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testAndCompositionNodeLeftTrue() throws Exception {
    setUpTreeForAndTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, true);
    attributeMap.put(nuclearWeaponAttributeName, false);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertFalse((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testAndCompositionNodeRightTrue() throws Exception {
    setUpTreeForAndTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, false);
    attributeMap.put(nuclearWeaponAttributeName, true);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertFalse((Boolean) compositeExpressionNode.getResult());
  }

  @Test
  public void testAndCompositionNodeBothFalse() throws Exception {
    setUpTreeForAndTesting();
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(icbmAttributeName, false);
    attributeMap.put(nuclearWeaponAttributeName, false);

    dailyDestructiveWeaponReport = new EventJson();
    dailyDestructiveWeaponReport.setAttributes(attributeMap);

    ExpressionEvaluator.evaluateExpression(compositeExpressionNode, dailyDestructiveWeaponReport);
    Assert.assertNotNull(compositeExpressionNode.getResult());
    Assert.assertTrue(compositeExpressionNode.getResult() instanceof Boolean);
    Assert.assertFalse((Boolean) compositeExpressionNode.getResult());
  }

  private void setUpTreeForOrTesting() {
    Token token = new Token(TokenType.COMPOSITE, "OR");
    compositeExpressionNode = new CompositeExpressionNode(token);
    compositeExpressionNode.setLeftChild(icbmAttribute);
    compositeExpressionNode.setRightChild(nuclearWeaponAttribute);
  }

  private void setUpTreeForAndTesting() {
    Token token = new Token(TokenType.COMPOSITE, "AND");
    compositeExpressionNode = new CompositeExpressionNode(token);
    compositeExpressionNode.setLeftChild(icbmAttribute);
    compositeExpressionNode.setRightChild(nuclearWeaponAttribute);
  }
}
