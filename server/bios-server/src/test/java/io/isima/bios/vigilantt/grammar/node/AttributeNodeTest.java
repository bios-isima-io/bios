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
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AttributeNodeTest {
  private final String interContinentalBallisticMissileAttribute = "icbm";
  private final Integer interContinentalBallisticMissileCount = 50;
  private final String countAttributeName = "count";
  private final Integer countAttributeValue = 100;
  private final String nuclearWeaponsSumAttribute = "sum(nukes)";
  private final Integer nuclearWeaponsSum = 200;
  private final String nuclearWeaponCountAttribute = "count()";
  private final Integer nuclearWeaponCount = 100;
  private final String friendlyCountriesAttribute = "numCountriesFriendlyWithNorthKorea";
  private final String hostileCountriesAttribute = "numCountriesHostileWithNorthKorea";
  private final Integer hostileCountriesCount = 100;
  private Event event;
  private AttributeNode nuclearWeaponAttributeNode;
  private AttributeNode icbmAttributeNode;

  @Before
  public void setUp() {
    Map<String, Object> attributeMap = new HashMap<>();
    attributeMap.put(nuclearWeaponsSumAttribute, nuclearWeaponsSum);
    attributeMap.put(nuclearWeaponCountAttribute, nuclearWeaponCount);
    attributeMap.put(
        interContinentalBallisticMissileAttribute, interContinentalBallisticMissileCount);
    attributeMap.put(hostileCountriesAttribute, hostileCountriesCount);
    attributeMap.put(countAttributeName, countAttributeValue);
    event = new EventJson();
    event.setAttributes(attributeMap);
  }

  @Test
  public void testNormalAttributeInEventMapIsEvaluatedCorrectly() throws Exception {
    Token token = new Token(TokenType.STRING, interContinentalBallisticMissileAttribute);
    icbmAttributeNode = new AttributeNode(token);
    icbmAttributeNode.computeResult(event);

    Object attributeResult = icbmAttributeNode.getResult();
    Assert.assertNotNull(attributeResult);
    Assert.assertTrue(attributeResult instanceof Integer);
    Assert.assertEquals(interContinentalBallisticMissileCount, attributeResult);
  }

  @Test
  public void testAttributeWithNameCountIsEvaluatedCorrectly() throws Exception {
    Token token = new Token(TokenType.STRING, countAttributeName);
    AttributeNode countAttributeNode = new AttributeNode(token);
    countAttributeNode.computeResult(event);

    Object attributeResult = countAttributeNode.getResult();
    Assert.assertNotNull(attributeResult);
    Assert.assertTrue(attributeResult instanceof Integer);
    Assert.assertEquals(nuclearWeaponCount, attributeResult);
  }

  @Test
  public void tesAttributeWithCountFunctionIsEvaluatedCorrectly() throws Exception {
    Token token = new Token(TokenType.STRING, nuclearWeaponsSumAttribute);
    nuclearWeaponAttributeNode = new AttributeNode(token);
    nuclearWeaponAttributeNode.computeResult(event);

    Object attributeResult = nuclearWeaponAttributeNode.getResult();
    Assert.assertNotNull(attributeResult);
    Assert.assertTrue(attributeResult instanceof Integer);
    Assert.assertEquals(nuclearWeaponsSum, attributeResult);
  }

  @Test(expected = UnexpectedValueException.class)
  public void testExceptionWhenAttributeIsNotFoundInEventMap() throws Exception {
    Token token = new Token(TokenType.STRING, friendlyCountriesAttribute);
    nuclearWeaponAttributeNode = new AttributeNode(token);
    nuclearWeaponAttributeNode.computeResult(event);
  }
}
