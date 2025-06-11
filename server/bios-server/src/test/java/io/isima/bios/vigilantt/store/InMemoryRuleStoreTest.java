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
package io.isima.bios.vigilantt.store;

import io.isima.bios.vigilantt.exceptions.NoSuchRuleException;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.validators.ExampleRuleValidator;
import io.isima.bios.vigilantt.validators.RuleValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryRuleStoreTest {
  private RuleStore ruleStore;
  private RuleValidator ruleValidator;

  private final String supremeRule = "supremeRule";
  private final String supremeRuleExpression = "nuclear_weapons > 100";
  private final String supremeRuleCreator = "Kim Jong Un";

  @Before
  public void setUp() {
    ruleValidator = new ExampleRuleValidator();
    ruleStore = new InMemoryRuleStore(ruleValidator);
  }

  @After
  public void tearDown() {}

  @Test
  public void testCreateRule() throws Exception {
    Rule rule = new Rule(supremeRule, supremeRuleExpression, supremeRuleCreator);
    String idOfCreatedRule = ruleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);
  }

  @Test
  public void testGetRule() throws Exception {
    Rule rule = new Rule(supremeRule, supremeRuleExpression, supremeRuleCreator);
    String idOfCreatedRule = ruleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    Rule registeredRule = ruleStore.getRule(idOfCreatedRule);
    Assert.assertNotNull(registeredRule);
    Assert.assertEquals(idOfCreatedRule, registeredRule.getId());
    Assert.assertEquals(supremeRuleExpression, registeredRule.getRuleExpression());
    Assert.assertEquals(supremeRuleCreator, registeredRule.getSource());
  }

  @Test
  public void testUpdateRule() throws Exception {
    Rule rule = new Rule(supremeRule, supremeRuleExpression, supremeRuleCreator);
    String idOfCreatedRule = ruleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    String updatedSupremeRule = "nuclear_weapons > 200";
    rule.setId(idOfCreatedRule);
    rule.setRuleExpression(updatedSupremeRule);

    ruleStore.updateRule(idOfCreatedRule, rule);
    Rule updatedRule = ruleStore.getRule(idOfCreatedRule);
    Assert.assertNotNull(updatedRule);
    Assert.assertEquals(updatedSupremeRule, updatedRule.getRuleExpression());
  }

  @Test
  public void testDeleteRule() throws Exception {
    Rule rule = new Rule(supremeRule, supremeRuleExpression, supremeRuleCreator);
    String idOfCreatedRule = ruleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    ruleStore.deleteRule(idOfCreatedRule);
    try {
      Rule registeredRule = ruleStore.getRule(idOfCreatedRule);
      Assert.fail("This rule should have been deleted");
    } catch (NoSuchRuleException e) {
      // This exception is expected
    }
  }
}
