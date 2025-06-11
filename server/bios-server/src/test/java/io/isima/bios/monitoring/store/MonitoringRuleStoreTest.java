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
package io.isima.bios.monitoring.store;

import io.isima.bios.monitoring.validator.MonitoringRuleValidator;
import io.isima.bios.vigilantt.exceptions.NoSuchRuleException;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.RuleStore;
import io.isima.bios.vigilantt.validators.RuleValidator;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringRuleStoreTest {
  private final ExpressionParser parser = new ExpressionParser();
  private final RuleValidator ruleValidator = new MonitoringRuleValidator(parser);
  private final RuleStore monitoringRuleStore = new MonitoringRuleStore(ruleValidator);
  private final String newMonitoringRule = "exceptionRule";
  private final String newMonitoringRuleExpression = "(exception == 'NullPointerException')";
  private final String newMonitoringRuleOwner = "AdminInternal";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testBootstrappedMonitoringRulesStore() throws Exception {
    final String system = "system";
    MonitoringRuleBootstrapper.addBootstrappedRulesToStore(monitoringRuleStore);
    List<Rule> registeredRules = monitoringRuleStore.getAllRules();
    Assert.assertNotNull(registeredRules);
    Assert.assertEquals(8, registeredRules.size());
    for (Rule rule : registeredRules) {
      Assert.assertEquals(system, rule.getSource());
    }
  }

  @Test
  public void testCreateRule() throws Exception {
    Rule rule = new Rule(newMonitoringRule, newMonitoringRuleExpression, newMonitoringRuleOwner);
    String idOfCreatedRule = monitoringRuleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);
  }

  @Test
  public void testGetRule() throws Exception {
    Rule rule = new Rule(newMonitoringRule, newMonitoringRuleExpression, newMonitoringRuleOwner);
    String idOfCreatedRule = monitoringRuleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    Rule registeredRule = monitoringRuleStore.getRule(idOfCreatedRule);
    Assert.assertNotNull(registeredRule);
    Assert.assertEquals(idOfCreatedRule, registeredRule.getId());
    Assert.assertEquals(newMonitoringRuleExpression, registeredRule.getRuleExpression());
    Assert.assertEquals(newMonitoringRuleOwner, registeredRule.getSource());
  }

  @Test
  public void testUpdateRule() throws Exception {
    Rule rule = new Rule(newMonitoringRule, newMonitoringRuleExpression, newMonitoringRuleOwner);
    String idOfCreatedRule = monitoringRuleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    String updatedMonitoringRule = "(exception contains NullPointerException)";
    rule.setId(idOfCreatedRule);
    rule.setRuleExpression(updatedMonitoringRule);

    monitoringRuleStore.updateRule(idOfCreatedRule, rule);
    Rule updatedRule = monitoringRuleStore.getRule(idOfCreatedRule);
    Assert.assertNotNull(updatedRule);
    Assert.assertEquals(updatedMonitoringRule, updatedRule.getRuleExpression());
  }

  @Test
  public void testDeleteRule() throws Exception {
    Rule rule = new Rule(newMonitoringRule, newMonitoringRuleExpression, newMonitoringRuleOwner);
    String idOfCreatedRule = monitoringRuleStore.createRule(rule);
    Assert.assertNotNull(idOfCreatedRule);

    monitoringRuleStore.deleteRule(idOfCreatedRule);
    try {
      Rule registeredRule = monitoringRuleStore.getRule(idOfCreatedRule);
      Assert.fail("This rule should have been deleted");
    } catch (NoSuchRuleException e) {
      // This exception is expected
    }
  }
}
