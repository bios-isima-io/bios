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

import io.isima.bios.monitoring.factories.MonitoringRuleStoreFactory;
import io.isima.bios.monitoring.factories.StoreType;
import io.isima.bios.monitoring.validator.MonitoringRuleValidator;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.RuleStore;
import io.isima.bios.vigilantt.validators.RuleValidator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringRuleBootstrapperTest {
  private Set<String> ruleNames;

  @Before
  public void setUp() {
    ruleNames = new HashSet<>();
    for (BootStrappedRule bootStrappedRule : BootstrappedRules.bootstrappedRules) {
      ruleNames.add(bootStrappedRule.getRuleName());
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testBootstrappedRulesAreLoaded() throws Exception {
    ExpressionParser parser = new ExpressionParser();
    RuleValidator ruleValidator = new MonitoringRuleValidator(parser);
    RuleStore ruleStore = MonitoringRuleStoreFactory.getRuleStore(StoreType.MONITORING);
    List<Rule> ruleList = ruleStore.getAllRules();
    Assert.assertNotNull(ruleList);
    for (Rule rule : ruleList) {
      ruleStore.deleteRule(rule.getId());
    }

    ruleList = ruleStore.getAllRules();
    Assert.assertNotNull(ruleList);
    Assert.assertEquals(0, ruleList.size());

    MonitoringRuleBootstrapper.addBootstrappedRulesToStore(ruleStore);
    ruleList = ruleStore.getAllRules();
    Assert.assertNotNull(ruleList);
    Assert.assertEquals(8, ruleList.size());

    for (Rule rule : ruleList) {
      Assert.assertTrue(ruleNames.contains(rule.getRuleName()));
    }
  }
}
