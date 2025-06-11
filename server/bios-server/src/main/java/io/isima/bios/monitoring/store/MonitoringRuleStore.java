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

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.exceptions.NoSuchRuleException;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.RuleStore;
import io.isima.bios.vigilantt.validators.RuleValidator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** In-memory store for monitoring rules. */
public class MonitoringRuleStore implements RuleStore {
  private final Map<String, Rule> ruleMap;
  private final RuleValidator ruleValidator;

  public MonitoringRuleStore(RuleValidator ruleValidator) {
    this.ruleMap = new HashMap<>();
    this.ruleValidator = ruleValidator;
  }

  public String createRule(Rule rule) throws InvalidRuleException {
    if (!ruleValidator.isValid(rule.getRuleExpression())) {
      throw new InvalidRuleException();
    }
    String ruleId = UUID.randomUUID().toString();
    rule.setId(ruleId);
    ruleMap.put(ruleId, rule);
    return ruleId;
  }

  public Rule getRule(String ruleId) throws NoSuchRuleException {
    if (!ruleMap.containsKey(ruleId)) {
      throw new NoSuchRuleException();
    }
    return ruleMap.get(ruleId);
  }

  public void updateRule(String ruleId, Rule rule) throws InvalidRuleException {
    if (!ruleValidator.isValid(rule.getRuleExpression())) {
      throw new InvalidRuleException();
    }
    ruleMap.put(ruleId, rule);
  }

  public void deleteRule(String ruleId) throws NoSuchRuleException {
    if (!ruleMap.containsKey(ruleId)) {
      throw new NoSuchRuleException();
    }
    ruleMap.remove(ruleId);
  }

  public List<Rule> getAllRules() {
    List<Rule> rules = new ArrayList<>();
    for (String key : ruleMap.keySet()) {
      rules.add(ruleMap.get(key));
    }
    return rules;
  }
}
