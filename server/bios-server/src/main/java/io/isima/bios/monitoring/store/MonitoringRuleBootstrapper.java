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
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.RuleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class to load the monitoring rules in file into memory. */
public class MonitoringRuleBootstrapper {
  private static final Logger logger = LoggerFactory.getLogger(MonitoringRuleBootstrapper.class);
  private static final String SYSTEM = "system";

  public static void addBootstrappedRulesToStore(RuleStore ruleStore) {
    for (BootStrappedRule bootStrappedRule : BootstrappedRules.bootstrappedRules) {
      try {
        Rule rule =
            new Rule(bootStrappedRule.getRuleName(), bootStrappedRule.getRuleExpression(), SYSTEM);
        ruleStore.createRule(rule);
      } catch (InvalidRuleException e) {
        logger.warn("Rule {} is not valid. Skipping", bootStrappedRule.getRuleExpression());
      }
    }
  }
}
