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
package io.isima.bios.monitoring.factories;

import io.isima.bios.monitoring.classifier.CompiledRule;
import io.isima.bios.monitoring.classifier.MonitoringEventClassifier;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.parser.ExpressionParser;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.RuleStore;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class to prepare and create an instance of MonitoringEventClassifier. */
public class MonitoringEventClassifierFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(MonitoringEventClassifierFactory.class);

  public static EventClassifier getClassifier() {
    RuleStore ruleStore = MonitoringRuleStoreFactory.getRuleStore(StoreType.MONITORING);
    ExpressionParser parser = new ExpressionParser();
    List<CompiledRule> compiledRuleList = new ArrayList<>();
    for (Rule rule : ruleStore.getAllRules()) {
      try {
        ExpressionTreeNode node = parser.processExpression(rule.getRuleExpression());
        compiledRuleList.add(new CompiledRule(rule.getRuleName(), node));
      } catch (InvalidRuleException e) {
        logger.debug("Rule {} is not valid", rule.getRuleName());
        // Ignore
      }
    }
    return new MonitoringEventClassifier(compiledRuleList);
  }
}
