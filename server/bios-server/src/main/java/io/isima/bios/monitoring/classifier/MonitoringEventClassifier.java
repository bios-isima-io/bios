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
package io.isima.bios.monitoring.classifier;

import io.isima.bios.models.Event;
import io.isima.bios.vigilantt.classifiers.ClassificationInfo;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.evaluator.ExpressionEvaluator;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the list of active compiled monitoring rules. The implementation
 * evaluates the incoming event against all the compiled rules and returns ClassificationInfo which
 * contains details of all the rules which are violated
 */
public class MonitoringEventClassifier implements EventClassifier {
  private final Logger logger = LoggerFactory.getLogger(MonitoringEventClassifier.class);

  private final List<CompiledRule> compiledRuleList;

  public MonitoringEventClassifier(List<CompiledRule> compiledRuleList) {
    this.compiledRuleList = compiledRuleList;
  }

  public ClassificationInfo classify(Event event) {
    ClassificationInfo classificationInfo = new ClassificationInfo(Boolean.FALSE);
    for (CompiledRule compiledRule : compiledRuleList) {
      try {
        ExpressionTreeNode node = compiledRule.getExpressionTreeNode();
        ExpressionEvaluator.evaluateExpression(node, event);
        if ((Boolean) node.getResult()) {
          logger.trace("Rule {} is violated", compiledRule.getRuleName());
          classificationInfo.setIsAnomaly(Boolean.TRUE);
          List<String> violatedRules = classificationInfo.getViolatedRules();
          violatedRules.add(compiledRule.getRuleName());
          classificationInfo.setViolatedRules(violatedRules);
        }
      } catch (UnexpectedValueException e) {
        logger.debug("Got UnexpectedValueException while processing event {}", event);
      }
    }
    return classificationInfo;
  }

  public List<CompiledRule> getCompiledRuleList() {
    return compiledRuleList;
  }
}
