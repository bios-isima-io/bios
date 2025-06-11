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

import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;

/** This class encapsulates the parsed tree against a specific monitoring rule */
public class CompiledRule {
  private final String ruleName;
  private final ExpressionTreeNode expressionTreeNode;

  public CompiledRule(String ruleName, ExpressionTreeNode expressionTreeNode) {
    this.ruleName = ruleName;
    this.expressionTreeNode = expressionTreeNode;
  }

  public String getRuleName() {
    return ruleName;
  }

  public ExpressionTreeNode getExpressionTreeNode() {
    return expressionTreeNode;
  }
}
