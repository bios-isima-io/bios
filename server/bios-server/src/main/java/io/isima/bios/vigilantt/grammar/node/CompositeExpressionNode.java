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
import io.isima.bios.vigilantt.exceptions.UnexpectedValueException;
import io.isima.bios.vigilantt.grammar.combiners.Composition;
import io.isima.bios.vigilantt.grammar.combiners.CompositionFactory;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.utils.ComputationUtil;

public class CompositeExpressionNode extends ExpressionTreeNode {

  public CompositeExpressionNode(Token token) {
    super(token);
  }

  @Override
  public void computeResult(Event event) throws UnexpectedValueException {
    Boolean leftResult = ComputationUtil.getValueAsBoolean(left.result);
    Boolean rightResult = ComputationUtil.getValueAsBoolean(right.result);

    Composition composition = CompositionFactory.getComposition(token.getContent());
    result = composition.apply(leftResult, rightResult);
  }
}
