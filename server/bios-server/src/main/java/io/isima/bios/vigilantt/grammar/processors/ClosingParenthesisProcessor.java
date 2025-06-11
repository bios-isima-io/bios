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
package io.isima.bios.vigilantt.grammar.processors;

import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.AttributeNode;
import io.isima.bios.vigilantt.grammar.node.BooleanNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import io.isima.bios.vigilantt.grammar.node.ScalarDoubleNode;
import io.isima.bios.vigilantt.grammar.node.ScalarStringNode;
import io.isima.bios.vigilantt.grammar.tokenizer.Token;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenType;
import io.isima.bios.vigilantt.grammar.tokenizer.TokenizerUtils;
import io.isima.bios.vigilantt.grammar.utils.ParserConstants;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class ClosingParenthesisProcessor implements TokenProcessor {

  private final Stack<ExpressionTreeNode> expressionStack;
  private final Set<TokenType> operationOrCompositionSet =
      new HashSet<>(Arrays.asList(TokenType.OPERATOR, TokenType.COMPOSITE));

  public ClosingParenthesisProcessor(Stack<ExpressionTreeNode> expressionStack) {
    this.expressionStack = expressionStack;
  }

  @Override
  public void process(Token token) throws InvalidRuleException {
    if (expressionStack.size() < 2) {
      throw new InvalidRuleException(
          "Given rule is not valid; not enough expressions" + " before closing parenthesis.");
    }

    try {
      ExpressionTreeNode first = expressionStack.pop();

      if (first.getToken().getType() == TokenType.PARENTHESIS_OPEN) {
        // An empty pair of parentheses must be preceded by a function name?
        final ExpressionTreeNode functionNameNode = expressionStack.pop();
        if (functionNameNode.getToken().getType() != TokenType.STRING) {
          throw new InvalidRuleException("Syntax error: Invalid function expression before ().");
        }
        processMathematicalFormulaOnAttribute(functionNameNode, null);
        return;
      }

      ExpressionTreeNode second = expressionStack.pop();

      if (second.getToken().getType() == TokenType.PARENTHESIS_OPEN) {
        if (first instanceof ScalarDoubleNode) {
          processNoopScalarExpressions(first);
        } else {
          final ExpressionTreeNode functionNameNode = expressionStack.pop();
          processMathematicalFormulaOnAttribute(functionNameNode, first);
        }
      } else {
        ExpressionTreeNode third = expressionStack.pop();
        processOperationsAndCompositions(third, second, first);
      }
    } catch (EmptyStackException e) {
      throw new InvalidRuleException(
          "Given rule is not valid; not enough expressions"
              + " before closing parenthesis. Do not enclose single values in parentheses.");
    }
  }

  /**
   * Handle no-op scalar expressions like (20), (23.49).
   *
   * @param node scalar expression node
   */
  private void processNoopScalarExpressions(ExpressionTreeNode node) {
    expressionStack.push(node);
  }

  /**
   * Handle mathematical expressions. For example - sum(quantity_sold), max(item_price)
   *
   * @param formulaNode Node with mathematical formula
   * @param attributeNode Attribute node
   * @throws InvalidRuleException if the rule string is not valid
   */
  private void processMathematicalFormulaOnAttribute(
      ExpressionTreeNode formulaNode, ExpressionTreeNode attributeNode)
      throws InvalidRuleException {
    String function = formulaNode.getToken().getContent().toLowerCase();
    if (ParserConstants.allowedFunctions.contains(function)) {
      final String attributeString;
      if (function.equalsIgnoreCase("count")) {
        if (attributeNode != null) {
          throw new InvalidRuleException(
              String.format("Function '%s' must not take domains", function));
        }
        attributeString =
            function + TokenizerUtils.OPENING_PARENTHESIS + TokenizerUtils.CLOSING_PARENTHESIS;
      } else if (attributeNode == null) {
        throw new InvalidRuleException(String.format("Function '%s' must take a domain", function));
      } else {
        attributeString =
            function
                + TokenizerUtils.OPENING_PARENTHESIS
                + attributeNode.getToken().getContent()
                + TokenizerUtils.CLOSING_PARENTHESIS;
      }
      Token newToken = new Token(TokenType.STRING, attributeString);
      ExpressionTreeNode node = new AttributeNode(newToken);
      expressionStack.push(node);
    } else {
      throw new InvalidRuleException(
          "Given function "
              + function
              + " is not supported."
              + " Attribute in parentheses must be preceded by a function such as 'sum'.");
    }
  }

  /**
   * Handle operations and compositions. Example - (quantity_sold > 100), (1 AND 2)
   *
   * @param left left operand
   * @param operation operation to perform
   * @param right right operand
   * @throws InvalidRuleException if the rule string is not valid
   */
  private void processOperationsAndCompositions(
      ExpressionTreeNode left, ExpressionTreeNode operation, ExpressionTreeNode right)
      throws InvalidRuleException {
    ExpressionTreeNode previousNode = expressionStack.peek();
    if (previousNode.getToken().getType() != TokenType.PARENTHESIS_OPEN
        || !operationOrCompositionSet.contains(operation.getToken().getType())
        || isInValidChild(left)
        || isInValidChild(right)) {
      throw new InvalidRuleException(
          String.format(
              "Given rule is not valid:{%s}{%s}{%s}{%s}",
              previousNode.getToken().getContent(),
              left.getToken().getContent(),
              operation.getToken().getContent(),
              right.getToken().getContent()));
    }
    expressionStack.pop();
    operation.setLeftChild(left);
    operation.setRightChild(right);
    expressionStack.push(operation);
  }

  /**
   * Determine whether node is an invalid child for operations and compositions.
   *
   * @param node input node
   * @return whether given node is invalid child node
   */
  private boolean isInValidChild(ExpressionTreeNode node) {
    boolean isIntermediateNode = !node.isLeafNode();
    boolean isValidLeafNode =
        (node instanceof AttributeNode)
            || (node instanceof ScalarDoubleNode)
            || (node instanceof ScalarStringNode)
            || (node instanceof BooleanNode);
    return !(isIntermediateNode || isValidLeafNode);
  }
}
