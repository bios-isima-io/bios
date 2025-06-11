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
import io.isima.bios.vigilantt.grammar.tokenizer.Token;

public abstract class ExpressionTreeNode {

  protected final Token token;
  protected ExpressionTreeNode left;
  protected ExpressionTreeNode right;
  protected Object result;

  public ExpressionTreeNode(Token token) {
    this.token = token;
    left = null;
    right = null;
    result = null;
  }

  public void setLeftChild(ExpressionTreeNode node) {
    this.left = node;
  }

  public void setRightChild(ExpressionTreeNode node) {
    this.right = node;
  }

  public Token getToken() {
    return token;
  }

  public ExpressionTreeNode getLeft() {
    return left;
  }

  public ExpressionTreeNode getRight() {
    return right;
  }

  public Object getResult() {
    return result;
  }

  public abstract void computeResult(Event event) throws UnexpectedValueException;

  public String printTree() {
    String delimiter = "";
    StringBuilder sb = new StringBuilder();
    if (left != null) {
      sb.append(left.printTree());
      delimiter = ".";
    }
    sb.append(delimiter).append(token.getContent());
    delimiter = ".";
    if (right != null) {
      sb.append(delimiter).append(right.printTree());
    }
    return sb.toString();
  }

  public int getHeight() {
    int leftSubtreeHeight = left == null ? 0 : left.getHeight();
    int rightSubtreeHeight = right == null ? 0 : right.getHeight();
    return Math.max(leftSubtreeHeight, rightSubtreeHeight) + 1;
  }

  public boolean isLeafNode() {
    return left == null && right == null;
  }
}
