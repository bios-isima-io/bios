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
package io.isima.bios.admin.v1.impl;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.vigilantt.exceptions.InvalidRuleException;
import io.isima.bios.vigilantt.grammar.node.AttributeNode;
import io.isima.bios.vigilantt.grammar.node.ExpressionTreeNode;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AlertValidator {

  public static void validateAttributesInExpressionTree(
      String tenantName, String streamName, ExpressionTreeNode node, Set<String> allowedAttributes)
      throws InvalidRuleException {
    if (node.getLeft() != null) {
      validateAttributesInExpressionTree(tenantName, streamName, node.getLeft(), allowedAttributes);
    }

    if (node instanceof AttributeNode) {
      String attributeName = node.getToken().getContent();
      if (!allowedAttributes.contains(attributeName)) {
        throw new InvalidRuleException(
            String.format(
                "Invalid attribute in alert condition; tenant=%s, stream=%s, attribute=%s",
                tenantName, streamName, attributeName));
      }
    }

    if (node.getRight() != null) {
      validateAttributesInExpressionTree(
          tenantName, streamName, node.getRight(), allowedAttributes);
    }
  }

  public static Set<String> getAllowedAttributesInAlertCondition(
      PostprocessDesc postprocessDesc, StreamDesc streamDesc) {
    final Set<String> allowedAttributes = new HashSet<>();
    final List<ViewDesc> viewDescList = streamDesc.getViews();
    if (viewDescList != null) {
      final ViewDesc viewForPostProcess =
          viewDescList.stream()
              .filter(view -> postprocessDesc.getView().equals(view.getName()))
              .findFirst()
              .orElse(null);
      if (viewForPostProcess != null) {
        allowedAttributes.add("count()");
        for (String rolledUpAttr : viewForPostProcess.getAttributes()) {
          allowedAttributes.add("sum(" + rolledUpAttr + ")");
          allowedAttributes.add("min(" + rolledUpAttr + ")");
          allowedAttributes.add("max(" + rolledUpAttr + ")");
        }
        for (var dimension : viewForPostProcess.getGroupBy()) {
          allowedAttributes.add(dimension);
        }
      }
    }
    return allowedAttributes;
  }

  public static void validateUniqueAlertName(Set<String> uniqueNames, String alertName)
      throws InvalidRuleException {
    if (uniqueNames.contains(alertName.toLowerCase())) {
      throw new InvalidRuleException(String.format("Duplicate alert with name %s", alertName));
    } else {
      uniqueNames.add(alertName.toLowerCase());
    }
  }
}
