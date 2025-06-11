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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttributeNode extends ExpressionTreeNode {

  private static final Logger logger = LoggerFactory.getLogger(AttributeNode.class);

  public AttributeNode(Token token) {
    super(token);
  }

  @Override
  public void computeResult(Event event) throws UnexpectedValueException {
    String attributeString = token.getContent();
    Map<String, Object> eventAttributes = event.getAttributes();
    logger.trace("Trying to fetch value for attribute {} ", attributeString);
    if (eventAttributes.containsKey(attributeString)) {
      this.result = eventAttributes.get(attributeString);
    } else {
      logger.trace("No value found for attribute {}", attributeString);
      throw new UnexpectedValueException(
          String.format("Unexpected attribute %s in the expression", attributeString));
    }
    logger.trace("Got value {} for attribute {}", result, attributeString);
  }
}
