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
package io.isima.bios.deli.flow;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.DataPickupSpec.Attribute;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessingUnit {
  private static final Logger logger = LoggerFactory.getLogger(ProcessingUnit.class);

  private final String flowName;
  private final int unitIndex;
  private final List<AttributeSourceSpec> attributeSources;
  private final List<Transformer> transformers;

  public ProcessingUnit(Configuration configuration, String flowName, int unitIndex,
      Attribute attributeSpec) throws InvalidConfigurationException {
    this.flowName = flowName;
    this.unitIndex = unitIndex;

    // parse source attributes
    final var sourceAttribute = attributeSpec.getSourceAttributeName();
    final var sourceAttributes = attributeSpec.getSourceAttributeNames();
    if (sourceAttribute == null && sourceAttributes == null) {
      throw new InvalidConfigurationException(
          "flow=%s, dataPickupSpec[%d]: Either of property 'sourceAttributeName'"
              + " or 'sourceAttributeNames' must be specified",
          flowName, unitIndex);
    }
    attributeSources = parseSourceAttributes(
        sourceAttributes != null ? sourceAttributes : List.of(sourceAttribute));

    // parse transforms
    final var transforms = attributeSpec.getTransforms();
    if (transforms == null) {
      final var as =
          attributeSpec.getAs() != null ? attributeSpec.getAs() : attributeSources.get(0).getName();
      transformers = List.of(new TransparentTransformer(as.toLowerCase()));
    } else {
      final var temp = new ArrayList<Transformer>();
      for (int tindex = 0; tindex < transforms.size(); ++tindex) {
        final var transform = transforms.get(tindex);
        final var rule = transform.getRule();
        final var as = transform.getAs();
        if (rule == null || as == null) {
          throw new InvalidConfigurationException(
              "flow=%s, dataPickupSpec[%d].transforms[%d]:"
                  + " Both of properties 'rule' and 'as' must be set",
              flowName, unitIndex, tindex);
        }
        // final var rule = null; // TODO(Naoki): Jython module?
        // temp.add(rule);
      }
      transformers = Collections.unmodifiableList(temp);
    }
  }

  private List<AttributeSourceSpec> parseSourceAttributes(List<String> sourceAttributes)
      throws InvalidConfigurationException {
    final var specs = new ArrayList<AttributeSourceSpec>();
    for (String attributeSpec : sourceAttributes) {
      String name = null;
      final AttributeSourceSpec.Source source;
      if (attributeSpec.startsWith("$")) {
        final String trim1 = attributeSpec.substring(1);
        if (trim1.startsWith("{")) {
          name = trim1.substring(1);
          if (name.endsWith("}")) {
            name = name.substring(0, name.length() - 1);
          } else {
            throw new InvalidConfigurationException("Syntax error in attribute spec: "
                    + "Missing } in source attribute definition");
          }
        } else {
          name = trim1;
        }
        source = AttributeSourceSpec.Source.ADDITIONAL_ATTRIBUTE;
      } else {
        name = attributeSpec;
        source = AttributeSourceSpec.Source.MESSAGE;
      }
      specs.add(new AttributeSourceSpec(name, source));
    }
    return specs;
  }

  public void process(Map<String, Object> inputRecord, Map<String, Object> outputRecord,
      FlowContext context) {

    final List<Object> sourceData = attributeSources.stream()
        .map((sourceSpec) -> {
          if (sourceSpec.getSource() == AttributeSourceSpec.Source.MESSAGE) {
            return inputRecord.get(sourceSpec.getName());
          } else {
            return context.getAdditionalAttribute(sourceSpec.getName());
          }
        })
        .collect(Collectors.toList());

    for (var transformer : transformers) {
      transformer.transform(sourceData, outputRecord);
    }
  }
}
