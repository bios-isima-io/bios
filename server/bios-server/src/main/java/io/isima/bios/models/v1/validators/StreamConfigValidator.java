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
package io.isima.bios.models.v1.validators;

import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

/**
 * Validator of Stream configuration.
 *
 * <p>The validator checks following constraints.
 *
 * <dl>
 *   <li>Stream name must be set.
 *   <li>Attributes may not have duplicate names.
 * </dl>
 */
public class StreamConfigValidator implements ConstraintValidator<ValidStreamConfig, StreamConfig> {
  @Override
  public void initialize(ValidStreamConfig arg0) {}

  @Override
  public boolean isValid(StreamConfig streamConfig, ConstraintValidatorContext context) {
    if (streamConfig == null) {
      return true;
    }

    // verify attribute uniqueness
    Set<String> names = new HashSet<>();
    if (!verifyAttributeUniqueness(
        streamConfig.getName(), streamConfig.getAttributes(), names, context)) {
      return false;
    }
    if (!verifyAttributeUniqueness(
        streamConfig.getName(), streamConfig.getAdditionalAttributes(), names, context)) {
      return false;
    }
    if (streamConfig.getPreprocesses() != null) {
      for (PreprocessDesc processor : streamConfig.getPreprocesses()) {
        if (processor.getActions() != null) {
          for (ActionDesc action : processor.getActions()) {
            final String actionAttribute =
                (action.getAs() != null) ? action.getAs() : action.getAttribute();
            if (!verifyAttributeUniqueness(
                streamConfig.getName(),
                actionAttribute.toLowerCase(),
                "Stream '${stream}' has duplicate attribute names '${attr}' in preprocess config",
                names,
                context)) {
              return false;
            }
          }
        }
      }
    }

    if (streamConfig.getType() == StreamType.CONTEXT && names.size() < 2) {
      prepareReport(context, streamConfig.getName())
          .buildConstraintViolationWithTemplate(
              "Context type stream '${stream}' must have at least two attributes")
          .addConstraintViolation();
      return false;
    }

    return true;
  }

  private boolean verifyAttributeUniqueness(
      String streamName,
      String attr,
      String template,
      Set<String> names,
      ConstraintValidatorContext context) {
    if (attr == null) {
      return true;
    }
    if (names.contains(attr)) {
      prepareReport(context, streamName)
          .addExpressionVariable("attr", attr)
          .buildConstraintViolationWithTemplate(template)
          .addConstraintViolation();
      return false;
    }
    names.add(attr);
    return true;
  }

  private boolean verifyAttributeUniqueness(
      String streamName,
      List<AttributeDesc> attributeDescs,
      Set<String> names,
      ConstraintValidatorContext context) {
    if (attributeDescs == null) {
      return true;
    }
    if (streamName == null) {
      prepareReport(context, streamName)
          .buildConstraintViolationWithTemplate("Stream name is not set.")
          .addConstraintViolation();
    }
    for (AttributeDesc attributeDesc : attributeDescs) {
      String name = attributeDesc.getName().toLowerCase();
      if (name == null || name.isEmpty()) {
        prepareReport(context, streamName)
            .buildConstraintViolationWithTemplate(
                "Stream '${stream}' has an attribute without name")
            .addConstraintViolation();
        return false;
      }
      if (!verifyAttributeUniqueness(
          streamName,
          name,
          "Stream '${stream}' has duplicate attribute names '${attr}'",
          names,
          context)) {
        return false;
      }
    }
    return true;
  }

  private HibernateConstraintValidatorContext prepareReport(
      ConstraintValidatorContext context, String streamName) {
    return ValidatorUtils.prepareReport(context).addExpressionVariable("stream", streamName);
  }
}
