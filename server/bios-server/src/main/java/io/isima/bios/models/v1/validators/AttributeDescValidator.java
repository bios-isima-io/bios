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

import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.HashMap;
import java.util.Map;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Validator of AttributeDesc objects.
 *
 * <p>The validator checks following constraints.
 *
 * <dl>
 *   <li>Attribute name may not conflict with reserved names. Reserved names are 'eventId' and
 *       'ingestTimestamp'
 * </dl>
 */
public class AttributeDescValidator
    implements ConstraintValidator<ValidAttributeDesc, AttributeDesc> {
  private static Map<String, String> reservedNames;

  static {
    reservedNames = new HashMap<>();
    reservedNames.put("eventid", "eventId");
    reservedNames.put("ingesttimestamp", "ingestTimestamp");
  }

  @Override
  public void initialize(ValidAttributeDesc arg0) {}

  @Override
  public boolean isValid(AttributeDesc attributeDesc, ConstraintValidatorContext context) {
    if (attributeDesc == null) {
      return true;
    }

    if (attributeDesc.getName() == null || attributeDesc.getName().isEmpty()) {
      ValidatorUtils.prepareReport(context)
          .buildConstraintViolationWithTemplate("Attribute name must be set")
          .addConstraintViolation();
      return false;
    }

    if (attributeDesc.getAttributeType() == null) {
      ValidatorUtils.prepareReport(context)
          .buildConstraintViolationWithTemplate("Attribute type must be set")
          .addConstraintViolation();
      return false;
    }

    String normalizedName = attributeDesc.getName().toLowerCase();
    if (reservedNames.containsKey(normalizedName)) {
      ValidatorUtils.prepareReport(context)
          .addExpressionVariable("attribute", reservedNames.get(normalizedName))
          .buildConstraintViolationWithTemplate("attributeName ${attribute} is reserved.")
          .addConstraintViolation();
      return false;
    }

    // special constraint for enum type
    if (attributeDesc.getAttributeType() == InternalAttributeType.ENUM) {
      if (attributeDesc.getEnum() == null || attributeDesc.getEnum().isEmpty()) {
        ValidatorUtils.prepareReport(context)
            .buildConstraintViolationWithTemplate(
                "Enum type attribute description must contain non-empty list of enum entries")
            .addConstraintViolation();
        return false;
      }
      if (attributeDesc.getDefaultValue() != null
          && !attributeDesc.getEnum().contains(attributeDesc.getDefaultValue())) {
        ValidatorUtils.prepareReport(context)
            .buildConstraintViolationWithTemplate(
                "Default value of enum type should be one of description's enum entries")
            .addConstraintViolation();
        return false;
      }
    }

    return true;
  }
}
