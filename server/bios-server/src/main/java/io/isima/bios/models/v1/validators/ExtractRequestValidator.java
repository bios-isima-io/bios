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

import io.isima.bios.models.ExtractRequest;
import java.util.HashSet;
import java.util.Set;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class ExtractRequestValidator
    implements ConstraintValidator<ValidExtractRequest, ExtractRequest> {
  @Override
  public void initialize(ValidExtractRequest request) {}

  @Override
  public boolean isValid(ExtractRequest extractRequest, ConstraintValidatorContext context) {
    if (extractRequest == null) {
      return true;
    }

    if (extractRequest.getAttributes() != null) {
      int index = 0;
      Set<String> names = new HashSet<>();
      for (String attribute : extractRequest.getAttributes()) {
        if (attribute == null || attribute.isEmpty()) {
          CommonValidatorUtils.prepareReport(context)
              .buildConstraintViolationWithTemplate("may not be null or empty")
              .addPropertyNode("attributes[" + index + "]")
              .addConstraintViolation();
          return false;
        }
        if (names.contains(attribute)) {
          CommonValidatorUtils.prepareReport(context)
              .buildConstraintViolationWithTemplate("may not contain duplicate names")
              .addPropertyNode("attributes[" + index + "]")
              .addConstraintViolation();
          return false;
        }
        names.add(attribute);
        ++index;
      }
    }

    return true;
  }
}
