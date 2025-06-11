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

import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

public class TenantConfigValidator implements ConstraintValidator<ValidTenantConfig, TenantConfig> {
  @Override
  public void initialize(ValidTenantConfig arg0) {}

  @Override
  public boolean isValid(TenantConfig tenantConfig, ConstraintValidatorContext context) {
    if (tenantConfig == null) {
      return true;
    }

    Set<String> names = new HashSet<>();
    return verifyStreamUniqueness(
        tenantConfig.getName(), tenantConfig.getStreams(), names, context);
  }

  private boolean verifyStreamUniqueness(
      String tenantName,
      List<StreamConfig> streams,
      Set<String> names,
      ConstraintValidatorContext context) {
    if (streams == null) {
      return true;
    }
    if (tenantName == null) {
      prepareReport(context, tenantName)
          .buildConstraintViolationWithTemplate("Tenant name is not set.")
          .addConstraintViolation();
    }
    for (StreamConfig streamConfig : streams) {
      String name = streamConfig.getName().toLowerCase();
      if (name == null || name.isEmpty()) {
        prepareReport(context, tenantName)
            .buildConstraintViolationWithTemplate("Tenant '${tenant}' has a stream without name")
            .addConstraintViolation();
        return false;
      }
      if (names.contains(name)) {
        prepareReport(context, tenantName)
            .addExpressionVariable("stream", name)
            .buildConstraintViolationWithTemplate(
                "Tenant '${tenant}' has duplicate stream names '${stream}'")
            .addConstraintViolation();
        return false;
      }
      names.add(name);
    }
    return true;
  }

  private HibernateConstraintValidatorContext prepareReport(
      ConstraintValidatorContext context, String tenantName) {
    return ValidatorUtils.prepareReport(context).addExpressionVariable("tenant", tenantName);
  }
}
