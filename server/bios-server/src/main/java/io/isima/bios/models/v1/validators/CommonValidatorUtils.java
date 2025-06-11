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

import javax.validation.ConstraintValidatorContext;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

/** Class to provide utility methods used in this validator package. */
public class CommonValidatorUtils {

  /**
   * Method to unwrap {@link ConstraintValidatorContext} to Hibernate implementation.
   *
   * <p>The method also strips off the first default violation message to prepare for inserting a
   * dynamically-generated message.
   *
   * @param context Context as generic ConstraintValidatorContext
   * @return Context as HibernateConstraintValidatorContext.
   */
  // TODO(TFOS-131): The method assumes Hibernate as the validator provider. If we use a different
  // provider, the method throws ValidationException.
  public static HibernateConstraintValidatorContext prepareReport(
      ConstraintValidatorContext context) {
    HibernateConstraintValidatorContext hibernateContext =
        context.unwrap(HibernateConstraintValidatorContext.class);
    hibernateContext.disableDefaultConstraintViolation();
    return hibernateContext;
  }
}
