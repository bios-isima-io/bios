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

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import javax.validation.Constraint;
import javax.validation.Payload;

@Retention(RUNTIME)
@Constraint(validatedBy = TenantConfigValidator.class)
public @interface ValidTenantConfig {
  /**
   * Method to return constraint violation message.
   *
   * @return default template string for violation message
   */
  String message() default "{io.isima.bios.models.v1.validators.validstenantconfig}";

  /**
   * For user to customize the target groups.
   *
   * @return
   */
  Class<?>[] groups() default {};

  /**
   * For extensibility purposes.
   *
   * @return
   */
  Class<? extends Payload>[] payload() default {};
}
