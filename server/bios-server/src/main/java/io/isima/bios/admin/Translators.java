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
package io.isima.bios.admin;

import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.v1.StreamConfig;
import java.util.ArrayList;
import java.util.Objects;

/** Class that is used temporarily to translate between TFOS and BIOS schema objects. */
public class Translators {

  public static io.isima.bios.models.v1.TenantConfig toTfosLeast(TenantConfig biosTenant)
      throws ValidatorException {
    Objects.requireNonNull(biosTenant);
    final var tfosTenant = new io.isima.bios.models.v1.TenantConfig();
    tfosTenant.setName(validateNonEmptyString(biosTenant.getName(), "tenantConfig.tenantName"));
    // tfosTenant.setVersion(validateGenericValue(biosTenant.getVersion(), "tenantConfig.version"));
    tfosTenant.setDomain(biosTenant.getDomain());
    tfosTenant.setStreams(new ArrayList<>());
    tfosTenant.setAppMaster(biosTenant.getAppMaster());

    return tfosTenant;
  }

  public static TenantConfig toBiosLeast(TenantDesc tfosTenant) {
    final var biosTenant = new TenantConfig();
    biosTenant.setName(tfosTenant.getName());
    biosTenant.setVersion(tfosTenant.getVersion());
    biosTenant.setAppMaster(tfosTenant.getAppMaster());
    return biosTenant;
  }

  public static StreamConfig toTfos(SignalConfig signalConfig)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {
    return StreamConfigTranslator.toTfos(signalConfig);
  }

  public static StreamConfig toTfos(ContextConfig contextConfig)
      throws ConstraintViolationValidatorException, NotImplementedValidatorException {
    return StreamConfigTranslator.toTfos(contextConfig);
  }

  static String validateNonEmptyString(String value, String name)
      throws ConstraintViolationValidatorException {
    Objects.requireNonNull(name);
    if (value == null || value.isBlank()) {
      throw new ConstraintViolationValidatorException(
          String.format("Property '%s' must be a non-empty string", name));
    }
    return value;
  }

  static <T> T validateGenericValue(T value, String name)
      throws ConstraintViolationValidatorException {
    Objects.requireNonNull(name);
    if (value == null) {
      throw new ConstraintViolationValidatorException(
          String.format("Property '%s' must be set", name));
    }
    return value;
  }
}
