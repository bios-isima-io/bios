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

import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.models.TenantConfig;
import org.hamcrest.junit.ExpectedException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class TenantValidatorTest {
  private static Validators validators;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    validators = new Validators();
  }

  @Test
  public void testNormal() throws Exception {
    validators.validateTenant(new TenantConfig("normal"));
  }

  @Test
  public void testNameWithUnderscore() throws Exception {
    validators.validateTenant(new TenantConfig("hello_world_"));
  }

  @Test
  public void testNameWithLength1() throws Exception {
    validators.validateTenant(new TenantConfig("i"));
  }

  @Test
  public void testNameWithLength40() throws Exception {
    validators.validateTenant(new TenantConfig("0123456789012345678901234567890123456789"));
  }

  @Test
  public void testNameWithLength41() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage("tenantConfig.tenantName:");
    validators.validateTenant(new TenantConfig("0123456789012345678901234567890123456789x"));
  }

  @Test
  public void testNullTenantName() throws Exception {
    thrown.expect(ConstraintViolationValidatorException.class);
    thrown.expectMessage("tenantConfig.tenantName:");
    validators.validateTenant(new TenantConfig(null));
  }

  @Test
  public void testBlankTenantName() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage("tenantConfig.tenantName:");
    validators.validateTenant(new TenantConfig(" "));
  }

  @Test
  public void testTenantNameWithUnderscore() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage("tenantConfig.tenantName:");
    validators.validateTenant(new TenantConfig("_this_is_invalid"));
  }

  @Test
  public void testTenantNameWithDot() throws Exception {
    thrown.expect(InvalidValueValidatorException.class);
    thrown.expectMessage("tenantConfig.tenantName:");
    validators.validateTenant(new TenantConfig("this.is.invalid"));
  }
}
