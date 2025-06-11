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
package io.isima.bios.models;

import io.isima.bios.models.v1.validators.ValidatorConstants;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

public class UserOperationRequest {

  @NotNull UserOperation operation;

  @Size(min = 1, max = ValidatorConstants.USERNAME_MAX_LENGTH)
  private String username;

  @Size(min = 1, max = 40)
  private String tenant;

  @Size(min = 1, max = ValidatorConstants.PASSWORD_MAX_LENGTH)
  @Pattern(regexp = ValidatorConstants.PASSWORD_PATTERN)
  private String password;

  public UserOperation getOperation() {
    return operation;
  }

  public void setOperation(UserOperation operation) {
    this.operation = operation;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public UserOperationRequest() {}
}
