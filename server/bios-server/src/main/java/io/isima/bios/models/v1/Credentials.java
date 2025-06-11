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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.AppType;
import io.isima.bios.models.v1.validators.ValidatorConstants;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/** A POJO class to represent a set of Authentication credentials. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@Getter
@Setter
@AllArgsConstructor
public class Credentials {

  @Size(max = ValidatorConstants.USERNAME_MAX_LENGTH)
  private String username;

  @Size(max = ValidatorConstants.PASSWORD_MAX_LENGTH)
  private String password;

  private String scope;

  private String appName;

  private AppType appType;

  public Credentials() {}

  public Credentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public Credentials(String username, String password, String scope) {
    this.username = username;
    this.password = password;
    this.scope = scope;
  }

  public static String createScope(final String tenant) {
    if (tenant == null || tenant.isEmpty() || tenant.equals("/")) {
      return "/";
    }
    return "/tenants/" + tenant + "/";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String delimiter = "";

    if (username != null) {
      sb.append("username=").append(username);
      delimiter = ", ";
    }
    if (password != null) {
      sb.append(delimiter).append("password=").append("***");
      delimiter = ", ";
    }
    if (scope != null) {
      sb.append(delimiter).append("scope=").append(scope);
    }

    return sb.toString();
  }
}
