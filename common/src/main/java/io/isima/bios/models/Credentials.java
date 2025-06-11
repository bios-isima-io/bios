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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/** Credentials submitted for login request. */
@AllArgsConstructor
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Credentials {
  /** User's email address. */
  @JsonProperty("email")
  private String email;

  /** User's password. */
  @JsonProperty("password")
  private String password;

  @JsonProperty("appName")
  private String appName;

  @JsonProperty("appType")
  private AppType appType;

  public Credentials() {}

  public Credentials(String email, String password) {
    this.email = email;
    this.password = password;
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder("{");
    String delimiter = "";
    if (email != null) {
      sb.append("email=").append(email);
      delimiter = ", ";
    }
    if (password != null) {
      sb.append(delimiter).append("password=***");
      delimiter = ", ";
    }
    if (appName != null) {
      sb.append("appName=").append(appName);
      delimiter = ", ";
    }
    if (appType != null) {
      sb.append("appType=").append(appType);
    }
    return sb.append("}").toString();
  }
}
