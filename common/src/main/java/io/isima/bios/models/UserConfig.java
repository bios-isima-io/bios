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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** User information. */
@JsonInclude(Include.NON_NULL)
@ToString
@Getter
@Setter
public class UserConfig {
  @JsonProperty("userId")
  private String userId;

  @JsonProperty("email")
  private String email;

  @JsonProperty("fullName")
  private String fullName;

  @JsonProperty("tenantName")
  private String tenantName;

  @JsonProperty("roles")
  private List<Role> roles;

  @JsonProperty("status")
  private MemberStatus status;

  @JsonProperty("password")
  private String password;

  @ToString.Include(name = "password")
  private String passwordMasker() {
    return password != null ? "****" : "null";
  }

  @JsonProperty("passwordHashed")
  private Boolean passwordHashed;

  @JsonProperty("devInstance")
  private DevInstanceConfig devInstance;

  @JsonProperty("homePageConfig")
  private String homePageConfig;

  @JsonProperty("createTimestamp")
  private Long createTimestamp;

  @JsonProperty("modifyTimestamp")
  private Long modifyTimestamp;

  public UserConfig() {
    //
  }

  public UserConfig(UserConfig src) {
    this.email = src.email;
    this.fullName = src.fullName;
    this.tenantName = src.tenantName;
    this.roles = new ArrayList<>(src.roles);
    this.status = src.status;
    this.password = src.password;
    this.passwordHashed = src.passwordHashed;
    this.devInstance = src.devInstance;
    this.homePageConfig = src.homePageConfig;
    this.createTimestamp = src.createTimestamp;
    this.modifyTimestamp = src.modifyTimestamp;
  }
}
