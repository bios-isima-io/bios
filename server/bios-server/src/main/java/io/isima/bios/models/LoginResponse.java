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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.models.v1.Permission;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** POJO for returning login response. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class LoginResponse {

  public static enum AuthCode {
    SUCCESS("success"),
    MISMATCH_EMAIL("mismatch email"),
    EXPIRED_TOKEN("expired token"),
    INVALID_TOKEN("invalid token"),
    INVALID_DATA("invalid data"),
    INSUFFICIENT_DATA("insufficient data"),
    ORG_NOT_LINKED("org link missing"),
    INTERNAL_ISSUE("server issue"),
    ;

    private String text;

    AuthCode(String text) {
      this.text = text;
    }

    @JsonValue
    public String getText() {
      return this.text;
    }
  }

  private String token;

  // The parameter is necessary for biOS API. We don't encode/decode this for TFOS service.
  @JsonIgnore private String tenant;

  @JsonIgnore private String userName;

  @Setter private String appName;

  @Setter private AppType appType;

  private Long expiry;

  private Long sessionTimeoutMillis;

  private List<Permission> permissions = List.of();

  @Setter
  @JsonProperty("sessionAttributes")
  private List<SessionAttribute> sessionAttributes = List.of();

  @Setter private AuthCode code = AuthCode.SUCCESS;

  @JsonIgnore private DevInstanceConfig devInstance;

  LoginResponse() {}

  public LoginResponse(AuthCode code) {
    this.code = code;
  }

  public LoginResponse(String token, AuthCode code) {
    this.token = token;
    this.code = code;
  }

  public LoginResponse(
      String token,
      String tenant,
      String userName,
      String appName,
      AppType appType,
      long expiry,
      List<Permission> permissions,
      long sessionTimeoutMillis,
      DevInstanceConfig devInstance) {
    this.token = token;
    this.tenant = tenant;
    this.userName = userName;
    this.appName = appName;
    this.appType = appType;
    this.expiry = expiry;
    this.permissions = permissions;
    this.sessionTimeoutMillis = sessionTimeoutMillis;
    this.devInstance = devInstance;
  }

  /**
   * Used by JSON builder to return only display name of permissions.
   *
   * @return list of permission names
   */
  @JsonGetter(value = "permissions")
  public List<String> serializePermissions() {
    if (permissions != null && !permissions.isEmpty()) {
      List<String> permissionNames = new ArrayList<>(permissions.size());
      permissions.forEach(perm -> permissionNames.add(perm.getDisplayName()));
      return permissionNames;
    } else {
      return null;
    }
  }
}
