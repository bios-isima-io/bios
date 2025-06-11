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

import io.isima.bios.models.v1.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserContext {
  public static final String SCOPE_TENANT = "/tenants/%s/";
  public static final String SCOPE_ROOT = "/";

  private Long userId;
  private Long orgId;
  private String subject;
  private String tenant;
  private List<Integer> permissions;
  private String scope;
  private Long sessionExpiry;
  private String clientVersion;
  private String newSessionToken;
  private String appName;
  private AppType appType;

  // used for signup/registration
  private String domain;
  private List<String> additionalUsers;
  private String source;

  private Boolean isAppMaster;

  public UserContext() {
    super();
  }

  public UserContext(
      Long userId,
      Long orgId,
      String subject,
      String tenant,
      List<Integer> permissions,
      String appName,
      AppType appType) {
    super();
    this.userId = userId;
    this.orgId = orgId;
    this.subject = subject;
    this.tenant = tenant;
    this.permissions = permissions;
    this.appName = appName;
    this.appType = appType;
  }

  public UserContext(UserContext src) {
    this.userId = src.userId;
    this.orgId = src.orgId;
    this.subject = src.subject;
    this.tenant = src.tenant;
    this.permissions = new ArrayList<>(src.permissions);
    this.appName = src.appName;
    this.appType = src.appType;
    this.domain = src.domain;
    this.additionalUsers = src.additionalUsers;
  }

  public String getScope() {
    if (scope == null && tenant != null) {
      if (tenant.equals(SCOPE_ROOT)) {
        return tenant;
      } else {
        return String.format(SCOPE_TENANT, tenant);
      }
    }
    return scope;
  }

  /**
   * Method get id of permissions.
   *
   * @return the permissions
   */
  public List<Integer> getPermissionIds() {
    if (permissions == null || permissions.isEmpty()) {
      return Collections.emptyList();
    }
    return permissions;
  }

  /**
   * Method get permissions.
   *
   * @return the permissions
   */
  public List<Permission> getPermissions() {
    if (permissions == null || permissions.isEmpty()) {
      return Collections.emptyList();
    }
    return Permission.getByIds(permissions);
  }

  /**
   * Method to set permissions.
   *
   * @param permissions the permissions to set
   */
  public void setPermissions(List<Integer> permissions) {
    this.permissions = permissions;
  }

  /**
   * Method to add permissions.
   *
   * @param permIds the permission id's to add
   */
  public void addPermissionIds(Collection<Integer> permIds) {
    if (permissions == null) {
      permissions = new ArrayList<>();
    }
    permissions.addAll(permIds);
  }

  /**
   * Method to add permissions.
   *
   * @param perms the permissions to add
   */
  public void addPermissions(Collection<Permission> perms) {
    if (permissions == null) {
      permissions = new ArrayList<>();
    }
    perms.forEach(perm -> permissions.add(perm.getId()));
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{")
            .append("subject=")
            .append(subject)
            .append(", tenant=")
            .append(tenant)
            .append(", permissions=")
            .append(permissions)
            .append(", appName=")
            .append(appName)
            .append(", appType=")
            .append(appType)
            .append(", domain=")
            .append(domain)
            .append(", additionalUsers=")
            .append(additionalUsers);
    return sb.append("}").toString();
  }
}
