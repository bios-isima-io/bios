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
package io.isima.bios.models.auth;

import io.isima.bios.models.BaseModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Group extends BaseModel {
  public static final String TABLE_NAME = "groups";
  public static final String KEY_NAME = "Group";

  private Long orgId;

  private Long id;

  // This is to distinguish whether system defined or user defined.
  private Boolean systemDefined = false;

  private Boolean enable = true;

  private String permissions = "";
  private List<Long> members;

  public Group() {
    this.key = KEY_NAME;
  }

  public Long getOrgId() {
    return orgId;
  }

  public void setOrgId(Long orgId) {
    this.orgId = orgId;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Boolean isEnable() {
    return enable;
  }

  public void setEnable(Boolean enable) {
    this.enable = enable;
  }

  public Boolean isSystemDefined() {
    return systemDefined;
  }

  public void setSystemDefined(Boolean systemDefined) {
    this.systemDefined = systemDefined;
  }

  public String getPermissions() {
    return permissions;
  }

  public void setPermissions(String permissions) {
    this.permissions = permissions;
  }

  public List<Long> getMembers() {
    return members;
  }

  public void setMembers(List<Long> members) {
    this.members = members;
  }

  public void addMember(Long member) {
    if (this.members == null) {
      this.members = new ArrayList<>();
    }
    this.members.add(member);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Group group = (Group) obj;
    return Objects.equals(this.getKey(), group.getKey())
        && Objects.equals(this.orgId, group.orgId)
        && Objects.equals(this.id, group.id)
        && Objects.equals(this.enable, group.enable)
        && Objects.equals(this.systemDefined, group.systemDefined)
        && Objects.equals(this.members, group.members)
        && Objects.equals(this.getName(), group.getName())
        && Objects.equals(this.permissions, group.permissions)
        && Objects.equals(this.getCreatedAt(), group.getCreatedAt())
        && Objects.equals(this.getUpdatedAt(), group.getUpdatedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getKey(), this.getOrgId(), this.getId());
  }
}
