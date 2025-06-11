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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.isima.bios.models.BaseModel;
import io.isima.bios.models.DevInstanceConfig;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.UserConfig;
import io.isima.bios.models.v1.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
@Getter
@Setter
public class User extends BaseModel {
  public static final String TABLE_NAME = "users";
  public static final String KEY_NAME = "User";

  private Long orgId;

  private Long id;

  private String email;
  private String password;

  private String permissions = "";

  private String accessId = "";
  private String secretKey = "";

  private String imageUrl;

  private List<Long> groupIds;

  private List<Long> favoriteReports;
  private List<Long> favoriteDashboards;

  private String sshKey;

  private String sshUser;

  private String sshIp;

  private String devInstance;

  private long sshPort;

  private long cloudId;

  private MemberStatus status;

  public User() {
    this.key = KEY_NAME;
  }

  public void addGroupId(Long groupId) {
    if (this.groupIds == null) {
      this.groupIds = new ArrayList<>();
    }
    this.groupIds.add(groupId);
  }

  public long getSshPort() {
    if (sshPort != 0) {
      return sshPort;
    } else {
      return 22;
    }
  }

  public UserConfig toUserConfig() {
    final var userConfig = new UserConfig();
    userConfig.setUserId(id.toString());
    userConfig.setEmail(email.toLowerCase());
    userConfig.setFullName(getName());
    userConfig.setRoles(
        Permission.getByIds(Permission.parse(permissions)).stream()
            .map((perm) -> perm.toBios())
            .collect(Collectors.toList()));
    userConfig.setStatus(status);
    final var devInstanceConfig = new DevInstanceConfig();
    devInstanceConfig.setDevInstanceName(devInstance);
    devInstanceConfig.setSshIp(sshIp);
    devInstanceConfig.setSshKey(sshKey);
    devInstanceConfig.setSshPort((int) sshPort);
    devInstanceConfig.setSshUser(sshUser);
    userConfig.setDevInstance(devInstanceConfig);
    userConfig.setCreateTimestamp(getCreatedAt().getTime());
    userConfig.setModifyTimestamp(getUpdatedAt().getTime());
    return userConfig;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    User user = (User) obj;
    return Objects.equals(this.getKey(), user.getKey())
        && Objects.equals(this.orgId, user.orgId)
        && Objects.equals(this.id, user.id)
        && Objects.equals(this.name, user.name)
        && Objects.equals(this.email, user.email)
        && Objects.equals(this.password, user.password)
        && Objects.equals(this.permissions, user.permissions)
        && Objects.equals(this.accessId, user.accessId)
        && Objects.equals(this.secretKey, user.secretKey)
        && Objects.equals(this.imageUrl, user.imageUrl)
        && Objects.equals(this.getCreatedAt(), user.getCreatedAt())
        && Objects.equals(this.getUpdatedAt(), user.getUpdatedAt())
        && Objects.equals(this.groupIds, user.groupIds)
        && Objects.equals(this.favoriteReports, user.favoriteReports)
        && Objects.equals(this.favoriteDashboards, user.favoriteDashboards)
        && Objects.equals(this.sshKey, user.sshKey)
        && Objects.equals(this.sshIp, user.sshIp)
        && Objects.equals(this.sshUser, user.sshUser)
        && Objects.equals(this.status, user.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        key,
        orgId,
        id,
        name,
        email,
        password,
        permissions,
        accessId,
        secretKey,
        imageUrl,
        createdAt,
        updatedAt,
        groupIds,
        favoriteReports,
        favoriteDashboards,
        sshKey,
        sshIp,
        sshUser,
        status);
  }
}
