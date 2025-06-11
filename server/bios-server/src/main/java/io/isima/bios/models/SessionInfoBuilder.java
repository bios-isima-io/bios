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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.experimental.Accessors;

/** Builder class used for generating a BIOS API Login Response. */
@Accessors(fluent = true, chain = true)
@Setter
public class SessionInfoBuilder {
  private enum Mask {
    EMAIL,
    USER_NAME,
    ROLES,
    TOKEN,
  }

  private static final BiosVersion VERSION_DEFAULT = new BiosVersion(null);
  private static final BiosVersion VERSION_0_9_52 = new BiosVersion(0, 9, 52, false);
  private static final BiosVersion VERSION_0_9_57 = new BiosVersion(0, 9, 57, false);

  private String token;
  private String email;
  private String userName;
  private String tenantName;
  private String appName;
  private AppType appType;
  private Long expiry;
  private DevInstanceConfig devInstance;
  private UpstreamConfig upstreams;
  private String homePageConfig;
  private List<Role> roles;
  private List<SessionAttribute> sessionAttributes;
  private final Set<Mask> masks;

  public static SessionInfoBuilder getBuilder(AuthResponse resp, BiosVersion clientVersion) {
    return new SessionInfoBuilder(clientVersion)
        .token(resp.getToken())
        .email(resp.getEmail())
        .userName(resp.getUserName())
        .tenantName(resp.getTenantName())
        .appName(resp.getAppName())
        .appType(resp.getAppType())
        .expiry(resp.getExpiry())
        .devInstance(resp.getDevInstance())
        .upstreams(resp.getUpstreams())
        .homePageConfig(resp.getHomePageConfig())
        .roles(resp.getRoles())
        .sessionAttributes(resp.getSessionAttributes());
  }

  public static SessionInfoBuilder getBuilder(UserContext userContext) {
    return new SessionInfoBuilder(new BiosVersion(userContext.getClientVersion()))
        .email(userContext.getSubject())
        .tenantName(userContext.getTenant())
        .appName(userContext.getAppName())
        .appType(userContext.getAppType())
        .expiry(userContext.getSessionExpiry())
        .roles(convertPermissions(userContext.getPermissions()));
  }

  private SessionInfoBuilder(BiosVersion clientVersion) {
    if (clientVersion == null) {
      clientVersion = VERSION_DEFAULT;
    }
    if (clientVersion.compareTo(VERSION_0_9_57) >= 0) {
      masks = Set.of(Mask.TOKEN);
    } else if (clientVersion.compareTo(VERSION_0_9_52) >= 0) {
      masks = Set.of();
    } else {
      masks = Set.of(Mask.EMAIL, Mask.USER_NAME, Mask.ROLES);
    }
  }

  public SessionInfo build() {
    final var resp = new SessionInfo();
    if (!masks.contains(Mask.TOKEN)) {
      resp.setToken(token);
    }
    resp.setTenantName(tenantName);
    resp.setAppName(appName);
    resp.setAppType(appType);
    resp.setExpiry(expiry);
    resp.setDevInstance(devInstance);
    resp.setUpstreams(upstreams);
    resp.setHomePageConfig(homePageConfig);
    if (!masks.contains(Mask.EMAIL)) {
      resp.setEmail(email);
    }
    if (!masks.contains(Mask.USER_NAME)) {
      resp.setUserName(userName);
    }
    if (!masks.contains(Mask.ROLES)) {
      resp.setRoles(roles);
    }
    resp.setSessionAttributes(sessionAttributes);
    return resp;
  }

  public static List<Role> convertPermissions(List<Permission> permissions) {
    if (permissions == null) {
      return null;
    }
    return permissions.stream()
        .map((permission) -> permission.toBios())
        .collect(Collectors.toList());
  }
}
