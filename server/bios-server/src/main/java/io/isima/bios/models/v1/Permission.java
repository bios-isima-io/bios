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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.isima.bios.models.Role;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum Permission {
  EXTRACT("Extract", Role.EXTRACT),

  INGEST("Ingest", Role.INGEST),

  INGEST_EXTRACT("Ingest + Extract", Role.SCHEMA_EXTRACT_INGEST),

  ADMIN("Admin", Role.TENANT_ADMIN),

  SUPERADMIN("SuperAdmin", Role.SYSTEM_ADMIN),

  BI_REPORT("Business user - create reports", Role.REPORT),

  DEVELOPER("Developer", Role.NA),

  INTERNAL("Internal", Role.INTERNAL),

  APP_MASTER("App Master", Role.APP_MASTER),
  ;

  private String displayName;
  private Role biosRole;

  Permission(String text, Role biosRole) {
    this.displayName = text;
    this.biosRole = biosRole;
  }

  /**
   * Method to get Permission id.
   *
   * @return permission id
   */
  public int getId() {
    return ordinal() + 1;
  }

  /**
   * Method to get Permission display name.
   *
   * @return permission display name
   */
  @JsonGetter("name")
  public String getDisplayName() {
    return this.displayName;
  }

  /**
   * Converts to BIOS Role.
   *
   * @return The BIOS Role. If there is no corresponding role, Role.NA is returned.
   */
  public Role toBios() {
    return biosRole;
  }

  /**
   * Gets the Permission for specified BIOS Role.
   *
   * @param role The BIOS role.
   * @return Corresponding TFOS Permission. Null is returned when there is no match.
   */
  public static Permission forBios(Role role) {
    Objects.requireNonNull(role);
    var permission = bios2tfos.get(role);
    if (permission == null) {
      throw new IllegalArgumentException("No Permission entry found for Role " + role);
    }
    return permission;
  }

  private static final Map<String, Permission> lookupByDisplayName = new HashMap<>();
  private static final Map<Integer, Permission> lookupById = new HashMap<>();
  private static final Map<Role, Permission> bios2tfos = new HashMap<>();

  static {
    for (Permission permission : Permission.values()) {
      lookupByDisplayName.put(permission.getDisplayName(), permission);
      lookupById.put(permission.getId(), permission);
      bios2tfos.put(permission.biosRole, permission);
    }
  }

  /**
   * Method to get Permission instance from id.
   *
   * @param id permission id
   * @return permission instance
   */
  public static Permission getById(int id) {
    return lookupById.get(id);
  }

  /**
   * Method to get Permission instance from id's.
   *
   * @param permIds List of permission id
   * @return List of permission instances
   */
  public static List<Permission> getByIds(List<Integer> permIds) {
    if (permIds == null || permIds.isEmpty()) {
      return List.of();
    }
    List<Permission> permissions = new ArrayList<>(permIds.size());
    permIds.forEach(
        id -> {
          if (lookupById.containsKey(id)) {
            permissions.add(lookupById.get(id));
          }
        });
    return permissions;
  }

  /**
   * Method to parse comma separated permission id's.
   *
   * @param permIdsStr Comma separated permission id's
   * @return List of permission id's
   */
  public static List<Integer> parse(String permIdsStr) {
    if (Strings.isNullOrEmpty(permIdsStr)) {
      return Collections.emptyList();
    }

    String[] tokens = permIdsStr.split(",");
    List<Integer> permissions = new ArrayList<>(tokens.length);
    for (String token : tokens) {
      permissions.add(Integer.parseInt(token));
    }

    return permissions;
  }

  @Override
  public String toString() {
    return displayName;
  }

  /**
   * Method to deserialize JSON to Permission instance.
   *
   * @param value Value to deserialize
   * @return permission instance
   * @throws IllegalArgumentException for invalid name
   */
  @JsonCreator
  public static Permission fromName(String value) {
    if (lookupByDisplayName.containsKey(value)) {
      return lookupByDisplayName.get(value);
    }

    try {
      int id = Integer.parseInt(value);
      if (lookupById.containsKey(id)) {
        return lookupById.get(id);
      } else {
        throw new IllegalArgumentException(String.format("'%s' is invalid", value));
      }
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(String.format("'%s' is invalid", value));
    }
  }

  public static List<Permission> getAllPermissions() {
    return Lists.newArrayList(values());
  }
}
