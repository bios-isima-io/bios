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
package io.isima.bios.auth;

import io.isima.bios.models.v1.Permission;
import java.util.List;
import java.util.Objects;

/** This class provides allowed roles for classes of operations. */
public class AllowedRoles {

  /**
   * Checks whether a user has a permission to do something.
   *
   * @param allowedRoles Allowed roles to do something
   * @param userRoles User's roles
   * @return True if allowed, else false
   */
  public static boolean isAllowed(List<Permission> allowedRoles, List<Permission> userRoles) {
    Objects.requireNonNull(allowedRoles);
    Objects.requireNonNull(userRoles);
    if (allowedRoles.isEmpty() || userRoles.isEmpty()) {
      return true;
    }
    return allowedRoles.stream().anyMatch((permission) -> userRoles.contains(permission));
  }

  /** Permitted roles for system admin operations. */
  public static final List<Permission> SYSADMIN = List.of(Permission.SUPERADMIN);

  /** Permitted roles for TENANT Read operations. */
  public static final List<Permission> TENANT_READ =
      List.of(Permission.SUPERADMIN, Permission.ADMIN);

  /**
   * Permitted roles for TENANT Write operations. The system admin is excluded since the configs of
   * the system tenant are read only.
   */
  public static final List<Permission> TENANT_WRITE = List.of(Permission.ADMIN);

  /** Permitted roles for Signal Read operations. */
  public static final List<Permission> SIGNAL_READ =
      List.of(
          Permission.SUPERADMIN,
          Permission.ADMIN,
          Permission.INGEST,
          Permission.EXTRACT,
          Permission.INGEST_EXTRACT,
          Permission.BI_REPORT,
          Permission.APP_MASTER);

  /** Permitted roles for Context Read operations. */
  public static final List<Permission> CONTEXT_READ =
      List.of(
          Permission.SUPERADMIN,
          Permission.ADMIN,
          Permission.INGEST,
          Permission.EXTRACT,
          Permission.INGEST_EXTRACT,
          Permission.BI_REPORT,
          Permission.APP_MASTER);

  /** Permitted roles for Stream Write operations. */
  public static final List<Permission> STREAM_WRITE =
      List.of(Permission.SUPERADMIN, Permission.ADMIN, Permission.INGEST_EXTRACT);

  /** Permitted roles for signal data extraction. */
  public static final List<Permission> SIGNAL_DATA_READ =
      List.of(
          Permission.SUPERADMIN,
          Permission.ADMIN,
          Permission.BI_REPORT,
          Permission.EXTRACT,
          Permission.INGEST_EXTRACT);

  /** Permitted roles for context data extraction. */
  public static final List<Permission> CONTEXT_DATA_READ =
      List.of(
          Permission.SUPERADMIN,
          Permission.ADMIN,
          Permission.BI_REPORT,
          Permission.EXTRACT,
          Permission.INGEST_EXTRACT);

  /** Permitted roles for writing context and signal data. */
  public static final List<Permission> DATA_WRITE =
      List.of(
          Permission.SUPERADMIN, Permission.ADMIN, Permission.INGEST, Permission.INGEST_EXTRACT);

  /** Permitted roles for REPORTS CRUD operations. */
  public static final List<Permission> REPORTS =
      List.of(Permission.SUPERADMIN, Permission.BI_REPORT);

  /** Permitted roles for cross-tenant app service operations. */
  public static final List<Permission> APP_MASTER = List.of(Permission.APP_MASTER);

  /** Permitted for any authenticated user. */
  public static final List<Permission> ANY = List.of();
}
