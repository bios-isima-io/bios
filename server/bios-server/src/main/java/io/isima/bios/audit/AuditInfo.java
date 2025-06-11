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
package io.isima.bios.audit;

import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;

public class AuditInfo {

  private Long operationStartedAt;

  private Long operationFinishedAt;

  private String operation;

  @Setter private String phase = "NA";

  private String user;

  private Long userId;

  private Long organizationId;

  private String tenant;

  private String stream;

  private String status;

  private String permissionATM;

  @Setter private String request;

  private String response;

  public AuditInfo(
      final String stream, final String operation, final String request, final String tenant) {
    this.stream = stream;
    this.operationStartedAt = System.currentTimeMillis();
    this.request = request;
    this.operation = operation;
    this.tenant = tenant;
  }

  public void setOperationFinishedAt(Long operationFinishedAt) {
    this.operationFinishedAt = operationFinishedAt;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setResponse(String response) {
    this.response = response;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setUserId(Long userId) {
    this.userId = userId;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public void setPermissionATM(String permissionATM) {
    this.permissionATM = permissionATM;
  }

  public void setOrganizationId(Long organizationId) {
    this.organizationId = organizationId;
  }

  private Long getOperationStartedAt() {
    if (null == operationStartedAt) {
      return System.currentTimeMillis();
    }
    return operationStartedAt;
  }

  private Long getOperationFinishedAt() {
    if (null == operationFinishedAt) {
      return System.currentTimeMillis();
    }
    return operationFinishedAt;
  }

  private String getOperation() {
    return operation;
  }

  private String getUser() {
    return user;
  }

  private Long getUserId() {
    if (null == this.userId) {
      return 0L;
    }
    return userId;
  }

  private Long getOrganizationId() {
    if (null == this.organizationId) {
      return 0L;
    }
    return organizationId;
  }

  public String getTenant() {
    if (null == this.tenant) {
      return "";
    }
    return tenant;
  }

  private String getStream() {
    if (null == this.stream) {
      return "";
    }
    return stream;
  }

  private String getStatus() {
    return status;
  }

  private String getPermissionATM() {
    if (null == this.permissionATM) {
      return "NULL";
    }
    return permissionATM;
  }

  private String getRequest() {
    if (null == this.request) {
      return "";
    }
    return request;
  }

  private String getResponse() {
    if (null == this.response) {
      return "";
    }
    return response;
  }

  public Map<String, Object> getAuditAtrributes() {
    Map<String, Object> auditAttributes = new HashMap<String, Object>();
    auditAttributes.put(AuditConstants.ATTR_OPERATION_STARTS_AT, getOperationStartedAt());
    auditAttributes.put(AuditConstants.ATTR_OPERATION_FINISHED_AT, getOperationFinishedAt());
    auditAttributes.put(AuditConstants.ATTR_OPERATION, getOperation());
    auditAttributes.put(AuditConstants.ATTR_REQUEST, getRequest());
    auditAttributes.put(AuditConstants.ATTR_RESPONSE, getResponse());
    auditAttributes.put(AuditConstants.ATTR_STATUS, getStatus());
    auditAttributes.put(AuditConstants.ATTR_STREAM, getStream());
    auditAttributes.put(AuditConstants.ATTR_TENANT, getTenant());

    // user info may be null
    auditAttributes.put(AuditConstants.ATTR_ORG_ID, valueOrDefault(getOrganizationId(), -1L));
    auditAttributes.put(AuditConstants.ATTR_PERMISSION, valueOrDefault(getPermissionATM(), ""));
    auditAttributes.put(AuditConstants.ATTR_USER, valueOrDefault(getUser(), ""));
    auditAttributes.put(AuditConstants.ATTR_USER_ID, valueOrDefault(getUserId(), -1L));

    return auditAttributes;
  }

  @SuppressWarnings("unchecked")
  private <T> T valueOrDefault(Object value, T defaultValue) {
    return value != null ? (T) value : defaultValue;
  }

  /** Method to set user information from user context. */
  public void setUserContext(UserContext userContext) {
    if (null != userContext) {
      if (!this.getTenant().isBlank()) {
        this.setTenant(userContext.getTenant());
      }
      this.setUser(userContext.getSubject());
      this.setUserId(userContext.getUserId());
      this.setOrganizationId(userContext.getOrgId());
      this.setPermissionATM(getSerializePermissions(userContext.getPermissions()));
    }
  }

  private String getSerializePermissions(List<Permission> permissions) {
    if (permissions != null && !permissions.isEmpty()) {
      List<String> permissionNames = new ArrayList<>(permissions.size());
      permissions.forEach(perm -> permissionNames.add(perm.getDisplayName()));
      return String.join(",", permissionNames);
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return "{ operationStartedAt="
        + operationStartedAt
        + ", operationFinishedAt="
        + operationFinishedAt
        + ", operation="
        + operation
        + ", phase="
        + phase
        + ", user="
        + user
        + ", OrganizationId="
        + organizationId
        + ", tenant="
        + tenant
        + ", stream="
        + stream
        + ", status="
        + status
        + ", permissionATM="
        + permissionATM
        + ", request="
        + request
        + ", response="
        + response
        + ", userId="
        + userId
        + "}";
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }
}
