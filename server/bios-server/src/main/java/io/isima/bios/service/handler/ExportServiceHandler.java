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
package io.isima.bios.service.handler;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.export.ExportService;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.recorder.ControlRequestType;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Service data export config requests. */
public class ExportServiceHandler extends ServiceHandler {
  private final ExportService exportService;

  public ExportServiceHandler(
      AuditManager auditManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics,
      ExportService exportService) {
    super(auditManager, auth, admin, dataEngine, metrics);
    this.exportService = exportService;
  }

  public CompletableFuture<ExportDestinationConfig> getExportDestination(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String exportDestinationId) {

    return new AsyncServiceHandler<ExportDestinationConfig>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.GET_EXPORT_DESTINATION,
        AllowedRoles.TENANT_READ,
        null) {
      @Override
      protected ExportDestinationConfig handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return exportService.getDestination(tenantName, exportDestinationId);
      }
    }.executeAsync();
  }

  public CompletableFuture<ExportDestinationConfig> createExportDestination(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      ExportDestinationConfig exportDestinationConfig) {

    return new AsyncServiceHandler<ExportDestinationConfig>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.CREATE_EXPORT_DESTINATION,
        AllowedRoles.TENANT_WRITE,
        null) {
      @Override
      protected ExportDestinationConfig handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return exportService.createDestination(tenantName, exportDestinationConfig);
      }
    }.executeAsync();
  }

  public CompletableFuture<ExportDestinationConfig> updateExportDestination(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String exportDestinationId,
      ExportDestinationConfig exportDestinationConfig) {

    return new AsyncServiceHandler<ExportDestinationConfig>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.UPDATE_EXPORT_DESTINATION,
        AllowedRoles.TENANT_WRITE,
        null) {
      @Override
      protected ExportDestinationConfig handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return exportService.updateDestination(
            tenantName, exportDestinationId, exportDestinationConfig);
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> deleteExportConfig(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String exportDestinationId) {

    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(exportDestinationId);

    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.GET_EXPORT_DESTINATION,
        AllowedRoles.TENANT_READ,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        exportService.deleteDestination(tenantName, exportDestinationId);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> startExport(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String storageName) {
    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.START_EXPORT,
        AllowedRoles.TENANT_WRITE,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        exportService.startService(tenantName, storageName);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> stopExport(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String storageName) {
    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.STOP_EXPORT,
        AllowedRoles.TENANT_WRITE,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        exportService.stopService(tenantName, storageName);
        return null;
      }
    }.executeAsync();
  }

  public void stop() {
    exportService.shutdown();
  }
}
