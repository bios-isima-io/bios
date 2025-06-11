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
import io.isima.bios.bi.Reports;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.GetReportConfigsResponse;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.recorder.ControlRequestType;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BiServiceHandler extends ServiceHandler {
  private final Reports reports;

  public BiServiceHandler(
      AuditManager auditManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      Reports reports,
      OperationMetrics metrics) {
    super(auditManager, auth, admin, dataEngine, metrics);
    this.reports = reports;
  }

  public CompletableFuture<GetReportConfigsResponse> getReportConfigs(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      boolean detail,
      List<String> reportIds) {

    return new AsyncServiceHandler<GetReportConfigsResponse>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.GET_REPORT_CONFIGS,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected GetReportConfigsResponse handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return reports.getReportConfigs(tenantName, userContext, reportIds, detail);
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> putReportConfig(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String reportId,
      String reportConfig) {

    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.PUT_REPORT_CONFIG,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        reports.putReportConfig(tenantName, userContext, reportId, reportConfig);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> deleteReportConfig(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String reportId) {
    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.DELETE_REPORT,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        reports.deleteReportConfig(tenantName, userContext, reportId);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<Object> getInsightConfigs(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String insightName) {

    return new AsyncServiceHandler<>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.GET_INSIGHT_CONFIGS,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected Object handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        return reports.getInsightConfigs(tenantName, insightName, userContext);
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> putInsightConfigs(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String insightName,
      String insightConfigs) {
    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.PUT_INSIGHT_CONFIGS,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        reports.putInsightConfigs(tenantName, insightName, userContext, insightConfigs);
        return null;
      }
    }.executeAsync();
  }

  public CompletableFuture<Void> deleteAllInsightConfigs(
      SessionToken sessionToken,
      OperationMetricsTracer metricsTracer,
      String tenantName,
      String insightName) {

    return new AsyncServiceHandler<Void>(
        sessionToken,
        metricsTracer,
        tenantName,
        "",
        ControlRequestType.DELETE_INSIGHT_CONFIGS,
        AllowedRoles.REPORTS,
        null) {
      @Override
      protected Void handleService(UserContext userContext)
          throws TfosException, ApplicationException {
        reports.deleteAllInsightConfigs(tenantName, insightName, userContext);
        return null;
      }
    }.executeAsync();
  }
}
