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
package io.isima.bios.bi;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.GetReportConfigsResponse;
import io.isima.bios.models.UserContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Internal module that serves BI Report features. */
public interface Reports {

  /**
   * Get report configurations.
   *
   * @param tenantName Tenant name
   * @param userContext User context
   * @param reportIds Report IDs to fetch. The method returns all available configuration if
   *     omitted.
   * @param detail The method returns full configuration properties if set, otherwise returns only
   *     reportId and reportName.
   * @return Report configuration descriptor.
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  GetReportConfigsResponse getReportConfigs(
      String tenantName, UserContext userContext, List<String> reportIds, boolean detail)
      throws TfosException, ApplicationException;

  CompletableFuture<GetReportConfigsResponse> getReportConfigsAsync(
      String tenantName,
      UserContext userContext,
      List<String> reportIds,
      boolean detail,
      ExecutionState state);

  /**
   * Persist a report configuration identified by tenant name and report ID.
   *
   * <p>The modules does not check the existence of the specified configuration. If an entry already
   * exists at the specified location, the module overwrites it silently.
   *
   * @param tenantName Tenant name
   * @param userContext User context
   * @param reportId Report ID to store
   * @param reportConfig Report content as a JSON string. The report must include properties
   *     "reportId" and "reportName" as strings.
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  @Deprecated
  void putReportConfig(
      String tenantName, UserContext userContext, String reportId, String reportConfig)
      throws TfosException, ApplicationException;

  CompletableFuture<Void> putReportConfigAsync(
      String tenantName,
      UserContext userContext,
      String reportId,
      Object reportConfig,
      ExecutionState state);

  /**
   * Delete the specified report configuration.
   *
   * @param tenantName Tenant name
   * @param userContext User context
   * @param reportId Report ID to delete
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  void deleteReportConfig(String tenantName, UserContext userContext, String reportId)
      throws TfosException, ApplicationException;

  CompletableFuture<Void> deleteReportConfigAsync(
      String tenantName, UserContext userContext, String reportId, ExecutionState state);

  /**
   * Get all available insight configurations for the user.
   *
   * @param tenantName Tenant name
   * @param insightName Insight name
   * @param userContext User context
   * @return All insight configurations as a generic JSON-serializable object.
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  Object getInsightConfigs(String tenantName, String insightName, UserContext userContext)
      throws TfosException, ApplicationException;

  CompletableFuture<Object> getInsightConfigsAsync(
      String tenantName, String insightName, UserContext userContext, ExecutionState state);

  /**
   * Put all insight configurations for the user.
   *
   * <p>The method overwrites the user's entire existing insight configurations if any.
   *
   * @param tenantName Tenant name
   * @param insightName Insight name
   * @param userContext User context
   * @param insightConfigs Insight configurations as a JSON string. The method user has the
   *     responsibility to generate the valid object.
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  @Deprecated
  void putInsightConfigs(
      String tenantName, String insightName, UserContext userContext, String insightConfigs)
      throws TfosException, ApplicationException;

  CompletableFuture<Void> putInsightConfigsAsync(
      String tenantName,
      String insightName,
      UserContext userContext,
      Object insightConfigs,
      ExecutionState state);

  /**
   * Deletes all available insight configurations for the user.
   *
   * @param tenantName Tenant name
   * @param insightName Insight name
   * @param userContext User context
   * @throws TfosException thrown to indicate that a user error happens.
   * @throws ApplicationException thrown to indicate that an application error happens.
   */
  void deleteAllInsightConfigs(String tenantName, String insightName, UserContext userContext)
      throws TfosException, ApplicationException;

  CompletableFuture<Void> deleteAllInsightConfigsAsync(
      String tenantName, String insightName, UserContext userContext, ExecutionState state);
}
