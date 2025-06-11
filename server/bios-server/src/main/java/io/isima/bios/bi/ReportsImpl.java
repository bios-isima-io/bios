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

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.GetReportConfigsResponse;
import io.isima.bios.models.UserContext;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportsImpl implements Reports {
  private static final Logger logger;
  private static final ObjectMapper objectMapper;

  private static final String COLUMN_TENANT_NAME = "tenant_name";
  private static final String COLUMN_TENANT_VERSION = "tenant_version";
  private static final String COLUMN_USER_ID = "user_id";
  private static final String COLUMN_REPORT_ID = "report_id";
  private static final String COLUMN_INSIGHT_NAME = "insight_name";
  private static final String COLUMN_REPORT_CONFIG = "config";
  private static final String COLUMN_INSIGHT_CONFIGS = "config";

  private static final String PROPERTY_REPORT_ID = "reportId";
  private static final String PROPERTY_REPORT_NAME = "reportName";

  private static final String TABLE_REPORTS;
  private static final String TABLE_INSIGHTS;

  private static final String STATEMENT_CREATE_REPORTS_TABLE;
  private static final String STATEMENT_PUT_REPORT;
  private static final String STATEMENT_DELETE_REPORT;

  private static final String STATEMENT_CREATE_INSIGHTS_TABLE;
  private static final String STATEMENT_GET_INSIGHTS;
  private static final String STATEMENT_PUT_INSIGHTS;
  private static final String STATEMENT_DELETE_ALL_INSIGHTS;

  static {
    logger = LoggerFactory.getLogger(ReportsImpl.class);
    objectMapper = BiosObjectMapperProvider.get();

    TABLE_REPORTS = CassandraConstants.KEYSPACE_ADMIN + "." + CassandraConstants.TABLE_REPORTS;
    TABLE_INSIGHTS = CassandraConstants.KEYSPACE_ADMIN + "." + CassandraConstants.TABLE_INSIGHTS;

    STATEMENT_CREATE_REPORTS_TABLE =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s"
                + " (%s TEXT, %s TIMESTAMP, %s TEXT, %s TEXT,"
                + " PRIMARY KEY (%s, %s, %s));",
            TABLE_REPORTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_REPORT_ID,
            COLUMN_REPORT_CONFIG,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_REPORT_ID);

    STATEMENT_PUT_REPORT =
        String.format(
            "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
            TABLE_REPORTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_REPORT_ID,
            COLUMN_REPORT_CONFIG);

    STATEMENT_DELETE_REPORT =
        String.format(
            "DELETE FROM %s WHERE %s = ? AND %s = ? AND %s = ?",
            TABLE_REPORTS, COLUMN_TENANT_NAME, COLUMN_TENANT_VERSION, COLUMN_REPORT_ID);

    STATEMENT_CREATE_INSIGHTS_TABLE =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s"
                + " (%s TEXT, %s TIMESTAMP, %s VARINT, %s TEXT, %s TEXT,"
                + " PRIMARY KEY (%s, %s, %s, %s));",
            TABLE_INSIGHTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_USER_ID,
            COLUMN_INSIGHT_NAME,
            COLUMN_INSIGHT_CONFIGS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_USER_ID,
            COLUMN_INSIGHT_NAME);

    STATEMENT_GET_INSIGHTS =
        String.format(
            "SELECT %s FROM %s WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?",
            COLUMN_INSIGHT_CONFIGS,
            TABLE_INSIGHTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_USER_ID,
            COLUMN_INSIGHT_NAME);

    STATEMENT_PUT_INSIGHTS =
        String.format(
            "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
            TABLE_INSIGHTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_USER_ID,
            COLUMN_INSIGHT_NAME,
            COLUMN_INSIGHT_CONFIGS);

    STATEMENT_DELETE_ALL_INSIGHTS =
        String.format(
            "DELETE FROM %s WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?",
            TABLE_INSIGHTS,
            COLUMN_TENANT_NAME,
            COLUMN_TENANT_VERSION,
            COLUMN_USER_ID,
            COLUMN_INSIGHT_NAME);
  }

  private final AdminInternal tfosAdmin;
  private final CassandraConnection cassandraConnection;

  public ReportsImpl(AdminImpl tfosAdmin, CassandraConnection cassandraConnection) {
    logger.debug("ReportsImpl initializing ###");
    this.tfosAdmin = tfosAdmin;
    this.cassandraConnection = cassandraConnection;

    try {
      createTables();
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }

    logger.info("### Reports initialized");
  }

  @Override
  public GetReportConfigsResponse getReportConfigs(
      String tenantName, UserContext userContext, List<String> reportIds, boolean detail)
      throws TfosException, ApplicationException {
    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    // TODO(Naoki): Get tenant version
    final Long tenantVersion = Long.valueOf(0);

    // build statement
    final var statement =
        QueryBuilder.select(COLUMN_REPORT_ID, COLUMN_REPORT_CONFIG)
            .from(CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_REPORTS)
            .where(QueryBuilder.eq(COLUMN_TENANT_NAME, tenantDesc.getName()))
            .and(QueryBuilder.eq(COLUMN_TENANT_VERSION, tenantVersion));
    if (reportIds != null) {
      statement.and(QueryBuilder.in(COLUMN_REPORT_ID, reportIds));
    }

    // execute
    final var results = cassandraConnection.execute("Getting report configs", logger, statement);

    // parse the result
    final var response = new GetReportConfigsResponse();
    final var configs = new ArrayList<Map<String, Object>>();
    while (!results.isExhausted()) {
      final var row = results.one();
      final String reportId = row.getString(COLUMN_REPORT_ID);
      final String configAsString = row.getString(COLUMN_REPORT_CONFIG);
      try {
        final Map<String, Object> config =
            objectMapper.readValue(configAsString, new TypeReference<Map<String, Object>>() {});
        if (detail) {
          configs.add(config);
        } else {
          final var tiny = new LinkedHashMap<String, Object>();
          tiny.put(PROPERTY_REPORT_ID, config.get(PROPERTY_REPORT_ID));
          tiny.put(PROPERTY_REPORT_NAME, config.get(PROPERTY_REPORT_NAME));
          configs.add(tiny);
        }
      } catch (IOException e) {
        throw new ApplicationException(
            String.format(
                "Fetching report config failed for %s; reportId=%s, config=%s",
                e.getMessage(), reportId, configAsString));
      }
    }
    response.setReportConfigs(configs);
    return response;
  }

  @Override
  public CompletableFuture<GetReportConfigsResponse> getReportConfigsAsync(
      String tenantName,
      UserContext userContext,
      List<String> reportIds,
      boolean detail,
      ExecutionState state) {

    return CompletableFuture.supplyAsync(
        () ->
            ExecutionHelper.supply(
                () -> getReportConfigs(tenantName, userContext, reportIds, detail)),
        state.getExecutor());
  }

  @Override
  public void putReportConfig(
      String tenantName, UserContext userContext, String reportId, String reportConfig)
      throws TfosException, ApplicationException {
    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    final long tenantVersion = 0;
    cassandraConnection.execute(
        "Putting report config",
        logger,
        STATEMENT_PUT_REPORT,
        tenantDesc.getName(),
        tenantVersion,
        reportId,
        reportConfig);
  }

  @Override
  public CompletableFuture<Void> putReportConfigAsync(
      String tenantName,
      UserContext userContext,
      String reportId,
      Object reportConfig,
      ExecutionState state) {
    final String encoded;
    try {
      encoded = objectMapper.writeValueAsString(reportConfig);
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new InvalidValueException("Failed to encode report config", e));
    }

    final TenantDesc tenantDesc;
    try {
      tenantDesc = getTenantDesc(tenantName, userContext);
    } catch (NoSuchTenantException e) {
      return CompletableFuture.failedFuture(e);
    }
    final long tenantVersion = 0;
    return cassandraConnection
        .executeAsync(
            new SimpleStatement(
                STATEMENT_PUT_REPORT, tenantDesc.getName(), tenantVersion, reportId, encoded),
            state)
        .thenAccept((x) -> {})
        .toCompletableFuture();
  }

  @Override
  public void deleteReportConfig(String tenantName, UserContext userContext, String reportId)
      throws TfosException, ApplicationException {
    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    final long tenantVersion = 0;
    cassandraConnection.execute(
        "Deleting report config",
        logger,
        STATEMENT_DELETE_REPORT,
        tenantDesc.getName(),
        tenantVersion,
        reportId);
  }

  @Override
  public CompletableFuture<Void> deleteReportConfigAsync(
      String tenantName, UserContext userContext, String reportId, ExecutionState state) {
    return CompletableFuture.runAsync(
        () -> ExecutionHelper.run(() -> deleteReportConfig(tenantName, userContext, reportId)),
        state.getExecutor());
  }

  @Override
  public Object getInsightConfigs(String tenantName, String insightName, UserContext userContext)
      throws TfosException, ApplicationException {
    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    final Long tenantVersion = Long.valueOf(0);
    final var userId = userContext.getUserId();

    final var results =
        cassandraConnection.execute(
            "Getting insight configs",
            logger,
            STATEMENT_GET_INSIGHTS,
            tenantDesc.getName(),
            tenantVersion,
            userId,
            insightName);

    if (results.isExhausted()) {
      return Collections.emptyMap();
    }

    final String configSrc = results.one().getString(COLUMN_INSIGHT_CONFIGS);
    try {
      final Object obj = objectMapper.readValue(configSrc, Object.class);
      return obj;
    } catch (IOException e) {
      throw new ApplicationException(
          String.format(
              "Fetching insight configs failed for %s; tenant=%s, user=%s, config=%s",
              e.getMessage(), userContext.getSubject(), tenantDesc.getName(), configSrc));
    }
  }

  @Override
  public CompletableFuture<Object> getInsightConfigsAsync(
      String tenantName, String insightName, UserContext userContext, ExecutionState state) {
    return CompletableFuture.supplyAsync(
        () -> ExecutionHelper.supply(() -> getInsightConfigs(tenantName, insightName, userContext)),
        state.getExecutor());
  }

  @Override
  public void putInsightConfigs(
      String tenantName, String insightName, UserContext userContext, String insightConfigs)
      throws TfosException, ApplicationException {

    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    final Long tenantVersion = Long.valueOf(0);
    final var userId = userContext.getUserId();

    cassandraConnection.execute(
        "Putting insight configs",
        logger,
        STATEMENT_PUT_INSIGHTS,
        tenantDesc.getName(),
        tenantVersion,
        userId,
        insightName,
        insightConfigs);
  }

  @Override
  public CompletableFuture<Void> putInsightConfigsAsync(
      String tenantName,
      String insightName,
      UserContext userContext,
      Object insightConfigs,
      ExecutionState state) {
    final String encoded;
    try {
      encoded = objectMapper.writeValueAsString(insightConfigs);
    } catch (JsonProcessingException e) {
      return CompletableFuture.failedFuture(
          new InvalidValueException("Failed to encode insight config", e));
    }

    final TenantDesc tenantDesc;
    try {
      tenantDesc = getTenantDesc(tenantName, userContext);
    } catch (NoSuchTenantException e) {
      return CompletableFuture.failedFuture(e);
    }
    final Long tenantVersion = Long.valueOf(0);
    final var userId = userContext.getUserId();

    return cassandraConnection
        .executeAsync(
            new SimpleStatement(
                STATEMENT_PUT_INSIGHTS,
                tenantDesc.getName(),
                tenantVersion,
                userId,
                insightName,
                encoded),
            state)
        .thenAccept((x) -> {})
        .toCompletableFuture();
  }

  @Override
  public void deleteAllInsightConfigs(
      String tenantName, String insightName, UserContext userContext)
      throws TfosException, ApplicationException {

    final TenantDesc tenantDesc = getTenantDesc(tenantName, userContext);
    final Long tenantVersion = Long.valueOf(0);
    final var userId = userContext.getUserId();

    cassandraConnection.execute(
        "Deleting all insight configs",
        logger,
        STATEMENT_DELETE_ALL_INSIGHTS,
        tenantDesc.getName(),
        tenantVersion,
        userId,
        insightName);
  }

  @Override
  public CompletableFuture<Void> deleteAllInsightConfigsAsync(
      String tenantName, String insightName, UserContext userContext, ExecutionState state) {
    return CompletableFuture.runAsync(
        () ->
            ExecutionHelper.run(
                () -> deleteAllInsightConfigs(tenantName, insightName, userContext)),
        state.getExecutor());
  }

  private void createTables() throws ApplicationException {
    cassandraConnection.createTable(
        CassandraConstants.KEYSPACE_ADMIN,
        CassandraConstants.TABLE_REPORTS,
        STATEMENT_CREATE_REPORTS_TABLE,
        logger);

    cassandraConnection.createTable(
        CassandraConstants.KEYSPACE_ADMIN,
        CassandraConstants.TABLE_INSIGHTS,
        STATEMENT_CREATE_INSIGHTS_TABLE,
        logger);
  }

  /**
   * Retrieves the tenant name from the specified parameter.
   *
   * <p>Either of tenantName or userContext.tenant must be set.
   *
   * @param tenantName Tenant name in the request message
   * @param userContext User context
   * @return The target tenant name
   * @throws NoSuchTenantException when the specified tenant is not found
   * @throws NullPointerException when userContext is null.
   * @throws IllegalArgumentException when the tenant name is not available.
   */
  private TenantDesc getTenantDesc(String tenantName, UserContext userContext)
      throws NoSuchTenantException {
    Objects.requireNonNull(userContext, "Parameter 'userContext may not be null");
    var theTenantName = (tenantName != null) ? tenantName : userContext.getTenant();
    if (theTenantName == null) {
      throw new IllegalArgumentException(
          "Either of parameter 'tenantName' or 'userContext.tenant' must be set");
    }
    return tfosAdmin.getTenant(theTenantName);
  }
}
