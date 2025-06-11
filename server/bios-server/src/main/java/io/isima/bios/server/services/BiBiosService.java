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
package io.isima.bios.server.services;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_SIGNAL_PREFIX;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.GetReportConfigsResponse;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.BiServiceHandler;
import io.isima.bios.utils.StringUtils;
import io.netty.handler.codec.Headers;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiBiosService extends BiosService {
  public static final Logger logger = LoggerFactory.getLogger(BiBiosService.class);

  private final BiServiceHandler v1Handler = BiosModules.getBiServiceHandler();

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "GetReportConfigs",
            GET,
            BiosServicePath.PATH_REPORTS,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            GetReportConfigsResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<GetReportConfigsResponse> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final boolean detail = getBoolean(queryParams, Keywords.DETAIL);
            final List<String> reportIds = parseQueryParameterList(queryParams, Keywords.IDS);

            return v1Handler.getReportConfigs(
                sessionToken, state.getMetricsTracer(), tenantName, detail, reportIds);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "PutReportConfig",
            PUT,
            BiosServicePath.PATH_REPORT,
            NO_CONTENT,
            MediaType.APPLICATION_JSON_DECODE_TO_STRING,
            MediaType.APPLICATION_JSON,
            String.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              String reportConfig,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String reportId = pathParams.get(Keywords.REPORT_ID);

            return v1Handler.putReportConfig(
                sessionToken, state.getMetricsTracer(), tenantName, reportId, reportConfig);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteReportConfig",
            DELETE,
            BiosServicePath.PATH_REPORT,
            NO_CONTENT,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String reportId = pathParams.get(Keywords.REPORT_ID);

            return v1Handler.deleteReportConfig(
                sessionToken, state.getMetricsTracer(), tenantName, reportId);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetInsightConfigs",
            GET,
            BiosServicePath.PATH_INSIGHTS,
            OK,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            Object.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Object> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String insightName = pathParams.get(Keywords.INSIGHT_NAME);

            return v1Handler.getInsightConfigs(
                sessionToken, state.getMetricsTracer(), tenantName, insightName);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "PutInsightConfigs",
            POST,
            BiosServicePath.PATH_INSIGHTS,
            NO_CONTENT,
            MediaType.APPLICATION_JSON_DECODE_TO_STRING,
            MediaType.APPLICATION_JSON,
            String.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              String insightConfigs,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String insightName = pathParams.get(Keywords.INSIGHT_NAME);

            return v1Handler.putInsightConfigs(
                sessionToken, state.getMetricsTracer(), tenantName, insightName, insightConfigs);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteInsightConfigs",
            DELETE,
            BiosServicePath.PATH_INSIGHTS,
            NO_CONTENT,
            MediaType.NONE,
            MediaType.APPLICATION_JSON,
            Void.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              Void none,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final String tenantName = pathParams.get(Keywords.TENANT_NAME);
            final String insightName = pathParams.get(Keywords.INSIGHT_NAME);

            return v1Handler.deleteAllInsightConfigs(
                sessionToken, state.getMetricsTracer(), tenantName, insightName);
          }
        });
  }

  /** Utility to make a context's audit signal name. */
  private String makeContextAuditSignalName(String contextName) {
    return StringUtils.prefixToCamelCase(CONTEXT_AUDIT_SIGNAL_PREFIX, contextName);
  }

  private String generatePath(String pathTemplate, String... params) {
    return StringUtils.generatePath(pathTemplate, params);
  }
}
