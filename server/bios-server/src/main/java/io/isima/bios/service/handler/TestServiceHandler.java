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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.TestMethodParams;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.models.SessionToken;
import io.isima.bios.server.handlers.Bios2ServiceHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServiceHandler extends Bios2ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(TestServiceHandler.class);

  public TestServiceHandler(
      AuditManager auditManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics) {
    super(dataEngine, admin);
  }

  public CompletableFuture<List> checkJsonReaderTypesAsync(
      SessionToken sessionToken, List<Object> values, ExecutionState state) {
    return validateTokenAsync(
            sessionToken, BiosConstants.TENANT_SYSTEM, AllowedRoles.SYSADMIN, state)
        .thenApply(
            (none) -> {
              final var reply = new ArrayList<String>();
              for (Object value : values) {
                reply.add(value.getClass().getName());
                if (value instanceof String) {
                  ObjectMapper mapper = new ObjectMapper();
                  try {
                    Object obj = mapper.readValue((String) value, Object.class);
                    if (obj instanceof Map) {
                      final var params = (Map<String, Object>) obj;
                      final Long delay = Long.valueOf((Integer) params.get("delay"));
                      if (delay != null) {
                        try {
                          Thread.sleep(delay);
                        } catch (InterruptedException e) {
                          // it's ok
                          Thread.currentThread().interrupt();
                        }
                      }
                    }
                  } catch (IOException e) {
                    // it's ok, this is just a test method
                  }
                }
              }
              return reply;
            });
  }

  public CompletableFuture<Void> delayAsync(
      SessionToken sessionToken, TestMethodParams params, ExecutionState state) {
    return validateTokenAsync(
            sessionToken, BiosConstants.TENANT_SYSTEM, AllowedRoles.SYSADMIN, state)
        .thenRunAsync(() -> delay(params), ExecutorManager.getSidelineExecutor());
  }

  private void delay(TestMethodParams params) {
    if (params.getDelay() != null) {
      try {
        Thread.sleep(params.getDelay());
      } catch (InterruptedException e) {
        // do nothing
        Thread.currentThread().interrupt();
      }
    }
  }

  public CompletableFuture<Void> logMessage(
      SessionToken sessionToken, TestMethodParams params, ExecutionState state) {
    return validateTokenAsync(
            sessionToken, BiosConstants.TENANT_SYSTEM, AllowedRoles.SYSADMIN, state)
        .thenRun(
            () -> {
              logger.info(
                  "TEST MESSAGE: {}", params.getMessage() != null ? params.getMessage() : "null");
            });
  }
}
