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
package io.isima.bios.it.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.auth.Auth;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.InputStream;
import java.util.Objects;
import lombok.Getter;

/** Reads admin op replay file and execute it. */
public class AdminOpReplayer {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  private final Auth auth;
  private final AdminInternal tfosAdmin;
  private final AdminServiceHandler adminHandler;
  private final InputStream dataInput;

  @Getter private String tenantName;

  public AdminOpReplayer(
      Auth auth, AdminInternal tfosAdmin, AdminServiceHandler adminHandler, InputStream dataInput) {
    this.auth = Objects.requireNonNull(auth);
    this.tfosAdmin = Objects.requireNonNull(tfosAdmin);
    this.adminHandler = Objects.requireNonNull(adminHandler);
    this.dataInput = Objects.requireNonNull(dataInput);
  }

  public void execute() throws Exception {
    final var replayData = mapper.readValue(dataInput, ReplayData.class);
    tenantName = replayData.getTenantName();
    AdminTestUtils.deleteTenantIgnoreError(tfosAdmin, tenantName);
    AdminTestUtils.addTenant(tfosAdmin, tenantName);
    for (var entry : replayData.getHistory()) {
      if (entry.getSkip() == Boolean.TRUE) {
        continue;
      }
      if ("signal".equalsIgnoreCase(entry.getType())) {
        executeSignalOp(entry);
      } else if ("context".equalsIgnoreCase(entry.getType())) {
        executeContextOp(entry);
      } else {
        throw new Exception("Unknown stream type: " + entry.getType());
      }
    }
  }

  private void executeSignalOp(ReplayEntry entry) throws Exception {
    final var signalConfig =
        mapper.readValue(mapper.writeValueAsString(entry.getSchema()), SignalConfig.class);
    final var operation = entry.getOperation();
    if ("delete".equalsIgnoreCase(operation)) {
      AdminTestUtils.deleteStream(tfosAdmin, tenantName, signalConfig.getName());
    } else if ("create".equalsIgnoreCase(operation)) {
      AdminTestUtils.createSignal(auth, adminHandler, tenantName, signalConfig);
    } else if ("update".equalsIgnoreCase(operation)) {
      AdminTestUtils.updateSignal(
          auth, adminHandler, tenantName, signalConfig.getName(), signalConfig);
    } else {
      throw new Exception("Unknown operation: " + operation);
    }
  }

  private void executeContextOp(ReplayEntry entry) throws Exception {
    final var contextConfig =
        mapper.readValue(mapper.writeValueAsString(entry.getSchema()), ContextConfig.class);
    final var operation = entry.getOperation();
    if ("delete".equalsIgnoreCase(operation)) {
      AdminTestUtils.deleteStream(tfosAdmin, tenantName, contextConfig.getName());
    } else if ("create".equalsIgnoreCase(operation)) {
      AdminTestUtils.createContext(auth, adminHandler, tenantName, contextConfig);
    } else if ("update".equalsIgnoreCase(operation)) {
      AdminTestUtils.updateContext(
          auth, adminHandler, tenantName, contextConfig.getName(), contextConfig);
    } else {
      throw new Exception("Unknown operation: " + operation);
    }
  }
}
