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

import io.isima.bios.admin.TenantAppendixImpl;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.admin.v1.impl.SharedPropertiesImpl;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.audit.AuditManagerImpl;
import io.isima.bios.auth.AuthImpl;
import io.isima.bios.auth.v1.impl.AuthV1Impl;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.repository.auth.GroupRepository;
import io.isima.bios.repository.auth.OrganizationRepository;
import io.isima.bios.repository.auth.UserRepository;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.service.HttpClientManager;
import io.isima.bios.service.handler.AdminServiceHandler;
import io.isima.bios.storage.cassandra.CassandraConnection;
import lombok.Getter;

@Getter
public class TestModules {
  private BiosModules dummyBiosModules;
  private AdminServiceHandler adminServiceHandler;
  private DataServiceHandler dataServiceHandler;
  private DataEngineImpl dataEngine;
  private AdminStoreImpl adminStore;
  private io.isima.bios.admin.AdminImpl admin;
  private AdminImpl tfosAdmin;
  private AuditManagerImpl auditManager;
  private OperationMetrics metrics;
  private SharedProperties sharedProperties;
  private SharedConfig sharedConfig;
  private AuthImpl auth;
  private TenantAppendixImpl tenantAppendix;
  private HttpClientManager clientManager;

  public static TestModules reload(Class testClass, CassandraConnection connection)
      throws ApplicationException {
    return reloadInternal(testClass, connection);
  }

  public static TestModules reload(String testClass, CassandraConnection connection)
      throws ApplicationException {
    return reloadInternal(testClass, connection);
  }

  private static TestModules reloadInternal(Object testClass, CassandraConnection connection)
      throws ApplicationException {
    final var modules = new TestModules();
    final var dummyBios2Modules = BiosModules.makeDummyModulesForTesting(testClass);
    modules.dummyBiosModules = dummyBios2Modules;
    modules.sharedProperties = new SharedPropertiesImpl(connection);
    modules.sharedConfig = new SharedConfig(modules.sharedProperties);
    modules.adminStore = new AdminStoreImpl(connection);
    modules.dataEngine =
        new DataEngineImpl(
            connection, null, modules.sharedConfig, modules.sharedProperties, dummyBios2Modules);
    modules.tfosAdmin = new AdminImpl(modules.adminStore, null, modules.dataEngine);
    modules.admin = new io.isima.bios.admin.AdminImpl(modules.tfosAdmin, null, null);
    modules.auditManager = new AuditManagerImpl(modules.dataEngine);
    SharedProperties.setInstance(modules.sharedProperties);
    final var orep2 = new OrganizationRepository(connection);
    final var urep2 = new UserRepository(connection);
    final var grep2 = new GroupRepository(connection);
    final var tfosAuth2 = new AuthV1Impl(orep2, urep2, grep2, modules.sharedConfig);
    modules.auth = new AuthImpl(tfosAuth2, modules.sharedConfig);
    modules.tenantAppendix = new TenantAppendixImpl(connection);
    modules.metrics = new OperationMetrics(modules.dataEngine);
    modules.clientManager = new HttpClientManager();

    modules.adminServiceHandler =
        new AdminServiceHandler(
            modules.auditManager,
            modules.admin,
            modules.tfosAdmin,
            modules.tenantAppendix,
            modules.auth,
            modules.dataEngine,
            modules.metrics,
            modules.sharedConfig,
            null,
            null,
            null);
    modules.dataServiceHandler =
        new DataServiceHandler(
            modules.dataEngine, modules.tfosAdmin, modules.metrics, modules.sharedConfig);
    return modules;
  }
}
