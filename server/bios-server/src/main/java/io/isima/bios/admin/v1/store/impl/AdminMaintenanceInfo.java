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
package io.isima.bios.admin.v1.store.impl;

import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AdminMaintenanceInfo {
  private Map<String, TreeSet<TenantStoreDesc>> tenantRowsMap;
  private Map<TenantStreamPair, TreeSet<StreamStoreDesc>> streamRowsMap;

  public AdminMaintenanceInfo(
      Map<String, TreeSet<TenantStoreDesc>> tenantRowsMap,
      Map<TenantStreamPair, TreeSet<StreamStoreDesc>> streamRowsMap) {
    this.tenantRowsMap = tenantRowsMap;
    this.streamRowsMap = streamRowsMap;
  }

  public Map<String, TreeSet<TenantStoreDesc>> getTenantRowsMap() {
    return tenantRowsMap;
  }

  public Map<TenantStreamPair, TreeSet<StreamStoreDesc>> getStreamRowsMap() {
    return streamRowsMap;
  }

  public Set<String> getAllTenantsToMaintain() {
    final var tenants = new TreeSet<String>();
    streamRowsMap.forEach(
        (tenantStreamPair, streamRowsSorted) -> {
          tenants.add(tenantStreamPair.getTenant());
        });
    return tenants;
  }
}
