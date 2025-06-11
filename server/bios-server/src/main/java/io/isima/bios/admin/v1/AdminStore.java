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
package io.isima.bios.admin.v1;

import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.v1.TenantConfig;
import java.util.Deque;
import java.util.Map;

/** This interface serves storage of admin configurations. */
public interface AdminStore {

  /**
   * Returns map of tenant name and config by quering Cassandra DB.
   *
   * @return map
   * @throws ApplicationException when unexpected error happened.
   */
  Map<String, Deque<TenantStoreDesc>> getTenantStoreDescMap() throws ApplicationException;

  void storeTenant(TenantStoreDesc tenantStoreDesc) throws ApplicationException;

  void storeTenantOnly(TenantStoreDesc tenantStoreDesc) throws ApplicationException;

  void storeStream(String tenant, StreamStoreDesc streamStoreDesc) throws ApplicationException;

  void deleteTenant(String tenant) throws ApplicationException;

  TenantConfig getTenant(String name, Long timestamp) throws ApplicationException;

  StreamStoreDesc getStream(String tenant, String stream, Long timestamp)
      throws ApplicationException;
}
