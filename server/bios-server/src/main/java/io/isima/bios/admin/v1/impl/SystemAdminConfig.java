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
package io.isima.bios.admin.v1.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Class that reads system tenant configuration from a resource JSON file and provides the list of
 * system tenants.
 *
 * <p>The tenant JSON should be a list of TenantConfig description. The JSON file is read when the
 * class constructs an instance.
 */
public class SystemAdminConfig implements AdminConfig {
  private static final String JSON_FILE = "tenants.json";
  private static final String IP2GEO = "ip2geo_context.json";

  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  private final List<TenantConfig> tenants;

  /** Ctor. */
  public SystemAdminConfig() throws FileReadException {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(JSON_FILE)) {
      TenantConfig[] configs = mapper.readValue(inputStream, TenantConfig[].class);
      tenants = Arrays.asList(configs);
    } catch (IOException e) {
      throw new FileReadException("Failed to load system admin config", e);
    }
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(IP2GEO)) {
      final var ip2geo = mapper.readValue(inputStream, StreamConfig.class);
      for (var tenant : tenants) {
        if (BiosConstants.TENANT_SYSTEM.equalsIgnoreCase(tenant.getName())) {
          final var streams = new ArrayList<>(tenant.getStreams());
          streams.add(ip2geo);
          tenant.setStreams(List.copyOf(streams));
        }
      }
    } catch (IOException e) {
      throw new FileReadException("Failed to load system admin config", e);
    }
  }

  @Override
  public Collection<TenantConfig> getTenantConfigList() {
    return tenants;
  }
}
