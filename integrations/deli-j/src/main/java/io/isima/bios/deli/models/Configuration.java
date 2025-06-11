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
package io.isima.bios.deli.models;

import io.isima.bios.deli.PropertyNames;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.TenantConfig;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Class that provides application-level configuration parameters.
 */
public class Configuration {
  @Getter
  private final String deliHome;

  @Getter
  private final String dataDir;

  /**
   * Raw tenant configuration fetched from the bios server.
   */
  @Getter
  private TenantConfig tenantConfig;

  private Map<String, ImportSourceConfig> importSources;
  private Map<String, ImportDestinationConfig> importDestinations;

  /**
   * Properties read from the application configuration file.
   */
  @Getter
  private final Properties appProperties;

  public Configuration(String deliHome, String dataDir, Properties appProperties) {
    Objects.requireNonNull(deliHome);
    Objects.requireNonNull(appProperties);
    this.deliHome = deliHome.endsWith("/") ? deliHome : deliHome + "/";
    this.dataDir = dataDir.endsWith("/") ? dataDir : dataDir + "/";
    this.appProperties = appProperties;
  }

  /**
   * Makes the data file path for the import source.
   *
   * <p>
   * The data file path is at ${DELI_DATA}/&lt;importSourceId&gt/;. If the path does not exist,
   * the method creates one.
   * </p>
   *
   * @param importSourceConfig Import source configuration
   * @return the data file path name
   */
  public String makeDataPath(ImportSourceConfig importSourceConfig) {
    final var path = getDataDir() + importSourceConfig.getImportSourceId();
    new File(path).mkdirs();
    return path;
  }

  public void registerTenantConfig(TenantConfig tenantConfig) {
    this.tenantConfig = Objects.requireNonNull(tenantConfig);
    importSources = new HashMap<>();
    tenantConfig.getImportSources()
        .forEach((entry) -> importSources.put(entry.getImportSourceId(), entry));
    importDestinations = new HashMap<>();
    tenantConfig.getImportDestinations()
        .forEach((entry) -> importDestinations.put(entry.getImportDestinationId(), entry));
  }

  public ImportSourceConfig getImportSourceConfig(String id) {
    return importSources.get(id);
  }

  public ImportDestinationConfig getImportDestinationConfig(String id) {
    return importDestinations.get(id);
  }

  public List<ImportSourceConfig> getImportSourceConfigs(ImportSourceType importSourceType) {
    return tenantConfig.getImportSources().stream()
        .filter(x -> x.getType() == importSourceType)
        .collect(Collectors.toList());
  }

  /**
   * Returns whether the process is in proxy mode.
   */
  public boolean isProxyMode() {
    return Boolean.parseBoolean(appProperties.getProperty(PropertyNames.PROXY_ENABLED));
  }
}
