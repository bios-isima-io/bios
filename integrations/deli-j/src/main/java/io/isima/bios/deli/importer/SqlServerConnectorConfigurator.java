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
package io.isima.bios.deli.importer;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportSourceConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class SqlServerConnectorConfigurator extends ConnectorConfigurator<Map<String, Object>> {

  public static final CdcEventHandler EVENT_HANDLER = new GenericCdcEventHandler();

  private static final int MAX_DRIVER_ID = 100000;
  private static final int MIN_DRIVER_ID = 80000;

  public SqlServerConnectorConfigurator() {
    super("io.debezium.connector.sqlserver.SqlServerConnector");
  }

  @Override
  public CdcEventHandler getCdcEventHandler() {
    return EVENT_HANDLER;
  }

  @Override
  protected void configureStaticProperties(Configuration configuration,
      Properties connectorProps) {
  }

  @Override
  protected void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams, Properties props)
      throws IOException, InvalidConfigurationException {
    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());

    setDatabaseHostPort(importSourceConfig, props);

    props.setProperty("database.names", confParams.getDatabaseName());

    setLoginCredentials(configuration, importSourceConfig, props, errorContext);
  }
}
