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
import java.util.Map;
import java.util.Properties;

public class OracleConnectorConfigurator extends ConnectorConfigurator<Map<String, Object>> {

  public static final CdcEventHandler EVENT_HANDLER = new GenericCdcEventHandler();

  private static final int MAX_DRIVER_ID = 100000;
  private static final int MIN_DRIVER_ID = 80000;

  public OracleConnectorConfigurator() {
    super("io.debezium.connector.oracle.OracleConnector");
  }

  @Override
  public CdcEventHandler getCdcEventHandler() {
    return EVENT_HANDLER;
  }

  @Override
  protected void configureStaticProperties(Configuration configuration, Properties connectorProps) {
    connectorProps.setProperty("snapshot.mode", "schema_only");

    connectorProps.setProperty("database.oracle.version", "12+");
    connectorProps.setProperty("database.connection.adapter", "logminer");
  }

  @Override
  protected void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams,
      Properties connectorProps) throws InvalidConfigurationException {

    setDatabaseHostPort(importSourceConfig, connectorProps);

    connectorProps.setProperty("database.password",
        importSourceConfig.getAuthentication().getPassword());

    connectorProps.setProperty("database.dbname", confParams.getDatabaseName());
    String userString = importSourceConfig.getAuthentication().getUser();
    String[] uString = userString.split("@");
    if (uString.length > 1) {
      connectorProps.setProperty("database.pdb.name", uString[1]);
    }
    connectorProps.setProperty("database.user", uString[0]);
  }
}
