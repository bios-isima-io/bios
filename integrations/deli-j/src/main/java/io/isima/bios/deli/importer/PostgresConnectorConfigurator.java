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
import io.isima.bios.deli.models.SslConfiguration;
import io.isima.bios.models.ImportSourceConfig;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresConnectorConfigurator extends ConnectorConfigurator<Map<String, Object>> {
  private static final Logger logger = LoggerFactory.getLogger(PostgresConnectorConfigurator.class);

  public static final CdcEventHandler EVENT_HANDLER = new GenericCdcEventHandler();

  private static final int MAX_DRIVER_ID = 100000;
  private static final int MIN_DRIVER_ID = 80000;

  public PostgresConnectorConfigurator() {
    super("io.debezium.connector.postgresql.PostgresConnector");
  }

  @Override
  public CdcEventHandler getCdcEventHandler() {
    return EVENT_HANDLER;
  }

  @Override
  protected void configureStaticProperties(Configuration configuration, Properties connectorProps) {
    connectorProps.setProperty("snapshot.mode", "never");

    connectorProps.setProperty("plugin.name", "pgoutput");
  }

  @Override
  protected void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams,
      Properties props) throws InvalidConfigurationException, IOException {

    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());

    setDatabaseHostPort(importSourceConfig, props);

    props.setProperty("database.dbname", confParams.getDatabaseName());

    if (importSourceConfig.getSlotName() != null) {
      // The slot name is generated at the time of integration source creation.
      // Valid replication slot name must contain only digits, lowercase characters and
      // underscores with length <= 63
      props.setProperty("slot.name", importSourceConfig.getSlotName());
    }

    props.setProperty("decimal.handling.mode", "double");
    props.setProperty("hstore.handling.mode", "map");

    // Adding this stops the Postgres importer for some reason. We unset this for now.
    /*
    final var tableList = confParams.getTableList();
    if (tableList != null) {
      connectorProps.setProperty("table.include.list", tableList);
    }
    */

    setLoginCredentials(configuration, importSourceConfig, props, errorContext);

    configureTls(configuration, importSourceConfig, confParams.getSslConfig(), props, errorContext);
  }

  private void configureTls(Configuration configuration, ImportSourceConfig importSourceConfig,
      SslConfiguration sslConfig, Properties props, String errorContext)
      throws InvalidConfigurationException, IOException {
    if (sslConfig == null) {
      // Disable SSL explicitly. The default is "preferred" that may or may not use TLS.
      // We avoid such an ambiguity.
      props.setProperty("database.sslmode", "disable");
      return;
    }

    /*
     * We will support "require" option only in the near future. "verify-full" & "verify-ca"
     * require changes to backend & compute nodes to add CA certs to /root/.postgresql directory
     *
     * Since, our postgres deployments will be to a trusted network (probably with VPC peering)
     * it was decided not to go with any other options. UI will support either enable or disable
     *
     * All other options will default to "disable"
     */
    switch (sslConfig.getMode()) {
      case ENABLED:
        props.setProperty("database.sslmode", "require");
        break;
      default:
        props.setProperty("database.sslmode", "disable");
    }

    if (sslConfig.getRootCa() != null) {
      sslConfig.getRootCa().saveRootPem(logger);
      props.setProperty("database.sslrootcert", sslConfig.getRootCa().getRootCaFileName());
    }

    if (sslConfig.getClientCertificate() != null) {
      final var dataPath = configuration.makeDataPath(importSourceConfig);
      final String certFileName = dataPath + "/client-cert.pem";
      final String keyFileName = dataPath + "/client-key.pem";
      sslConfig.getClientCertificate().convertToPemFiles(certFileName, keyFileName, logger);
      props.setProperty("database.sslcert", certFileName);
      props.setProperty("database.sslkey", keyFileName);
      logger.info("Set database.sslcert={}", certFileName);
      logger.info("Set database.sslkey={}", keyFileName);
    }
  }
}
