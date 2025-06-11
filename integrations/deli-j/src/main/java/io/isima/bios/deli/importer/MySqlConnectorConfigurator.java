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
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlConnectorConfigurator extends ConnectorConfigurator<Map<String, Object>> {
  private static final Logger logger = LoggerFactory.getLogger(MySqlConnectorConfigurator.class);

  public static final CdcEventHandler EVENT_HANDLER = new GenericCdcEventHandler();

  private static final int MAX_DRIVER_ID = 100000;
  private static final int MIN_DRIVER_ID = 80000;

  public MySqlConnectorConfigurator() {
    super("io.debezium.connector.mysql.MySqlConnector");
  }

  @Override
  public CdcEventHandler getCdcEventHandler() {
    return EVENT_HANDLER;
  }

  @Override
  protected void configureStaticProperties(Configuration configuration, Properties connectorProps) {

    // TODO(Naoki): Make this configurable when a requirement to download initial data comes.
    // but it is risky to do so since huge amount of data may be stored in the source.
    // Also, we must not set "never" to this config parameter. If you set, the importer cannot
    // understand the schema of the incoming events.
    connectorProps.setProperty("snapshot.mode", "schema_only");

    // TODO(Naoki): Is it OK to assign different numbers among process executions?
    final var random = new Random(System.currentTimeMillis());
    connectorProps.setProperty("database.server.id",
        String.valueOf(random.nextInt(MAX_DRIVER_ID - MIN_DRIVER_ID) + MIN_DRIVER_ID));

    connectorProps.setProperty("decimal.handling.mode", "double");
    connectorProps.setProperty("binary.handling.mode", "base64");
    connectorProps.setProperty("time.precision.mode", "connect");
  }

  @Override
  protected void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams, Properties props)
      throws IOException, InvalidConfigurationException {

    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());

    setDatabaseHostPort(importSourceConfig, props);
    props.setProperty("database.include.list", confParams.getDatabaseName());

    final var tableList = confParams.getTableList();
    if (tableList != null) {
      props.setProperty("table.include.list", tableList);
    }

    setLoginCredentials(configuration, importSourceConfig, props, errorContext);

    configureTls(configuration, importSourceConfig, confParams.getSslConfig(), props);
  }

  private void configureTls(Configuration configuration, ImportSourceConfig importSourceConfig,
      SslConfiguration sslConfig, Properties props)
      throws InvalidConfigurationException, IOException {
    if (sslConfig == null) {
      // Disable SSL explicitly. The default is "preferred" that may or may not use TLS.
      // We avoid such an ambiguity.
      props.setProperty("database.ssl.mode", "disabled");
      return;
    }

    switch (sslConfig.getMode()) {
      case ENABLED:
        props.setProperty("database.ssl.mode", "verify_identity");
        break;
      case ENABLED_SKIP_VALIDATION:
        props.setProperty("database.ssl.mode", "required");
        break;
      default:
        props.setProperty("database.ssl.mode", "disabled");
    }

    final var rootCa = sslConfig.getRootCa();
    if (rootCa != null) {
      rootCa.generateTrustStore(logger);
      props.setProperty("database.ssl.truststore", rootCa.getRootCaFileName());
      props.setProperty("database.ssl.truststore.password", rootCa.getTrustStorePassword());
    }

    final var clientCertificate = sslConfig.getClientCertificate();
    if (clientCertificate != null) {
      clientCertificate.saveClientCertificate(logger);
      props.setProperty("database.ssl.keystore", clientCertificate.getClientCertificateFileName());
      props.setProperty("database.ssl.keystore.password",
          clientCertificate.getClientCertificatePassword());
    }
  }
}
