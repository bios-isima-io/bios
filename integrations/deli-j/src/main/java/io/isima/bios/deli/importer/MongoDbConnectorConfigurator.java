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

import static io.isima.bios.deli.Constants.CLIENT_CERTIFICATE_FILE;
import static io.isima.bios.deli.Constants.CLIENT_CERTIFICATE_PASSWORD_FILE;
import static io.isima.bios.deli.Constants.ROOT_CA_PEM;
import static io.isima.bios.deli.Constants.ROOT_CA_TRUSTSTORE;
import static io.isima.bios.deli.Constants.ROOT_CA_TRUSTSTORE_PASSWORD;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.deli.utils.Utils;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.models.SslMode;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbConnectorConfigurator extends ConnectorConfigurator<Map<String, Object>> {
  private static final Logger logger = LoggerFactory.getLogger(MongoDbConnectorConfigurator.class);

  public static final CdcEventHandler EVENT_HANDLER = new MongoDbCdcEventHandler();

  public MongoDbConnectorConfigurator() {
    super("io.debezium.connector.mongodb.MongoDbConnector");
  }

  @Override
  public CdcEventHandler getCdcEventHandler() {
    return EVENT_HANDLER;
  }

  @Override
  protected void configureStaticProperties(Configuration configuration, Properties connectorProps) {

    connectorProps.setProperty("mongodb.connection.mode", "replica_set");
    connectorProps.setProperty("snapshot.mode", "never");
    connectorProps.setProperty("capture.mode", "change_streams_update_full_with_pre_image");

    connectorProps.setProperty("decimal.handling.mode", "double");
    connectorProps.setProperty("binary.handling.mode", "base64");
    connectorProps.setProperty("time.precision.mode", "connect");
  }

  @Override
  protected void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams, Properties connectorProps)
      throws IOException, InvalidConfigurationException {

    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());

    final var port = importSourceConfig.getDatabasePort();
    final var hosts = require(importSourceConfig.getEndpoints(), "endpoints", errorContext)
        .stream()
        .map((host) -> port != null ? String.format("%s:%s", host, port) : host)
        .collect(Collectors.joining(","));

    final var protocol =
        importSourceConfig.getUseDnsSeedList() == Boolean.TRUE ? "mongodb+srv://" : "mongodb://";

    final var sslConfig = confParams.getSslConfig();
    final var sslEnabled = sslConfig != null && sslConfig.getMode() != SslMode.DISABLED;

    final var options = new StringJoiner("&");
    options.add(sslEnabled ? "ssl=true" : "ssl=false");
    if (importSourceConfig.getAuthSource() != null) {
      options.add("authSource=" + importSourceConfig.getAuthSource());
    }
    options.add(
        "replicaSet=" + require(importSourceConfig.getReplicaSet(), "replicaSet", errorContext));

    final var auth =
        require(importSourceConfig.getAuthentication(), "authentication", errorContext);

    final var userName = require(auth.getUser(), "authentication.user", errorContext);

    final String password;
    if (auth.getPassword() != null) {
      password = URLEncoder.encode(auth.getPassword(), "utf-8");
    } else {
      password = null;
    }
    if (auth.getType() == IntegrationsAuthenticationType.LOGIN) {
      require(password, "authentication.password", errorContext);
    } else if (auth.getType() == IntegrationsAuthenticationType.MONGODB_X509) {
      options.add("authSource=$external");
      options.add("authMechanism=MONGODB-X509");
    }

    final var databaseName = confParams.getDatabaseName();

    final var connectionString = new StringBuilder(protocol)
        .append(userName);
    if (password != null) {
      connectionString
          .append(":").append(password);
    }
    connectionString
        .append("@").append(hosts)
        .append("/").append(databaseName)
        .append("?").append(options)
        .toString();

    connectorProps.setProperty("mongodb.connection.string", connectionString.toString());
    logger.info("MongoDb connection string: " + connectionString);

    final var tableList = confParams.getTableList();
    if (tableList != null) {
      connectorProps.setProperty("collection.include.list", tableList);
    }

    if (sslConfig != null) {
      switch (sslConfig.getMode()) {
        case ENABLED:
          connectorProps.setProperty("mongodb.ssl.enabled", "true");
          break;
        case ENABLED_SKIP_VALIDATION:
          connectorProps.setProperty("mongodb.ssl.enabled", "true");
          connectorProps.setProperty("mongodb.ssl.invalid.hostname.allowed", "true");
          break;
        default:
          connectorProps.setProperty("mongodb.ssl.enabled", "false");
      }

      // Hack to set up TLS in case keystore and/or truststore are specified.
      // Debezium MongoDB connector relies on javax.net.ssl.* system properties, but setting them
      // here is too late to configure the TLS connection of this process.
      // So we keep the configuration to files, kill itself, then they are loaded first on the next
      // startup.
      boolean rebootRequired = false;

      final var clientCertificate = sslConfig.getClientCertificate();
      if (clientCertificate != null) {
        final var dataDir = configuration.getDataDir();
        final var globalClientCertificateName = dataDir + CLIENT_CERTIFICATE_FILE;
        if (clientCertificate.trySaving(globalClientCertificateName, logger)) {
          rebootRequired = true;
        }

        final var clientCertificatePasswordFile = dataDir + CLIENT_CERTIFICATE_PASSWORD_FILE;
        if (Utils.trySaveToFile(clientCertificate.getClientCertificatePassword(),
            clientCertificatePasswordFile)) {
          logger.info("Client certificate password saved in {}", clientCertificatePasswordFile);
          rebootRequired = true;
        }
      }

      final var rootCa = sslConfig.getRootCa();
      if (rootCa != null) {
        final String globalRootPemFileName = configuration.getDataDir() + ROOT_CA_PEM;
        if (Utils.trySaveToFile(rootCa.getRootCaPemData(), globalRootPemFileName)) {
          // Root CA PEM content has changed. Generate a new truststore and save
          rootCa.generateTrustStore(logger);
          final var processTrustStoreFile = configuration.getDataDir() + ROOT_CA_TRUSTSTORE;
          Utils.tryCopyFile(rootCa.getRootCaFileName(), processTrustStoreFile);

          // Also save the password
          final var trustStorePasswordFile =
              configuration.getDataDir() + ROOT_CA_TRUSTSTORE_PASSWORD;
          Utils.trySaveToFile(rootCa.getTrustStorePassword(), trustStorePasswordFile);
          rebootRequired = true;
        }
      }

      if (rebootRequired) {
        logger.warn("!!!");
        logger.warn("TLS configuration files updated. Restart needed. Shutting down");
        logger.warn("!!!");
        logger.warn("Exiting with status 1");
        logger.warn("");
        System.exit(1);
      }
    }
  }
}
