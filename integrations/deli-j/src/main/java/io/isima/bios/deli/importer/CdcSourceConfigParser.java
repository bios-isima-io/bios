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

import static io.isima.bios.deli.models.SslConfiguration.CLIENT_CERTIFICATE_CONTENT;
import static io.isima.bios.deli.models.SslConfiguration.CLIENT_CERTIFICATE_PASSWORD;
import static io.isima.bios.deli.models.SslConfiguration.ROOT_CA_CONTENT;

import io.isima.bios.deli.flow.DataFlow;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.deli.models.SslConfiguration;
import io.isima.bios.deli.utils.ClientCertificate;
import io.isima.bios.deli.utils.RootCA;
import io.isima.bios.models.ImportSourceConfig;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to carry CDC source configuration parameters.
 */
public class CdcSourceConfigParser {
  private static final Logger logger = LoggerFactory.getLogger(CdcSourceConfigParser.class);

  private final Configuration configuration;
  private final ImportSourceConfig importSourceConfig;
  private final CdcDataImporter dataImporter;

  public CdcSourceConfigParser(Configuration configuration, ImportSourceConfig importSourceConfig,
      CdcDataImporter dataImporter) {
    this.configuration = configuration;
    this.importSourceConfig = importSourceConfig;
    this.dataImporter = dataImporter;
  }

  public CdcConfParams parse() throws InvalidConfigurationException, IOException {
    final var params = new CdcConfParams();
    params.setDatabaseName(getDatabaseName(importSourceConfig));
    if (dataImporter.getDataFlows() != null) {
      params.setTables(getTables(dataImporter.getDataFlows()));
    }
    params.setSslConfig(parseSslConfiguration(configuration, importSourceConfig));
    return params;
  }

  private static String getDatabaseName(ImportSourceConfig importSourceConfig)
      throws InvalidConfigurationException {
    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());
    return require(importSourceConfig.getDatabaseName(), "databaseName", errorContext);
  }

  private static Collection<String> getTables(List<DataFlow<Map<String, Object>>> dataFlows)
      throws InvalidConfigurationException {
    final var tables = new HashSet<String>();
    for (var dataFlow : dataFlows) {
      final var flowConfig = dataFlow.getFlowConfig();
      final String errorContext = String.format("flow=%s (%s)", flowConfig.getImportFlowName(),
          flowConfig.getImportFlowId());
      final var sourceDataSpec = flowConfig.getSourceDataSpec();
      final var tableName =
          require(sourceDataSpec.getTableName(), "flowConfig.sourceDataSpec.tableName",
              errorContext);
      tables.add(tableName);
    }
    return tables;
  }

  /**
   * Read the SSL configuration in the import source config and translate it to internal SSL
   * configuration if set.
   */
  private static SslConfiguration parseSslConfiguration(Configuration configuration,
      ImportSourceConfig importSourceConfig) throws InvalidConfigurationException, IOException {

    final var ssl = importSourceConfig.getSsl();
    if (ssl == null) {
      return null;
    }

    final var errorContext =
        String.format("source=%s (%s)", importSourceConfig.getImportSourceName(),
            importSourceConfig.getImportSourceId());

    final var sslConfig = new SslConfiguration();
    sslConfig.setMode(require(ssl.getMode(), "ssl.mode", errorContext));

    final var dataPath = configuration.makeDataPath(importSourceConfig);
    final String truststoreFileName = dataPath + "/root-ca.crt";
    final String clientCertificateFileName = dataPath + "/client-certificate.crt";

    if (ssl.getRootCaContent() != null) {
      final var rootCaPemData = base64Decode(ssl.getRootCaContent(), ROOT_CA_CONTENT, errorContext);
      // We keep the root CA data, but defer processing it. How to process is up to the connector.
      sslConfig.setRootCa(new RootCA(truststoreFileName, rootCaPemData));
    }

    if (ssl.getClientCertificateContent() != null) {
      final var clientCertificateData = base64Decode(ssl.getClientCertificateContent(),
          CLIENT_CERTIFICATE_CONTENT, errorContext);

      final var clientCertificatePassword =
          require(ssl.getClientCertificatePassword(), CLIENT_CERTIFICATE_PASSWORD, errorContext);

      sslConfig.setClientCertificate(
          new ClientCertificate(clientCertificateFileName, clientCertificateData,
              clientCertificatePassword));
    }

    return sslConfig;
  }


  /**
   * Decodes a base64 string.
   *
   * @param src          The source string
   * @param propertyName Property name of the source string
   * @param errorContext Configuration handler context attached to a thrown error message
   * @throws InvalidConfigurationException thrown when an error encountered while decoding
   * @returns Decoded byte array
   */
  protected static byte[] base64Decode(String src, String propertyName, String errorContext)
      throws InvalidConfigurationException {
    try {
      return Base64.getDecoder().decode(require(src, propertyName, errorContext));
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigurationException(
          "Invalid value; %s, property=tls.truststoreContent, error=%s", errorContext,
          e.getMessage());
    }
  }

  /**
   * Checks if the specified value is available, throws
   * {@link io.isima.bios.deli.models.InvalidConfigurationException} if it is not.
   *
   * @param value        The value to test
   * @param name         The property name of the value
   * @param errorContext Configuration handler context attached to a thrown error message
   * @throws InvalidConfigurationException thrown to indicate that the value is not available
   */
  public static <T> T require(T value, String name, String errorContext)
      throws InvalidConfigurationException {
    if (value == null) {
      throw new InvalidConfigurationException("Source config is missing required property '%s'; %s",
          name, errorContext);
    }
    return value;
  }
}
