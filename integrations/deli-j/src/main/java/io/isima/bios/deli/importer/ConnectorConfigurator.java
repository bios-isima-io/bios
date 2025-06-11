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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConnectorConfigurator<SourceDataT> {
  private static final Logger logger = LoggerFactory.getLogger(ConnectorConfigurator.class);

  protected final String connectorClass;

  protected ConnectorConfigurator(String connectorClass) {
    this.connectorClass = connectorClass;
  }

  public abstract CdcEventHandler getCdcEventHandler();

  /**
   * Populate connector specific static properties.
   *
   * @param configuration  Deli configuration
   * @param connectorProps Output properties for the connector
   * @throws IOException thrown when the method fails to read any I/O resource
   */
  protected abstract void configureStaticProperties(Configuration configuration,
      Properties connectorProps) throws IOException;

  protected abstract void configureDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams, Properties connectorProps)
      throws IOException, InvalidConfigurationException;

  /**
   * Makes properties for the connector.
   *
   * <p>The method populates the basic values for the connector first, then overrides the values
   * by the application property parameters if any are set.</p>
   *
   * @param configuration      Deli configuration
   * @param importSourceConfig Import source configuration
   * @param dataImporter       Data importer for this configuration
   * @return Built properties
   * @throws IOException thrown when the method fails to read any I/O resource
   */
  public Properties makeProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcDataImporter dataImporter)
      throws IOException, InvalidConfigurationException {
    final Properties props = new Properties();

    // Set default static properties
    configureCommonStaticProperties(configuration, props);
    configureStaticProperties(configuration, props);

    // Override the values in the application properties if any are set
    configuration.getAppProperties().forEach((objKey, objValue) -> {
      final var key = (String) objKey;
      final var value = (String) objValue;
      if (!key.startsWith("io.isima.")) {
        props.setProperty(key, value);
      }
    });

    final var confParser =
        new CdcSourceConfigParser(configuration, importSourceConfig, dataImporter);
    final var confParams = confParser.parse();

    // Set properties determined by the import source config
    configureCommonDynamicProperties(configuration, importSourceConfig, confParams, props);
    configureDynamicProperties(configuration, importSourceConfig, confParams, props);

    return props;
  }

  /**
   * Sets up properties to configure the Debezium Engine.
   *
   * <p>The values are common among the types of connectors.</p>
   */
  protected void configureCommonStaticProperties(Configuration configuration, Properties props) {
    // Required for a Debezium engine
    props.setProperty("connector.class", connectorClass);

    // Offset and history file
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    props.setProperty("offset.flush.interval.ms", "60000");

    props.setProperty("schema.history.internal",
        "io.debezium.storage.file.history.FileSchemaHistory");

    // Kafka connect converter config.
    // The following parameters eliminate schema in event object. The schema is large, and we are
    // not using the info.
    props.setProperty("key.converter.schemas.enable", "false");
    props.setProperty("value.converter.schemas.enable", "false");
  }

  /**
   * Set properties defined by the import source schema.
   *
   * <p>This method places properties that are common among types of connectors.</p>
   */
  private void configureCommonDynamicProperties(Configuration configuration,
      ImportSourceConfig importSourceConfig, CdcConfParams confParams, Properties props)
      throws IOException, InvalidConfigurationException {

    props.setProperty("name", importSourceConfig.getImportSourceId());

    // Offset and history file
    final String dataPath = configuration.makeDataPath(importSourceConfig);
    final String offsetsFile = dataPath + "/offsets.dat";
    final String historyFile = dataPath + "/dbhistory.dat";
    final var storageDir = Paths.get(offsetsFile).getParent();
    if (!Files.exists(storageDir)) {
      Files.createDirectory(storageDir);
    }
    props.setProperty("offset.storage.file.filename", offsetsFile);
    props.setProperty("schema.history.internal.file.filename", historyFile);


    // props.setProperty("database.user", importSourceConfig.getAuthentication().getUser());
    // props.setProperty("database.password", importSourceConfig.getAuthentication().getPassword());

    // Kafka topic to use
    props.setProperty("topic.prefix", importSourceConfig.getImportSourceId());
  }

  /**
   * Generic method to set database host and port.
   */
  protected void setDatabaseHostPort(ImportSourceConfig importSourceConfig, Properties props)
      throws InvalidConfigurationException {
    final var endpoint = importSourceConfig.getEndpoint();
    if (endpoint != null) {
      final var elements = endpoint.split(":", 2);
      props.setProperty("database.hostname", elements[0]);
      if (elements.length > 1) {
        props.setProperty("database.port", elements[1]);
      }
    } else {
      if (importSourceConfig.getDatabaseHost() != null) {
        props.setProperty("database.hostname", importSourceConfig.getDatabaseHost());
      }
      if (importSourceConfig.getDatabasePort() != null) {
        props.setProperty("database.port", String.valueOf(importSourceConfig.getDatabasePort()));
      }
    }
    if (props.getProperty("database.hostname") == null) {
      throw new InvalidConfigurationException("Required database host is missing");
    }
    final var hostPort = new StringJoiner(":");
    hostPort.add(props.getProperty("database.hostname"));
    if (props.getProperty("database.port") != null) {
      hostPort.add(props.getProperty("database.port"));
    }
    logger.info("Endpoint set to {}", hostPort);
  }

  // Utilities ///////////////////////////////////////////////////////

  /**
   * Makes the data file path for the import source.
   *
   * <p>
   * The data file path is at ${DELI_DATA}/&lt;importSourceId&gt/;. If the path does not exist,
   * the method creates one.
   * </p>
   *
   * @param configuration      App configuration
   * @param importSourceConfig Import source configuration
   * @return the data file path name
   */
  /*
  private String makeDataPath(Configuration configuration, ImportSourceConfig importSourceConfig) {
    final var path = configuration.getDataDir() + importSourceConfig.getImportSourceId();
    new File(path).mkdirs();
    return path;
  }
  */
  protected void setLoginCredentials(Configuration configuration,
      ImportSourceConfig importSourceConfig, Properties props,
      String errorContext) throws InvalidConfigurationException {

    final var auth =
        require(importSourceConfig.getAuthentication(), "authentication", errorContext);

    props.setProperty("database.user",
        require(auth.getUser(), "authentication.user", errorContext));

    props.setProperty("database.password",
        require(auth.getPassword(), "authentication.password", errorContext));
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
  protected <T> T require(T value, String name, String errorContext)
      throws InvalidConfigurationException {
    return CdcSourceConfigParser.require(value, name, errorContext);
  }
}
