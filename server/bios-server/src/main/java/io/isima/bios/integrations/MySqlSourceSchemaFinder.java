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
package io.isima.bios.integrations;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.InvalidConfigurationException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.integrations.validator.IntegrationError;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ImportPayloadType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.IntegrationsSslConfig;
import io.isima.bios.models.SourceAttributeSchema;
import io.isima.bios.models.SslMode;
import io.isima.bios.models.SubjectSchema;
import io.isima.bios.models.SubjectType;
import io.isima.bios.utils.TrustStoreConverter;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlSourceSchemaFinder extends SourceSchemaFinder {

  interface MySqlTypeConverter {
    Object convert(Object obj);
  }

  static class DateTimeConverter implements MySqlTypeConverter {
    private final SimpleDateFormat dateTimeFormatter =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    public Object convert(final Object obj) {
      try {
        String dateStr = obj.toString();
        if (dateStr.length() == 16) {
          dateStr += ":00";
        }
        return dateTimeFormatter.parse(dateStr).getTime();
      } catch (ParseException e) {
        logger.error("Exception encountered while parsing datetime", e);
        return new Date().getTime();
      }
    }
  }

  static class YearConverter implements MySqlTypeConverter {
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public Object convert(final Object obj) {
      try {
        long timeInMillis = dateFormatter.parse(obj.toString()).getTime();
        return Instant.ofEpochMilli(timeInMillis)
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime()
            .getYear();
      } catch (ParseException e) {
        logger.error("Exception encountered while parsing datetime", e);
        return Instant.ofEpochMilli(new Date().getTime())
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime()
            .getYear();
      }
    }
  }

  private static final Map<String, AttributeType> typeMap;
  private static final Map<String, MySqlTypeConverter> conversionMap;
  private static final Map<String, BiosError> sqlErrors;

  static {
    typeMap = new HashMap<>();
    // Numeric Data Types
    typeMap.put("INTEGER", AttributeType.INTEGER);
    typeMap.put("INT", AttributeType.INTEGER);
    typeMap.put("TINYINT", AttributeType.INTEGER);
    typeMap.put("SMALLINT", AttributeType.INTEGER);
    typeMap.put("MEDIUMINT", AttributeType.INTEGER);
    typeMap.put("BIGINT", AttributeType.INTEGER);
    // UNSIGNED Numeric Data Types
    typeMap.put("INTEGER UNSIGNED", AttributeType.INTEGER);
    typeMap.put("INT UNSIGNED", AttributeType.INTEGER);
    typeMap.put("TINYINT UNSIGNED", AttributeType.INTEGER);
    typeMap.put("SMALLINT UNSIGNED", AttributeType.INTEGER);
    typeMap.put("MEDIUMINT UNSIGNED", AttributeType.INTEGER);
    typeMap.put("BIGINT UNSIGNED", AttributeType.INTEGER);
    // Fixed Point Data Types
    typeMap.put("DECIMAL", AttributeType.DECIMAL);
    typeMap.put("DEC", AttributeType.DECIMAL);
    typeMap.put("FIXED", AttributeType.DECIMAL);
    typeMap.put("NUMERIC", AttributeType.DECIMAL);
    typeMap.put("FLOAT", AttributeType.DECIMAL);
    typeMap.put("DOUBLE", AttributeType.DECIMAL);
    typeMap.put("DOUBLE PRECISION", AttributeType.DECIMAL);
    typeMap.put("REAL", AttributeType.DECIMAL);
    // UNSIGNED Fixed Point Data Types
    typeMap.put("DECIMAL UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("DEC UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("FIXED UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("NUMERIC UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("FLOAT UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("DOUBLE UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("DOUBLE PRECISION UNSIGNED", AttributeType.DECIMAL);
    typeMap.put("REAL UNSIGNED", AttributeType.DECIMAL);
    // DateTime Data Types
    typeMap.put("DATE", AttributeType.INTEGER);
    typeMap.put("TIME", AttributeType.INTEGER);
    typeMap.put("DATETIME", AttributeType.INTEGER);
    typeMap.put("TIMESTAMP", AttributeType.STRING);
    typeMap.put("YEAR", AttributeType.INTEGER);
    // String Data Types
    typeMap.put("CHARACTER", AttributeType.STRING);
    typeMap.put("CHAR", AttributeType.STRING);
    typeMap.put("VARCHAR", AttributeType.STRING);
    typeMap.put("TEXT", AttributeType.STRING);
    typeMap.put("TINYTEXT", AttributeType.STRING);
    typeMap.put("MEDIUMTEXT", AttributeType.STRING);
    typeMap.put("LONGTEXT", AttributeType.STRING);
    typeMap.put("ENUM", AttributeType.STRING);
    typeMap.put("SET", AttributeType.STRING);
    typeMap.put("BINARY", AttributeType.STRING);
    typeMap.put("VARBINARY", AttributeType.STRING);
    typeMap.put("BLOB", AttributeType.STRING);
    typeMap.put("TINYBLOB", AttributeType.STRING);
    typeMap.put("MEDIUMBLOB", AttributeType.STRING);
    typeMap.put("LONGBLOB", AttributeType.STRING);
    typeMap.put("BIT>1", AttributeType.STRING);
    // Boolean Data Types
    typeMap.put("BOOL", AttributeType.BOOLEAN);
    typeMap.put("BOOLEAN", AttributeType.BOOLEAN);
    typeMap.put("BIT:1", AttributeType.INTEGER);

    sqlErrors = new HashMap<>();
    sqlErrors.put("28000", IntegrationError.SOURCE_SIGN_IN_FAILURE);
    sqlErrors.put("08S01", IntegrationError.SOURCE_CONNECTION_ERROR);
    sqlErrors.put("42000", IntegrationError.SOURCE_ACCESS_RULE_VIOLATION);
    sqlErrors.put("08000", IntegrationError.IMPORT_SOURCE_CONFIGURATION_ERROR);

    conversionMap = new HashMap<>();
    conversionMap.put("datetime", new DateTimeConverter());
    conversionMap.put("year", new YearConverter());
  }

  /** Files saved temporarily to configure TLS certificate. */
  private String trustStoreFileName;

  private String clientCertificateFileName;

  public MySqlSourceSchemaFinder(String tenantName, ImportSourceConfig config) {
    super(tenantName, config);
  }

  @Override
  public ImportSourceSchema find(Integer timeoutSeconds)
      throws TfosException, ApplicationException {
    final var schema = new ImportSourceSchema();
    schema.setImportSourceName(config.getImportSourceName());
    schema.setImportSourceId(config.getImportSourceId());

    final var sb = new StringBuilder("jdbc:mysql://").append(config.getDatabaseHost());
    if (config.getDatabasePort() != null) {
      sb.append(":").append(config.getDatabasePort());
    }
    final var databaseName = config.getDatabaseName();
    sb.append("/").append(databaseName);
    boolean sslEnabled = config.getSsl() != null && config.getSsl().getMode() != SslMode.DISABLED;
    if (sslEnabled) {
      sb.append("?useLegacyDatetimeCode=false")
          .append("&verifyServerCertificate=false")
          .append("&useSSL=true")
          .append("&requireSSL=true")
          .append("&enabledTLSProtocols=TLSv1,TLSv1.1,TLSv1.2,TLSv1.3");
      checkTlsCertificates(config.getSsl(), sb);
    } else {
      sb.append("?allowPublicKeyRetrieval=true&useSSL=false");
    }

    final String user = config.getAuthentication().getUser();
    final String password = config.getAuthentication().getPassword();

    final String url = sb.toString();
    logger.info("Connecting to {} with user {} and password {}", url, user, password);

    try (Connection connection = DriverManager.getConnection(url, user, password)) {
      logger.info("Connection established");
      final var metadata = connection.getMetaData();
      final var tables =
          metadata.getTables(databaseName, null, null, new String[] {IntegrationConfig.TABLE});
      final var subjects = new ArrayList<SubjectSchema>();
      while (tables.next()) {
        final var tableName = tables.getString(IntegrationConfig.TABLE_NAME);
        final var subject = new SubjectSchema();
        subjects.add(subject);
        subject.setSubjectName(tableName);
        subject.setSubjectType(SubjectType.TABLE);
        subject.setPayloadType(ImportPayloadType.JSON);
        logger.info(
            "table={}, cat={}, schem={}, type={}",
            tableName,
            tables.getString(IntegrationConfig.TABLE_CAT),
            tables.getString(IntegrationConfig.TABLE_SCHEM),
            tables.getString(IntegrationConfig.TABLE_TYPE));
        try (var rs =
            connection
                .createStatement()
                .executeQuery(String.format("SELECT * from %s LIMIT 10", tableName))) {
          final var columnsMetadata = rs.getMetaData();
          // logger.info("  {}", columnsMetadata);
          rs.next();
          final var sourceAttributes = new ArrayList<SourceAttributeSchema>();
          subject.setSourceAttributes(sourceAttributes);
          for (int i = 1; i <= columnsMetadata.getColumnCount(); ++i) {
            final var attributeSchema = new SourceAttributeSchema();
            sourceAttributes.add(attributeSchema);
            final var columnName = columnsMetadata.getColumnName(i);
            final var columnType = columnsMetadata.getColumnType(i);
            final var columnTypeName = columnsMetadata.getColumnTypeName(i);
            final var precision = columnsMetadata.getPrecision(i);
            final var isNullable = columnsMetadata.isNullable(i);
            AttributeType suggestedType = null;
            if (columnTypeName.equalsIgnoreCase("BIT")) {
              var bitColumnTypeName = columnTypeName + (precision == 1 ? ":1" : ">1");
              suggestedType = typeMap.getOrDefault(bitColumnTypeName, AttributeType.STRING);
            } else {
              suggestedType = typeMap.getOrDefault(columnTypeName, AttributeType.STRING);
            }
            try {
              Object exampleValue = rs.getObject(i);
              if (conversionMap.get(columnTypeName.toLowerCase()) != null) {
                exampleValue =
                    conversionMap.get(columnTypeName.toLowerCase()).convert(exampleValue);
              }
              attributeSchema.setExampleValue(exampleValue);
            } catch (SQLException e) {
              logger.warn(
                  "error occur while fetching data from table = {} error = {}",
                  tableName,
                  e.getMessage());
            }
            attributeSchema.setSourceAttributeName(columnName);
            attributeSchema.setOriginalType(columnTypeName);
            attributeSchema.setIsNullable(isNullable != 0);
            attributeSchema.setSuggestedType(suggestedType);

            logger.info(
                "name={}, type={}({}):{} suggested={} nullable={} exampleValue={} "
                    + "columnClass={} schemaName={}",
                columnName,
                columnType,
                columnTypeName,
                precision,
                suggestedType,
                isNullable,
                attributeSchema.getExampleValue(),
                columnsMetadata.getColumnClassName(i),
                columnsMetadata.getSchemaName(i));
          }
        }
      }
      schema.setSubjects(subjects);

      // Remove temporary cert files if created. We leave the files in case of failure for debugging
      if (trustStoreFileName != null) {
        try {
          Files.delete(new File(trustStoreFileName).toPath());
        } catch (IOException e) {
          logger.error(
              "Failed to remove root CA truststore; file={}, {}",
              trustStoreFileName,
              errorContext,
              e);
        }
      }
      if (clientCertificateFileName != null) {
        try {
          Files.delete(new File(clientCertificateFileName).toPath());
        } catch (IOException e) {
          logger.error(
              "Failed to remove client certificate; file={}, {}",
              clientCertificateFileName,
              errorContext,
              e);
        }
      }
    } catch (SQLException e) {
      final var sqlState = e.getSQLState();
      final BiosError error = sqlErrors.getOrDefault(sqlState, GenericError.INVALID_CONFIGURATION);
      logger.warn(
          "SQL error; {}, sqlState={}, error={}", errorContext, sqlState, e.getMessage(), e);
      throw new TfosException(error, e.getMessage());
    }
    return schema;
  }

  private void checkTlsCertificates(IntegrationsSslConfig sslConfig, StringBuilder requestUrl)
      throws InvalidConfigurationException, ApplicationException {
    Objects.requireNonNull(sslConfig);
    Objects.requireNonNull(requestUrl);
    checkTlsRootCa(sslConfig.getRootCaContent(), requestUrl);
    checkTlsClientCertificate(sslConfig, requestUrl);
  }

  /**
   * Parse root CA content if configured, save it to a trustStore, and set corresponding connection
   * properties.
   */
  private void checkTlsRootCa(String rootCaContent, StringBuilder requestUrl)
      throws InvalidConfigurationException, ApplicationException {
    if (rootCaContent == null) {
      return;
    }

    // Convert the root PEM to a trustStore file
    trustStoreFileName = String.format("/tmp/root-ca-%d.truststore", hashCode());
    final String trustStorePassword = TfosConfig.mysqlSourceTrustStorePassword();
    final String alias = "rootCA";
    final var pemData = base64Decode(rootCaContent, "ssl.rootCaContent");
    try {
      TrustStoreConverter.convertPemToTrustStore(
          pemData, trustStoreFileName, trustStorePassword, alias);
      logger.info(
          "Generated truststore from root CA PEM; truststore={}, alias={}",
          trustStoreFileName,
          alias);
    } catch (GeneralSecurityException e) {
      logger.warn("Failed to parse TLS/SSL root CA; {}", errorContext, e);
      throw new InvalidConfigurationException(
          String.format(
              "Invalid TLS/SSL root CA. Check the import config; error=%s", e.getMessage()),
          e);
    } catch (IOException e) {
      throw new ApplicationException(
          "An I/O error encountered while handling root CA in import config", e);
    }

    // Set connection properties
    requestUrl
        .append("&trustCertificateKeyStoreUrl=file://")
        .append(trustStoreFileName)
        .append("&trustStorePassword=")
        .append(trustStorePassword);
  }

  /**
   * Parse client certificate if configured, save it to a clientCertificate file, and set
   * corresponding connection properties.
   */
  private void checkTlsClientCertificate(IntegrationsSslConfig sslConfig, StringBuilder requestUrl)
      throws InvalidConfigurationException, ApplicationException {
    Objects.requireNonNull(sslConfig);
    Objects.requireNonNull(requestUrl);
    final var clientCertificateContent = sslConfig.getClientCertificateContent();
    final var clientCertificatePassword = sslConfig.getClientCertificatePassword();
    if (clientCertificateContent == null || clientCertificatePassword == null) {
      return;
    }
    clientCertificateFileName = String.format("/tmp/client-cert-%d.crt", hashCode());
    final var certData = base64Decode(clientCertificateContent, "ssl.clientCertificateContent");

    final var clientCertFile = new File(clientCertificateFileName);
    try {
      Files.write(clientCertFile.toPath(), certData, CREATE, TRUNCATE_EXISTING);
      requestUrl
          .append("&clientCertificateKeyStoreUrl=file://")
          .append(clientCertificateFileName)
          .append("&clientCertificateKeyStorePassword=")
          .append(URLEncoder.encode(clientCertificatePassword, "utf-8"));
    } catch (IOException e) {
      throw new ApplicationException(
          String.format("Failed to write client cert file; %s", errorContext), e);
    }
  }
}
