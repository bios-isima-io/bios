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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.integrations.validator.IntegrationError;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.ImportPayloadType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceSchema;
import io.isima.bios.models.SourceAttributeSchema;
import io.isima.bios.models.SslMode;
import io.isima.bios.models.SubjectSchema;
import io.isima.bios.models.SubjectType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresSourceSchemaFinder extends SourceSchemaFinder {

  private static final ObjectMapper mapper = new ObjectMapper();

  interface PostgresTypeConverter {
    Object convert(Object obj);
  }

  private static final Map<String, AttributeType> typeMap;
  private static final Map<String, PostgresTypeConverter> conversionMap;
  private static final Map<String, BiosError> sqlErrors;

  static {
    typeMap = new HashMap<>();
    // Integer Data Types
    typeMap.put("int2", AttributeType.INTEGER);
    typeMap.put("int4", AttributeType.INTEGER);
    typeMap.put("integer", AttributeType.INTEGER);
    typeMap.put("smallint", AttributeType.INTEGER);
    typeMap.put("bigint", AttributeType.INTEGER);
    typeMap.put("smallserial", AttributeType.INTEGER);
    typeMap.put("serial", AttributeType.INTEGER);
    typeMap.put("bigserial", AttributeType.INTEGER);
    typeMap.put("bit", AttributeType.INTEGER);
    typeMap.put("bit varying", AttributeType.INTEGER);
    // Range Data Types
    typeMap.put("int4range", AttributeType.INTEGER);
    typeMap.put("int8range", AttributeType.INTEGER);
    typeMap.put("numrange", AttributeType.DECIMAL);
    typeMap.put("tsrange", AttributeType.INTEGER);
    typeMap.put("tstzrange", AttributeType.INTEGER);
    typeMap.put("daterange", AttributeType.INTEGER);
    // Fixed Point Data Types
    typeMap.put("numeric", AttributeType.DECIMAL);
    typeMap.put("decimal", AttributeType.DECIMAL);
    typeMap.put("real", AttributeType.DECIMAL);
    typeMap.put("double precision", AttributeType.DECIMAL);
    typeMap.put("float", AttributeType.DECIMAL);
    typeMap.put("money", AttributeType.DECIMAL);
    // DateTime Data Types
    typeMap.put("timestamp", AttributeType.INTEGER);
    typeMap.put("time", AttributeType.STRING);
    typeMap.put("date", AttributeType.STRING);
    typeMap.put("interval", AttributeType.INTEGER);
    typeMap.put("interval year", AttributeType.INTEGER);
    typeMap.put("interval month", AttributeType.INTEGER);
    typeMap.put("interval day", AttributeType.INTEGER);
    typeMap.put("interval hour", AttributeType.INTEGER);
    typeMap.put("interval minute", AttributeType.INTEGER);
    typeMap.put("interval second", AttributeType.INTEGER);
    typeMap.put("interval year to month", AttributeType.INTEGER);
    typeMap.put("interval day to hour", AttributeType.INTEGER);
    typeMap.put("interval day to minute", AttributeType.INTEGER);
    typeMap.put("interval day to second", AttributeType.INTEGER);
    typeMap.put("interval hour to minute", AttributeType.INTEGER);
    typeMap.put("interval hour to second", AttributeType.INTEGER);
    typeMap.put("interval minute to second", AttributeType.INTEGER);
    // String Data Types
    typeMap.put("char", AttributeType.STRING);
    typeMap.put("character", AttributeType.STRING);
    typeMap.put("varchar", AttributeType.STRING);
    typeMap.put("character varying", AttributeType.STRING);
    typeMap.put("text", AttributeType.STRING);
    typeMap.put("_text", AttributeType.STRING);
    typeMap.put("bytea", AttributeType.STRING);
    typeMap.put("enum", AttributeType.STRING);
    typeMap.put("tsvector", AttributeType.STRING);
    typeMap.put("tsquery", AttributeType.STRING);
    typeMap.put("uuid", AttributeType.STRING);
    typeMap.put("xml", AttributeType.STRING);
    typeMap.put("json", AttributeType.STRING);
    typeMap.put("jsonb", AttributeType.STRING);
    typeMap.put("jsonpath", AttributeType.STRING);
    // Boolean Data Types
    typeMap.put("bool", AttributeType.BOOLEAN);
    typeMap.put("boolean", AttributeType.BOOLEAN);
    // Geometric Data Types
    typeMap.put("point", AttributeType.STRING);
    typeMap.put("line", AttributeType.STRING);
    typeMap.put("lseg", AttributeType.STRING);
    typeMap.put("box", AttributeType.STRING);
    typeMap.put("path", AttributeType.STRING);
    typeMap.put("polygon", AttributeType.STRING);
    typeMap.put("circle", AttributeType.STRING);
    // Network Address Types
    typeMap.put("cidr", AttributeType.STRING);
    typeMap.put("inet", AttributeType.STRING);
    typeMap.put("macaddr", AttributeType.STRING);
    typeMap.put("macaddr8", AttributeType.STRING);
    // Other Postgres Specific Data Types
    typeMap.put("mpaa_rating", AttributeType.STRING);
    typeMap.put("bpchar", AttributeType.STRING);

    sqlErrors = new HashMap<>();
    sqlErrors.put("28000", IntegrationError.SOURCE_SIGN_IN_FAILURE);
    sqlErrors.put("08S01", IntegrationError.SOURCE_CONNECTION_ERROR);
    sqlErrors.put("42000", IntegrationError.SOURCE_ACCESS_RULE_VIOLATION);
    sqlErrors.put("08000", IntegrationError.IMPORT_SOURCE_CONFIGURATION_ERROR);

    conversionMap = new HashMap<>();
    conversionMap.put("_text", new ComplexTypeToStringConverter());
    conversionMap.put("point", new ComplexTypeToStringConverter());
    conversionMap.put("line", new ComplexTypeToStringConverter());
    conversionMap.put("lseg", new ComplexTypeToStringConverter());
    conversionMap.put("box", new ComplexTypeToStringConverter());
    conversionMap.put("path", new ComplexTypeToStringConverter());
    conversionMap.put("polygon", new ComplexTypeToStringConverter());
    conversionMap.put("circle", new ComplexTypeToStringConverter());
    conversionMap.put("cidr", new ComplexTypeToStringConverter());
    conversionMap.put("inet", new ComplexTypeToStringConverter());
    conversionMap.put("macaddr", new ComplexTypeToStringConverter());
    conversionMap.put("macaddr8", new ComplexTypeToStringConverter());
    conversionMap.put("bytea", new ComplexTypeToStringConverter());
    conversionMap.put("tsvector", new ComplexTypeToStringConverter());
    conversionMap.put("tsquery", new ComplexTypeToStringConverter());
    conversionMap.put("complex", new ComplexTypeToStringConverter());
  }

  static class ComplexTypeToStringConverter implements PostgresTypeConverter {
    @Override
    public Object convert(final Object obj) {
      try {
        return mapper.writeValueAsString(obj.toString());
      } catch (JsonProcessingException e) {
        logger.error("Exception occurred during complex type conversion", e);
        return "";
      }
    }
  }

  public PostgresSourceSchemaFinder(String tenantName, ImportSourceConfig config) {
    super(tenantName, config);
  }

  @Override
  public ImportSourceSchema find(Integer timeoutSeconds)
      throws TfosException, ApplicationException {
    final var schema = new ImportSourceSchema();
    schema.setImportSourceName(config.getImportSourceName());
    schema.setImportSourceId(config.getImportSourceId());

    final var sb = new StringBuilder("jdbc:postgresql://").append(config.getDatabaseHost());
    if (config.getDatabasePort() != null) {
      sb.append(":").append(config.getDatabasePort());
    }
    final var databaseName = config.getDatabaseName();
    sb.append("/").append(databaseName);

    /*
     * Refer: https://www.postgresql.org/docs/9.1/libpq-ssl.html
     *
     * Postgres natively supports SSL connections, using libpq (NOT Java cacerts). It uses the
     * same connection to support both regular as well as SSL connections.
     *
     * The following options are available:
     *
     * - disable    : Regular JDBC connection
     * - allow      : Use it for encrypting traffic, but, no other validations
     * - prefer     : Use it for encrypting traffic, no validations whether I trust
     *                the network or not
     * - require    : Use it for encrypting traffic, I trust the network. No CA or
     *                hostname verification required
     * - verify-ca  : Use it for encrypting traffic, Check if the CA is trusted.
     *                No hostname verification
     * - verify-full: Use it for encrypting traffic, Check if CA is trusted as well as hostname
     *
     * For verify-ca and verify-full, the following needs to be done on the bios server nodes
     * - Create a root.crt file and copy it to /root/.postgresql/ directory
     *
     * To allow client side auth, add /root/.postgresql/postgresql.crt and postgresql.key files
     * on the server nodes
     *
     * If the sslMode is ENABLED for postgres, we will use sslmode=require
     */
    boolean sslEnabled = config.getSsl() != null && config.getSsl().getMode() != SslMode.DISABLED;
    if (sslEnabled) {
      sb.append("?sslmode=require");
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
        logger.info("");
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
        try (final var rs =
            connection
                .createStatement()
                .executeQuery(String.format("SELECT * from %s LIMIT 10", tableName))) {
          final var columnsMetadata = rs.getMetaData();
          logger.info("columnsMetadata ->  {}", columnsMetadata);
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
            AttributeType suggestedType =
                typeMap.getOrDefault(columnTypeName.toLowerCase(), AttributeType.STRING);
            var finalColumnTypeName = columnTypeName.toLowerCase();
            if (typeMap.getOrDefault(finalColumnTypeName, null) == null) {
              finalColumnTypeName = "complex";
            }
            try {
              Object exampleValue = rs.getObject(i);
              if (conversionMap.get(finalColumnTypeName) != null) {
                exampleValue = conversionMap.get(finalColumnTypeName).convert(exampleValue);
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
    } catch (SQLException e) {
      final var sqlState = e.getSQLState();
      final BiosError error = sqlErrors.getOrDefault(sqlState, GenericError.INVALID_CONFIGURATION);
      logger.warn(
          "SQL error; {}, sqlState={}, error={}", errorContext, sqlState, e.getMessage(), e);
      throw new TfosException(error, e.getMessage());
    }
    return schema;
  }
}
