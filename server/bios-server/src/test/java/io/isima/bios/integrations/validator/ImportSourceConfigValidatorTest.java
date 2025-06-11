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
package io.isima.bios.integrations.validator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.MultipleValidatorViolationsException;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.models.IntegrationsSslConfig;
import io.isima.bios.models.SslMode;
import org.junit.BeforeClass;
import org.junit.Test;

public class ImportSourceConfigValidatorTest {

  private static ObjectMapper mapper;

  private static String mySqlSource;

  @BeforeClass
  public static void setUpClass() {
    mapper = new ObjectMapper();
    mySqlSource =
        "{"
            + "\"importSourceId\": \"209\","
            + "  \"importSourceName\": \"MySQL Inventory\","
            + "  \"type\": \"MySql\","
            + "  \"databaseHost\": \"172.17.0.1\","
            + "  \"databasePort\": 3306,"
            + "  \"databaseName\": \"inventory\","
            + "  \"authentication\": {"
            + "    \"type\": \"Login\","
            + "    \"user\": \"naoki\","
            + "    \"password\": \"secret\""
            + "  }"
            + "}";
  }

  @Test
  public void testValidMySqlSourceConfigValidator() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    ImportSourceConfigValidator.newValidator(config).validate();
  }

  @Test
  public void missingSourceName() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.setImportSourceName(null);

    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception.getMessage(), startsWith("importSourceConfig: importSourceName is required"));
  }

  @Test
  public void missingSourceId() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.setImportSourceId(null);

    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception.getMessage(), startsWith("importSourceConfig: importSourceId is required"));
  }

  @Test
  public void mySqlMissingDatabaseHost() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);

    // missing
    config.setDatabaseHost(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception.getMessage(),
        is(
            "importSourceConfig: One of properties (databaseHost, endpoint) must be set;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));

    // invalid
    config.setDatabaseHost(" ");
    final var exception2 =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception2.getMessage(),
        is(
            "importSourceConfig: One of properties (databaseHost, endpoint) must be set;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));
  }

  @Test
  public void mySqlMissingDatabaseName() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);

    // missing
    config.setDatabaseName(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception.getMessage(),
        is(
            "importSourceConfig.databaseName: Property must be set;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));

    // invalid
    config.setDatabaseName(" ");
    final var exception2 =
        assertThrows(
            InvalidValueValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());

    assertThat(
        exception2.getMessage(),
        is(
            "importSourceConfig.databaseName: Value must not be blank;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));
  }

  @Test
  public void mySqlPortIsOptional() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.setDatabasePort(null);
    ImportSourceConfigValidator.newValidator(config).validate();
  }

  @Test
  public void mySqlSslSimplest() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    final var ssl = new IntegrationsSslConfig();
    ssl.setMode(SslMode.ENABLED_SKIP_VALIDATION);
    config.setSsl(ssl);
    ImportSourceConfigValidator.newValidator(config).validate();
  }

  @Test
  public void mySqlSslSimplest2() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    final var ssl = new IntegrationsSslConfig();
    ssl.setMode(SslMode.DISABLED);
    config.setSsl(ssl);
    ImportSourceConfigValidator.newValidator(config).validate();
  }

  @Test
  public void mySqlSslModeIsRequiredIfSslConfigured() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    final var ssl = new IntegrationsSslConfig();
    config.setSsl(ssl);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());
    assertThat(
        exception.getMessage(),
        is(
            "importSourceConfig.ssl.mode: Property must be set;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));
  }

  @Test
  public void mySqlAuthMissing() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.setAuthentication(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());
    assertThat(
        exception.getMessage(),
        is(
            "importSourceConfig.authentication: Property must be set;"
                + " importSourceName=MySQL Inventory, importSourceId=209, type=Mysql"));
  }

  @Test
  public void mySqlAuthTypeMissing() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.getAuthentication().setType(null);
    final var exception =
        assertThrows(
            ConstraintViolationValidatorException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());
    System.out.println(exception.getMessage());
  }

  @Test
  public void mySqlAuthTypeNotLogin() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.getAuthentication().setType(IntegrationsAuthenticationType.ACCESS_KEY);
    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> ImportSourceConfigValidator.newValidator(config).validate());
  }

  @Test
  public void mySqlAuthTypeUserMissing() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.getAuthentication().setUser(null);
    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> ImportSourceConfigValidator.newValidator(config).validate());
  }

  @Test
  public void mySqlAuthTypePasswordMissing() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.getAuthentication().setPassword(null);
    assertThrows(
        ConstraintViolationValidatorException.class,
        () -> ImportSourceConfigValidator.newValidator(config).validate());
  }

  @Test
  public void mySqlMultipleErrors() throws Exception {
    final var config = mapper.readValue(mySqlSource, ImportSourceConfig.class);
    config.setImportSourceName(null);
    config.setDatabaseHost(" ");
    config.getAuthentication().setPassword(null);
    final var exception =
        assertThrows(
            MultipleValidatorViolationsException.class,
            () -> ImportSourceConfigValidator.newValidator(config).validate());
    assertThat(
        exception.getMessage(),
        is(
            "3 violations:"
                + " [0] CONSTRAINT_VIOLATION - importSourceConfig: importSourceName is required,"
                + " [1] CONSTRAINT_VIOLATION - importSourceConfig: One of properties"
                + " (databaseHost, endpoint) must be set,"
                + " [2] CONSTRAINT_VIOLATION - importSourceConfig.authentication.password:"
                + " Property 'password' is required; importSourceId=209, type=Mysql"));
  }
}
