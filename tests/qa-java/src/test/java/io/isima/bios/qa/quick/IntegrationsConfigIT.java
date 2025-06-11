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
package io.isima.bios.qa.quick;

import static io.isima.bios.qautils.TestConfig.admin_pass;
import static io.isima.bios.qautils.TestConfig.admin_user;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.DataPickupSpec;
import io.isima.bios.models.DestinationDataSpec;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ExportStatus;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportDestinationType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportPayloadType;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.IntegrationsAuthentication;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.SourceDataSpec;
import io.isima.bios.qautils.TestConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class IntegrationsConfigIT {
  private static final String TENANT_NAME = "biosClientE2eTest";

  private static String host;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() {
    assumeTrue(TestConfig.isIntegrationTest());
    host = TestConfig.getEndpoint();
    port = TestConfig.getPort();
  }

  @Before
  public void setUp() {}

  @Test
  public void importSourceCrudIdSpecified() throws Exception {
    importSourceCrud("testSourceId");
  }

  @Test
  public void importSourceCrudIdUnspecified() throws Exception {
    importSourceCrud(null);
  }

  private void importSourceCrud(String entryId) throws Exception {
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      final var importSourceConfig = makeImportSourceConfig(entryId);

      boolean deleted = false;
      try {
        final var created = session.createImportSource(importSourceConfig);
        assertThat(created.getImportSourceId(), notNullValue());
        entryId = created.getImportSourceId();
        importSourceConfig.setImportSourceId(entryId);
        assertThat(created, equalTo(importSourceConfig));

        final var retrieved = session.getImportSource(entryId);
        assertThat(retrieved, equalTo(created));

        importSourceConfig.setImportSourceName("renamed");
        final var updated = session.updateImportSource(entryId, importSourceConfig);
        assertThat(updated, equalTo(importSourceConfig));

        final var retrieved2 = session.getImportSource(entryId);
        assertThat(retrieved2, equalTo(updated));

        session.deleteImportSource(entryId);
        deleted = true;

        final var id = entryId;
        var exception = assertThrows(BiosClientException.class, () -> session.getImportSource(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));

        exception = assertThrows(BiosClientException.class, () -> session.deleteImportSource(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));
      } finally {
        if (!deleted && entryId != null) {
          session.deleteImportSource(entryId);
        }
      }
    }
  }

  @Test
  public void importDestinationCrudIdSpecified() throws Exception {
    importDestinationCrud("testDestId");
  }

  @Test
  public void importDestinationCrudIdUnspecified() throws Exception {
    importDestinationCrud(null);
  }

  private void importDestinationCrud(String entryId) throws Exception {
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      final var importDestinationConfig = makeImportDestinationConfig(entryId);

      boolean deleted = false;
      try {
        final var created = session.createImportDestination(importDestinationConfig);
        assertThat(created.getImportDestinationId(), notNullValue());
        entryId = created.getImportDestinationId();
        importDestinationConfig.setImportDestinationId(entryId);
        assertThat(created, equalTo(importDestinationConfig));

        final var retrieved = session.getImportDestination(entryId);
        assertThat(retrieved, equalTo(created));

        importDestinationConfig.setImportDestinationName("renamed");
        final var updated = session.updateImportDestination(entryId, importDestinationConfig);
        assertThat(updated, equalTo(importDestinationConfig));

        final var retrieved2 = session.getImportDestination(entryId);
        assertThat(retrieved2, equalTo(updated));

        session.deleteImportDestination(entryId);
        deleted = true;

        final var id = entryId;
        var exception =
            assertThrows(BiosClientException.class, () -> session.getImportDestination(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));

        exception =
            assertThrows(BiosClientException.class, () -> session.deleteImportDestination(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));
      } finally {
        if (!deleted && entryId != null) {
          session.deleteImportDestination(entryId);
        }
      }
    }
  }

  @Test
  public void importDataProcessorCrud() throws Exception {
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      String code = "" + "def hello(src):\n" + "    return src.lower() if src else ''";

      final var processorName = "testProcessor";
      final var processorConfig = new ImportDataProcessorConfig();
      processorConfig.setProcessorName(processorName);
      processorConfig.setCode(Base64.getEncoder().encodeToString(code.getBytes()));
      processorConfig.setEncoding(ImportDataProcessorConfig.Encoding.BASE64);

      boolean deleted = false;
      try {
        session.createImportDataProcessor(processorConfig);

        final var retrieved = session.getImportDataProcessor(processorName);
        assertThat(retrieved, equalTo(processorConfig));

        String newCode = "" + "def hello(src):\n" + "    return src.upper() if src else ''";

        processorConfig.setCode(Base64.getEncoder().encodeToString(newCode.getBytes()));
        session.updateImportDataProcessor(processorName, processorConfig);

        final var retrieved2 = session.getImportDataProcessor(processorName);
        assertThat(retrieved2, equalTo(processorConfig));

        session.deleteImportDataProcessor(processorName);
        deleted = true;

        var exception =
            assertThrows(
                BiosClientException.class, () -> session.getImportDataProcessor(processorName));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));

        exception =
            assertThrows(
                BiosClientException.class, () -> session.deleteImportDataProcessor(processorName));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));
      } finally {
        if (!deleted) {
          session.deleteImportDataProcessor(processorName);
        }
      }
    }
  }

  @Test
  @Ignore("Unable to run without AWS")
  public void exportDestinationCrudIdSpecified() throws Exception {
    exportDestinationCrud("testExportDestId");
  }

  @Test
  @Ignore("Unable to run without AWS")
  public void exportDestinationCrudIdUnspecified() throws Exception {
    exportDestinationCrud(null);
  }

  private void exportDestinationCrud(String entryId) throws Exception {
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      final var exportDestinationConfig = makeExportDestinationConfig(entryId);

      boolean deleted = false;
      try {
        final var created = session.createExportDestination(exportDestinationConfig);
        assertThat(created.getExportDestinationId(), notNullValue());
        entryId = created.getExportDestinationId();
        exportDestinationConfig.setExportDestinationId(entryId);
        assertThat(created, equalTo(exportDestinationConfig));

        final var retrieved = session.getExportDestination(entryId);
        assertThat(retrieved, equalTo(created));

        exportDestinationConfig.setExportDestinationName("renamed");
        final var updated = session.updateExportDestination(entryId, exportDestinationConfig);
        assertThat(updated, equalTo(exportDestinationConfig));

        final var retrieved2 = session.getExportDestination(entryId);
        assertThat(retrieved2, equalTo(updated));

        session.deleteExportDestination(entryId);
        deleted = true;

        final var id = entryId;
        var exception =
            assertThrows(BiosClientException.class, () -> session.getExportDestination(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));

        exception =
            assertThrows(BiosClientException.class, () -> session.deleteExportDestination(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));
      } finally {
        if (!deleted && entryId != null) {
          session.deleteExportDestination(entryId);
        }
      }
    }
  }

  @Test
  public void importFlowCrudIdSpecified() throws Exception {
    importFlowCrud("testFlowId");
  }

  @Test
  public void importFlowCrudIdUnspecified() throws Exception {
    importFlowCrud(null);
  }

  private void importFlowCrud(String entryId) throws Exception {
    try (final Session session =
        Bios.newSession(host)
            .port(port)
            .user(admin_user + "@" + TENANT_NAME)
            .password(admin_pass)
            .connect()) {

      final var sourceId = "testImportSource";
      final var sourceConfig = makeImportSourceConfig(sourceId);

      final var destId = "testImportDestination";
      final var destConfig = makeImportDestinationConfig(destId);

      final var signalName = "integrationConfigIT";
      final var signalConfig = new SignalConfig(signalName);
      signalConfig.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
      signalConfig.setAttributes(
          List.of(
              new AttributeConfig("one", AttributeType.STRING),
              new AttributeConfig("two", AttributeType.STRING)));

      final var importFlowConfig = new ImportFlowConfig();
      importFlowConfig.setImportFlowId(entryId);
      importFlowConfig.setImportFlowName("testImport");
      final var sourceSpec = new SourceDataSpec();
      sourceSpec.setImportSourceId(sourceId);
      sourceSpec.setPayloadType(ImportPayloadType.JSON);
      importFlowConfig.setSourceDataSpec(sourceSpec);
      final var destSpec = new DestinationDataSpec();
      destSpec.setImportDestinationId(destId);
      destSpec.setName(signalName);
      destSpec.setType(ImportDestinationStreamType.SIGNAL);
      importFlowConfig.setDestinationDataSpec(destSpec);
      final var dataPickupSpec = new DataPickupSpec();
      dataPickupSpec.setAttributeSearchPath("");
      final var one = new DataPickupSpec.Attribute();
      one.setSourceAttributeName("one");
      final var two = new DataPickupSpec.Attribute();
      two.setSourceAttributeName("two");
      dataPickupSpec.setAttributes(List.of(one, two));
      importFlowConfig.setDataPickupSpec(dataPickupSpec);

      try {
        session.createSignal(signalConfig);
        session.createImportSource(sourceConfig);
        session.createImportDestination(destConfig);

        final var created = session.createImportFlowSpec(importFlowConfig);
        assertThat(created.getImportFlowId(), notNullValue());
        entryId = created.getImportFlowId();
        importFlowConfig.setImportFlowId(entryId);
        assertThat(created, equalTo(importFlowConfig));

        final var retrieved = session.getImportFlowSpec(entryId);
        assertThat(retrieved, equalTo(created));

        importFlowConfig.setImportFlowName("renamed");
        final var updated = session.updateImportFlowSpec(entryId, importFlowConfig);
        assertThat(updated, equalTo(importFlowConfig));

        final var retrieved2 = session.getImportFlowSpec(entryId);
        assertThat(retrieved2, equalTo(updated));

        session.deleteImportFlowSpec(entryId);

        final var id = entryId;
        var exception =
            assertThrows(BiosClientException.class, () -> session.getImportFlowSpec(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));

        exception = assertThrows(BiosClientException.class, () -> session.deleteImportFlowSpec(id));
        assertThat(exception.getCode(), is(BiosClientError.NOT_FOUND));
      } finally {
        if (entryId != null) {
          try {
            session.deleteImportFlowSpec(entryId);
          } catch (BiosClientException e) {
            // ignore
          }
        }
        try {
          session.deleteImportSource(sourceId);
        } catch (BiosClientException e) {
          // ignore
        }
        try {
          session.deleteImportDestination(destId);
        } catch (BiosClientException e) {
          // ignore
        }
        try {
          session.deleteSignal(signalName);
        } catch (BiosClientException e) {
          // ignore
        }
      }
    }
  }

  private ImportSourceConfig makeImportSourceConfig(String entryId) {
    final var importSourceConfig = new ImportSourceConfig();
    importSourceConfig.setImportSourceId(entryId);
    importSourceConfig.setImportSourceName("testSource");
    importSourceConfig.setType(ImportSourceType.WEBHOOK);
    importSourceConfig.setWebhookPath("/alerts");
    final var auth = new IntegrationsAuthentication();
    auth.setType(IntegrationsAuthenticationType.HTTP_HEADERS_PLAIN);
    auth.setUserHeader("x-bios-user");
    auth.setPasswordHeader("x-bios-password");
    importSourceConfig.setAuthentication(auth);
    return importSourceConfig;
  }

  private ImportDestinationConfig makeImportDestinationConfig(String entryId) {
    final var importDestinationConfig = new ImportDestinationConfig();
    importDestinationConfig.setImportDestinationId(entryId);
    importDestinationConfig.setImportDestinationName("testSource");
    importDestinationConfig.setType(ImportDestinationType.BIOS);
    final var auth = new IntegrationsAuthentication();
    auth.setType(IntegrationsAuthenticationType.LOGIN);
    auth.setUser("example@isima.io");
    auth.setPassword("password");
    importDestinationConfig.setAuthentication(auth);
    return importDestinationConfig;
  }

  private ExportDestinationConfig makeExportDestinationConfig(String entryId) {
    final var importDestinationConfig = new ExportDestinationConfig();
    importDestinationConfig.setExportDestinationId(entryId);
    importDestinationConfig.setExportDestinationName("testSource");
    importDestinationConfig.setStorageType("S3");
    importDestinationConfig.setStatus(ExportStatus.DISABLED);
    importDestinationConfig.setStorageConfig(
        Map.of(
            "s3BucketName", "export-s3-bios-test",
            "s3AccessKeyId", "",
            "s3SecretAccessKey", "",
            "s3Region", "ap-south-1"));
    return importDestinationConfig;
  }
}
