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
package io.isima.bios.admin.v1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.isima.bios.admin.Admin;
import io.isima.bios.admin.TenantAppendix;
import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.it.tools.Bios2TestModules;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ExportStatus;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.utils.StringUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantAppendixTest {
  private static final Logger logger = LoggerFactory.getLogger(TenantAppendixTest.class);

  private static Admin admin;
  private static TenantAppendix tenantAppendix;
  private static String tenantName;
  private static EntityId tenantId;

  private static boolean success;

  @Rule
  public TestWatcher watchman =
      new TestWatcher() {
        @Override
        protected void failed(Throwable t, Description description) {
          success = false;
        }
      };

  /** Setup BIOS modules, stop maintenance, and create a test tenant. */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(TenantAppendixTest.class);
    admin = BiosModules.getAdmin();
    tenantAppendix = BiosModules.getTenantAppendix();
    tenantName = TenantAppendixTest.class.getSimpleName();
    try {
      final Long timestamp = System.currentTimeMillis();
      admin.deleteTenant(tenantName, RequestPhase.INITIAL, timestamp);
      admin.deleteTenant(tenantName, RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    final Long timestamp = System.currentTimeMillis();
    final var tenantConfig = new TenantConfig(tenantName);
    admin.createTenant(tenantConfig, RequestPhase.INITIAL, timestamp, false);
    admin.createTenant(tenantConfig, RequestPhase.FINAL, timestamp, false);
    final var created = admin.getTenant(tenantName, false, false, false, List.of());
    tenantId = new EntityId(created);
    success = true;
  }

  /** Delete the test tenant and shutdown modules. */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    final Long timestamp = System.currentTimeMillis();
    // cleanup the appendix if the test is successful
    if (success) {
      tenantAppendix.hardDelete(tenantId);
    } else {
      logger.error(
          "TEST {} FAILED tenant={}, version={}",
          TenantAppendixTest.class.getSimpleName(),
          tenantId.getName(),
          StringUtils.tsToIso8601(tenantId.getVersion()));
    }
    try {
      admin.deleteTenant(tenantName, RequestPhase.INITIAL, timestamp);
      admin.deleteTenant(tenantName, RequestPhase.FINAL, timestamp);
    } catch (TfosException | ApplicationException e) {
      // ignore
    }
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() {}

  /** Runs fundamental operations. */
  @Test
  public void test() throws Exception {
    final var tenantConfig = admin.getTenant(tenantName, true, false, false, List.of());
    assertTrue(tenantConfig.getImportSources().isEmpty());

    TenantAppendixCategory category = TenantAppendixCategory.IMPORT_SOURCES;

    // Create an appendix
    String importSourceId1 = UUID.randomUUID().toString();
    final var importSource1 = new ImportSourceConfig();
    importSource1.setImportSourceId(importSourceId1);
    importSource1.setType(ImportSourceType.WEBHOOK);
    importSource1.setWebhookPath("/system");
    tenantAppendix.createEntry(tenantId, category, importSourceId1, importSource1);
    var retrieved =
        tenantAppendix.getEntry(tenantId, category, importSourceId1, ImportSourceConfig.class);
    assertThat(retrieved, is(importSource1));

    // Create another appendix
    String importSourceId2 = UUID.randomUUID().toString();
    final var importSource2 = new ImportSourceConfig();
    importSource2.setImportSourceId(importSourceId2);
    importSource2.setType(ImportSourceType.FILE);
    tenantAppendix.createEntry(tenantId, category, importSourceId2, importSource2);
    retrieved =
        tenantAppendix.getEntry(tenantId, category, importSourceId2, ImportSourceConfig.class);
    assertThat(retrieved, is(importSource2));
    // verify the first one was not overwritten
    retrieved =
        tenantAppendix.getEntry(tenantId, category, importSourceId1, ImportSourceConfig.class);
    assertThat(retrieved, is(importSource1));

    // Verify an entry cannot be overwritten
    assertThrows(
        AlreadyExistsException.class,
        () -> tenantAppendix.createEntry(tenantId, category, importSourceId1, importSource1));

    // Update an entry
    final var importSource3 = new ImportSourceConfig();
    importSource3.setImportSourceId(importSourceId1);
    importSource3.setType(ImportSourceType.WEBHOOK);
    importSource3.setWebhookPath("/deli/system");
    tenantAppendix.updateEntry(tenantId, category, importSourceId1, importSource3);
    retrieved =
        tenantAppendix.getEntry(tenantId, category, importSourceId1, ImportSourceConfig.class);
    assertThat(retrieved, is(importSource3));

    // File Tailer
    String importSourceId5 = UUID.randomUUID().toString();
    final var importSource5 = new ImportSourceConfig();
    importSource5.setImportSourceId(importSourceId5);
    importSource5.setType(ImportSourceType.FILE_TAILER);
    importSource5.setFileLocation("/var/log/file.log");

    // List entries via getTenant
    final var tenantConfig2 = admin.getTenant(tenantName, true, false, false, List.of());
    final List<ImportSourceConfig> importSources = tenantConfig2.getImportSources();
    assertThat(importSources.size(), is(2));
    assertThat(importSource2, isIn(importSources));
    assertThat(importSource3, isIn(importSources));

    // Delete an entry
    tenantAppendix.deleteEntry(tenantId, category, importSourceId1);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            tenantAppendix.getEntry(tenantId, category, importSourceId1, ImportSourceConfig.class));
    final List<ImportSourceConfig> importSources2 =
        admin.getTenant(tenantName, true, false, false, List.of()).getImportSources();
    assertThat(importSources2.size(), is(1));
    assertThat(importSource2, isIn(importSources2));

    // Kafka
    String importSourceId4 = UUID.randomUUID().toString();
    final var importSource4 = new ImportSourceConfig();
    importSource4.setImportSourceId(importSourceId1);
    importSource4.setType(ImportSourceType.KAFKA);
    importSource4.setApiVersion(Arrays.asList(0, 10, 2));
    importSource4.setBootstrapServers(Arrays.asList("34.93.18.170:9092"));
    tenantAppendix.createEntry(tenantId, category, importSourceId4, importSource4);
    retrieved =
        tenantAppendix.getEntry(tenantId, category, importSourceId4, ImportSourceConfig.class);
    assertThat(retrieved, is(importSource4));
  }

  @Test
  public void testExportInAppendix() throws Exception {
    final var tenantConfig = admin.getTenant(tenantName, true, false, false, List.of());
    assertTrue(tenantConfig.getExportDestinations().isEmpty());

    TenantAppendixCategory category = TenantAppendixCategory.EXPORT_TARGETS;

    String exportTargetName = "test123";
    final var exportTarget = new ExportDestinationConfig();
    exportTarget.setExportDestinationName(exportTargetName);
    exportTarget.setStatus(ExportStatus.ENABLED);
    exportTarget.setStorageType("S3");
    exportTarget.setStorageConfig(Map.of("s3BucketName", "bucket1", "region", "ap-south-1"));

    tenantAppendix.createEntry(tenantId, category, exportTargetName, exportTarget);
    var retrieved =
        tenantAppendix.getEntry(
            tenantId, category, exportTargetName, ExportDestinationConfig.class);
    assertThat(retrieved, is(exportTarget));
  }
}
