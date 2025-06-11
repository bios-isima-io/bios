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
package io.isima.bios.sdk.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportDestinationType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantAppendixSpec;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantAppendixOperatorTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void importSourceSpecTest() throws Exception {
    final var config = new ImportSourceConfig();
    config.setImportSourceName("name");
    config.setType(ImportSourceType.KAFKA);

    assertThrows(
        AssertionError.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_DESTINATIONS));

    final var spec = TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES);

    final var encoded = objectMapper.writeValueAsString(spec);
    final var decoded = objectMapper.readValue(encoded, TenantAppendixSpec.class);

    assertThat(decoded, instanceOf(TenantAppendixSpec.ImportSource.class));
  }

  @Test
  public void importDestinationSpecTest() throws Exception {
    final var config = new ImportDestinationConfig();
    config.setImportDestinationName("name");
    config.setType(ImportDestinationType.BIOS);

    assertThrows(
        AssertionError.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES));

    final var spec =
        TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_DESTINATIONS);

    final var encoded = objectMapper.writeValueAsString(spec);
    final var decoded = objectMapper.readValue(encoded, TenantAppendixSpec.class);

    assertThat(decoded, instanceOf(TenantAppendixSpec.ImportDestination.class));
  }

  @Test
  public void importFlowSpecTest() throws Exception {
    final var config = new ImportFlowConfig();
    config.setImportFlowName("name");

    assertThrows(
        AssertionError.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES));

    final var spec =
        TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_FLOW_SPECS);

    final var encoded = objectMapper.writeValueAsString(spec);
    final var decoded = objectMapper.readValue(encoded, TenantAppendixSpec.class);

    assertThat(decoded, instanceOf(TenantAppendixSpec.ImportFlowSpec.class));
  }

  @Test
  public void importDataProcessorTest() throws Exception {
    final var config = new ImportDataProcessorConfig();
    config.setProcessorName("name");

    assertThrows(
        AssertionError.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES));

    final var spec =
        TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_DATA_PROCESSORS);

    final var encoded = objectMapper.writeValueAsString(spec);
    final var decoded = objectMapper.readValue(encoded, TenantAppendixSpec.class);

    assertThat(decoded, instanceOf(TenantAppendixSpec.ImportDataProcessorSpec.class));
  }

  @Test
  public void exportDestinationSpec() throws Exception {
    final var config = new ExportDestinationConfig();
    config.setExportDestinationName("name");

    assertThrows(
        AssertionError.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES));

    final var spec = TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.EXPORT_TARGETS);

    final var encoded = objectMapper.writeValueAsString(spec);
    final var decoded = objectMapper.readValue(encoded, TenantAppendixSpec.class);

    assertThat(decoded, instanceOf(TenantAppendixSpec.ExportDestinations.class));
  }

  @Test
  public void invalidSpec() throws Exception {
    final var config = "just a string";

    assertThrows(
        UnsupportedOperationException.class,
        () -> TenantAppendixOperator.makeSpec(config, TenantAppendixCategory.IMPORT_SOURCES));
  }
}
