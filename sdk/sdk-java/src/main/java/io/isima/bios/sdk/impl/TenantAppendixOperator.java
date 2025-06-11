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

import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ImportDataProcessorConfig;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.models.TenantAppendixSpec;
import io.isima.bios.sdk.csdk.BiosClient;
import io.isima.bios.sdk.csdk.CSdkOperationId;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Objects;

public class TenantAppendixOperator {

  private final BiosClient biosClient;

  public TenantAppendixOperator(BiosClient biosClient) {
    this.biosClient = biosClient;
  }

  public <ConfigT> ConfigT create(TenantAppendixCategory category, String entryId, ConfigT config)
      throws BiosClientException {
    final var spec = makeSpec(Objects.requireNonNull(config), Objects.requireNonNull(category));
    spec.setContent(config);
    spec.setEntryId(entryId);

    final String[] resources =
        new String[] {
          "category", category.name(),
        };

    final var response =
        biosClient
            .makeGenericMethodCaller(
                CSdkOperationId.CREATE_TENANT_APPENDIX,
                null,
                resources,
                null,
                spec,
                TenantAppendixSpec.class)
            .invoke();
    return (ConfigT) response.getContent();
  }

  public <ConfigT> ConfigT get(TenantAppendixCategory category, String entryId)
      throws BiosClientException {
    final String[] resources =
        new String[] {
          "category", Objects.requireNonNull(category).name(),
          "entryId", Objects.requireNonNull(entryId),
        };
    final var response =
        biosClient
            .makeGenericMethodCaller(
                CSdkOperationId.GET_TENANT_APPENDIX,
                null,
                resources,
                null,
                null,
                TenantAppendixSpec.class)
            .invoke();
    return (ConfigT) response.getContent();
  }

  public <ConfigT> ConfigT update(TenantAppendixCategory category, String entryId, ConfigT config)
      throws BiosClientException {
    final var spec = makeSpec(Objects.requireNonNull(config), Objects.requireNonNull(category));
    spec.setContent(config);
    spec.setEntryId(Objects.requireNonNull(entryId));

    final String[] resources =
        new String[] {
          "category", category.name(), "entryId", entryId,
        };

    final var response =
        biosClient
            .makeGenericMethodCaller(
                CSdkOperationId.UPDATE_TENANT_APPENDIX,
                null,
                resources,
                null,
                spec,
                TenantAppendixSpec.class)
            .invoke();
    return (ConfigT) response.getContent();
  }

  public void delete(TenantAppendixCategory category, String entryId) throws BiosClientException {
    final String[] resources =
        new String[] {
          "category", Objects.requireNonNull(category).name(), "entryId", entryId,
        };
    biosClient
        .makeGenericMethodCaller(
            CSdkOperationId.DELETE_TENANT_APPENDIX, null, resources, null, null, Void.class)
        .invoke();
  }

  protected static <T> TenantAppendixSpec<T> makeSpec(T config, TenantAppendixCategory category) {
    if (config instanceof ImportSourceConfig) {
      assert category == TenantAppendixCategory.IMPORT_SOURCES;
      return (TenantAppendixSpec<T>) new TenantAppendixSpec.ImportSource();
    }
    if (config instanceof ImportDestinationConfig) {
      assert category == TenantAppendixCategory.IMPORT_DESTINATIONS;
      return (TenantAppendixSpec<T>) new TenantAppendixSpec.ImportDestination();
    }
    if (config instanceof ImportFlowConfig) {
      assert category == TenantAppendixCategory.IMPORT_FLOW_SPECS;
      return (TenantAppendixSpec<T>) new TenantAppendixSpec.ImportFlowSpec();
    }
    if (config instanceof ImportDataProcessorConfig) {
      assert category == TenantAppendixCategory.IMPORT_DATA_PROCESSORS;
      return (TenantAppendixSpec<T>) new TenantAppendixSpec.ImportDataProcessorSpec();
    }
    if (config instanceof ExportDestinationConfig) {
      assert category == TenantAppendixCategory.EXPORT_TARGETS;
      return (TenantAppendixSpec<T>) new TenantAppendixSpec.ExportDestinations();
    }
    throw new UnsupportedOperationException(
        "Unsupported tenant appendix class " + config.getClass().getName());
  }
}
