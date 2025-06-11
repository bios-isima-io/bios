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
package io.isima.bios.models;

import java.util.Objects;
import java.util.function.Supplier;

/** Enum to represent categories in the tenant appendix table. */
public enum TenantAppendixCategory {
  IMPORT_SOURCES(ImportSourceConfig.class, TenantAppendixSpec.ImportSource::new),
  IMPORT_DESTINATIONS(ImportDestinationConfig.class, TenantAppendixSpec.ImportDestination::new),
  IMPORT_FLOW_SPECS(ImportFlowConfig.class, TenantAppendixSpec.ImportFlowSpec::new),
  IMPORT_DATA_PROCESSORS(
      ImportDataProcessorConfig.class, TenantAppendixSpec.ImportDataProcessorSpec::new),
  EXPORT_TARGETS(ExportDestinationConfig.class, TenantAppendixSpec.ExportDestinations::new),
  APPS_INFO(AppsInfo.class, TenantAppendixSpec.BiosAppsInfo::new);

  private final Class<?> configClass;
  private final Supplier<TenantAppendixSpec<?>> specSupplier;

  private TenantAppendixCategory(
      Class<?> configClass, Supplier<TenantAppendixSpec<?>> specSupplier) {
    this.configClass = configClass;
    this.specSupplier = specSupplier;
  }

  /** Returns the class of tenant appendix content for the category. */
  public Class<?> getConfigClass() {
    return configClass;
  }

  /**
   * Supplies a tenant appendix spec.
   *
   * @param entryId Entry ID, nullable.
   * @param content Tenant appendix content, not nullable
   * @return Created tenant appendix
   */
  public TenantAppendixSpec<?> supplyAppendixSpec(String entryId, Object content) {
    Objects.requireNonNull(content);
    final var spec = specSupplier.get();
    spec.setEntryId(entryId);
    spec.setContentAsObject(content);
    return spec;
  }
}
