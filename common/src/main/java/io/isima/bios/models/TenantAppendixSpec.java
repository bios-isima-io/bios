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

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.NoArgsConstructor;
import lombok.ToString;

@JsonInclude(Include.NON_NULL)
@JsonTypeInfo(use = NAME, include = PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = TenantAppendixSpec.ImportSource.class, name = "IMPORT_SOURCES"),
  @JsonSubTypes.Type(
      value = TenantAppendixSpec.ImportDestination.class,
      name = "IMPORT_DESTINATIONS"),
  @JsonSubTypes.Type(value = TenantAppendixSpec.ImportFlowSpec.class, name = "IMPORT_FLOW_SPECS"),
  @JsonSubTypes.Type(
      value = TenantAppendixSpec.ImportDataProcessorSpec.class,
      name = "IMPORT_DATA_PROCESSORS"),
  @JsonSubTypes.Type(value = TenantAppendixSpec.ExportDestinations.class, name = "EXPORT_TARGETS"),
  @JsonSubTypes.Type(value = TenantAppendixSpec.BiosAppsInfo.class, name = "BIOS_APPS"),
})
@ToString
@NoArgsConstructor
public abstract class TenantAppendixSpec<T> implements Cloneable {

  protected String entryId;

  @NotNull @Valid protected T content;

  public abstract void commitEntryId();

  public String getEntryId() {
    return entryId;
  }

  public void setEntryId(String entryId) {
    this.entryId = entryId;
  }

  public T getContent() {
    return content;
  }

  public void setContent(T content) {
    this.content = content;
  }

  @JsonIgnore
  public void setContentAsObject(Object content) {
    this.content = (T) content;
  }

  @NoArgsConstructor
  public static class ImportSource extends TenantAppendixSpec<ImportSourceConfig> {
    @Override
    public void commitEntryId() {
      content.setImportSourceId(entryId);
    }
  }

  @NoArgsConstructor
  public static class ImportDestination extends TenantAppendixSpec<ImportDestinationConfig> {
    @Override
    public void commitEntryId() {
      content.setImportDestinationId(entryId);
    }
  }

  @NoArgsConstructor
  public static class ImportFlowSpec extends TenantAppendixSpec<ImportFlowConfig> {
    @Override
    public void commitEntryId() {
      content.setImportFlowId(entryId);
    }
  }

  @NoArgsConstructor
  public static class ImportDataProcessorSpec
      extends TenantAppendixSpec<ImportDataProcessorConfig> {
    @Override
    public void commitEntryId() {
      content.setProcessorName(entryId);
    }
  }

  @NoArgsConstructor
  public static class ExportDestinations extends TenantAppendixSpec<ExportDestinationConfig> {
    @Override
    public void commitEntryId() {
      content.setExportDestinationId(entryId);
    }
  }

  @NoArgsConstructor
  public static class BiosAppsInfo extends TenantAppendixSpec<AppsInfo> {
    @Override
    public void commitEntryId() {
      // ID is not necessary, only one instance is allowed for a tenant
    }
  }
}
