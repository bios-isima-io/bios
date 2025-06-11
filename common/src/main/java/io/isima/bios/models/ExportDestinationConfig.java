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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "exportDestinationId",
  "exportDestinationName",
  "storageType",
  "status",
  "storageConfig"
})
public final class ExportDestinationConfig {
  /** Identifier to uniquely reference this external storage internally */
  @JsonProperty("exportDestinationId")
  private String exportDestinationId;

  /** Display name of the export destination. */
  @JsonProperty("exportDestinationName")
  private String exportDestinationName;

  /**
   * Destination type
   *
   * <p>By design, the storage configuration is opaque to the actual backend server and only visible
   * to the pluggable export modules such as Transformers and Loaders.
   */
  @JsonProperty("storageType")
  private String storageType;

  /** Current status of export, whether it is enabled or disabled. */
  @JsonProperty("status")
  private ExportStatus status;

  /**
   * Storage configuration specific to a storage type as an opaque map.
   *
   * <p>By design, the storage configuration is opaque to the actual backend server and only visible
   * to the pluggable export modules such as Transformers and Loaders. Only the transformer and
   * loader components deployed on the server know to validate and operate on this configuration.
   */
  @JsonProperty("storageConfig")
  private Map<String, String> storageConfig;

  public ExportDestinationConfig duplicate() {
    final var config = new ExportDestinationConfig();
    config.setStorageConfig(Collections.unmodifiableMap(storageConfig));
    config.setStatus(status);
    config.setStorageType(storageType);
    config.setExportDestinationName(exportDestinationName);
    config.setExportDestinationId(exportDestinationId);
    return config;
  }
}
