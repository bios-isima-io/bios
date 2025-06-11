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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Tenant schema. */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@ToString
public class TenantConfig {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public TenantConfig() {
    //
  }

  public TenantConfig(String name) {
    this.name = name;
  }

  /** Tenant name. */
  @JsonProperty("tenantName")
  private String name;

  /** Domain name of the tenant. */
  @JsonProperty("domain")
  private String domain;

  /** Tenant version. */
  @JsonProperty("version")
  private Long version;

  /** Signals in the tenant. */
  @JsonProperty("signals")
  private List<SignalConfig> signals;

  /** Contexts in the tenant. */
  @JsonProperty("contexts")
  private List<ContextConfig> contexts;

  /** Associated users. */
  @JsonProperty("users")
  private List<UserConfig> users;

  /** User email domain names that are associated with the tenant. */
  @JsonProperty("domains")
  private List<String> domains;

  /** Import source configurations. */
  @JsonProperty("importSources")
  private List<ImportSourceConfig> importSources;

  /** Import forwarding destination configurations. */
  @JsonProperty("importDestinations")
  private List<ImportDestinationConfig> importDestinations;

  /** Import flow specifications. */
  @JsonProperty("importFlowSpecs")
  private List<ImportFlowConfig> importFlowSpecs;

  /** Import data processors. */
  @JsonProperty("importDataProcessors")
  private List<ImportDataProcessorConfig> importDataProcessors;

  /** Export target config. */
  @JsonProperty("exportDestinations")
  private List<ExportDestinationConfig> exportDestinations;

  /** Tenant status. */
  @JsonProperty("status")
  private MemberStatus status;

  @JsonProperty("appMaster")
  private String appMaster;
}
