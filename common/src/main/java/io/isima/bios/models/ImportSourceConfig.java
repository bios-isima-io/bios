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
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Import source configuration. */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class ImportSourceConfig {

  private String importSourceId;

  @NotNull private String importSourceName;

  @NotNull private ImportSourceType type;

  @Valid private IntegrationsAuthentication authentication;

  private IntegrationsPayloadValidation payloadValidation;

  private String importDestinationId;

  // webhook specific ////////////////////////
  private String webhookPath;

  // file specific ///////////////////////////
  private String fileLocation;

  // kafka specific //////////////////////////
  private List<String> bootstrapServers;

  private List<Integer> apiVersion;

  // mongodb specific ////////////////////////
  private Boolean useDnsSeedList;

  private String replicaSet;

  private String authSource;

  // database specific ///////////////////////
  private String databaseHost;

  private Integer databasePort;

  private String databaseName;

  // postgres specific ///////////////////////
  private String slotName;

  @Valid private IntegrationsSslConfig ssl;

  // rest client specific ////////////////////
  private Method method;

  // Used by CDC, REST and S3 clients /////////////
  private String endpoint;

  // Used by mongodb client //////////////////
  private List<String> endpoints;

  private Long pollingInterval;

  // Used by google ad clients ///////////////
  private String customerID;
}
