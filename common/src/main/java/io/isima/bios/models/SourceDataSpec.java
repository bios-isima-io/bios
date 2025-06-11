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
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class SourceDataSpec {
  @NotNull private String importSourceId;

  @NotNull private ImportPayloadType payloadType;

  // used only for payloadType == ImportPayloadType.CSV
  private Boolean headerPresent;

  private Integer pullIntervalSeconds;

  private Integer sourceBatchSize;

  // webhook specific /////////////////////////////////////////
  private String webhookSubPath;

  // file specific ////////////////////////////////////////////
  private String fileNamePrefix;

  // Hibernate specific ///////////////////////////////////////
  private String hibernateEntity;

  // Kafka specific ///////////////////////////////////////////
  private String topic;

  private String clientId;

  private String groupId;

  private Map<String, Object> kwArgs;

  // s3 specific //////////////////////////////////////////////
  private String s3Bucket;

  // database specific ////////////////////////////////////////
  private String tableName;

  private List<CdcOperationType> cdcOperationTypes;

  // table timestamp columnName
  private String columnName;

  @Deprecated private List<String> cdcAdditionalAttributes;

  private String serverId;

  // rest client specific /////////////////////////////////////
  private Map<String, String> headers;

  private Map<String, String> bodyParams;

  private Map<String, String> queryParams;

  private String subPath;
}
