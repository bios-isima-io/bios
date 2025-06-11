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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.isima.bios.models.GenericMetric;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"context", "where", "groupBy", "orderBy", "limit", "onTheFly"})
public class SelectContextRequest {
  @NotNull
  @JsonProperty("context")
  private String context;

  @JsonProperty("attributes")
  private List<String> attributes;

  @Valid
  @JsonProperty("metrics")
  private List<GenericMetric> metrics;

  @JsonProperty("where")
  private String where;

  @JsonProperty("groupBy")
  private List<String> groupBy;

  @JsonProperty("orderBy")
  private SelectOrder orderBy;

  @JsonProperty("limit")
  private Integer limit;

  @JsonProperty("onTheFly")
  private Boolean onTheFly;

  public static boolean isEmpty(SelectContextRequest request) {
    if ((request.context == null)
        && (request.attributes == null)
        && (request.metrics == null)
        && (request.where == null)
        && (request.groupBy == null)
        && (request.orderBy == null)
        && (request.limit == null)
        && (request.onTheFly == null)) {
      return true;
    }
    return false;
  }
}
