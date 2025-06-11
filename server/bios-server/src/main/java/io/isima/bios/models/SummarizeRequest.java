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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
// @ValidExtractRequest
public class SummarizeRequest {

  @NotNull private Long startTime;

  // This parameter is used by BIOS API. TFOS API cannot set this value.
  // When this is set, the server does not calculate snapped start time internally
  // but uses this value for the starting point of summarize calculation.
  @JsonIgnore private Long snappedStartTime;

  private Long origEndTime;

  @NotNull private Long endTime;

  @NotNull private Long interval;

  @JsonDeserialize(using = TfosObjectMapperProvider.TimeZoneDeserializer.class)
  private TimeZone timezone;

  private Long horizon;

  @Valid @NotNull private List<Aggregate> aggregates;

  private List<String> group = Collections.emptyList();

  private View sort;

  private Integer limit;

  private String filter;

  @Getter(AccessLevel.NONE)
  private Boolean onTheFly;

  public boolean isOnTheFly() {
    return onTheFly != null && onTheFly;
  }
}
