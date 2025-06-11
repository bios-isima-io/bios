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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;

/** Rollup Cache Message. */
@JsonInclude(Include.NON_NULL)
public class RollupCacheRequest {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  public RollupCacheRequest() {}

  public RollupCacheRequest(String tenantName, String signalName, String rollupName) {
    //
    this.tenantName = tenantName;
    this.signalName = signalName;
    this.rollupName = rollupName;
  }

  /** Tenant name. */
  @JsonProperty("tenantName")
  private String tenantName;

  /** Signal name. */
  @JsonProperty("signalName")
  private String signalName;

  /** Signal name. */
  @JsonProperty("rollupName")
  private String rollupName;

  /** Feature version. */
  @JsonProperty("rollupVersion")
  private Long rollupVersion;

  /** Rollup timestamp */
  @JsonProperty("rollupTimestamp")
  private Long rollupTimestamp;

  /** Events in the rollup. */
  @JsonProperty("events")
  private List<EventJson> events;

  public String getTenantName() {
    return tenantName;
  }

  public void setTenantName(String tenantName) {
    this.tenantName = tenantName;
  }

  public String getSignalName() {
    return signalName;
  }

  public void setSignalName(String signalName) {
    this.signalName = signalName;
  }

  public String getRollupName() {
    return rollupName;
  }

  public void setRollupName(String rollupName) {
    this.rollupName = rollupName;
  }

  public Long getRollupVersion() {
    return rollupVersion;
  }

  public void setRollupVersion(Long rollupVersion) {
    this.rollupVersion = rollupVersion;
  }

  public List<? extends Event> getEvents() {
    return events;
  }

  @SuppressWarnings("unchecked")
  @JsonIgnore
  public void setEvents(List<? extends Event> events) {
    this.events = (List<EventJson>) events;
  }

  public Long getRollupTimestamp() {
    return rollupTimestamp;
  }

  public void setRollupTimestamp(Long rollupTimestamp) {
    this.rollupTimestamp = rollupTimestamp;
  }

  @Override
  public String toString() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "";
    }
  }
}
