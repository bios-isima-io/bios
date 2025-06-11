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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;

/** Classed to be used for response of an Admin write operation. */
@JsonInclude(Include.NON_NULL)
public class AdminWriteResponse {
  /** Operation time stamp in millisecond. */
  private Long timestamp;

  /** List of endpoints. */
  private List<String> endpoints;

  public AdminWriteResponse() {}

  public AdminWriteResponse(Long timestamp, List<String> endpoints) {
    if (timestamp == null) {
      throw new IllegalArgumentException("parameter 'timestamp' must not be null");
    }
    if (endpoints == null) {
      throw new IllegalArgumentException("parameter 'endpoints' must not be null");
    }
    this.timestamp = timestamp;
    this.endpoints = endpoints;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public AdminWriteResponse setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public List<String> getEndpoints() {
    return endpoints;
  }

  public AdminWriteResponse setEndpoints(List<String> endpoints) {
    this.endpoints = endpoints;
    return this;
  }
}
