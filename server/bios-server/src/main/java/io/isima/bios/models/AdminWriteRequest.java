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
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AdminWriteRequest<T> {

  private Operation operation;

  @NotNull private RequestPhase phase;

  private Long timestamp;

  @NotNull @Valid private T payload;

  private Boolean force = Boolean.FALSE;

  public Operation getOperation() {
    return operation;
  }

  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  public RequestPhase getPhase() {
    return phase;
  }

  public void setPhase(RequestPhase phase) {
    this.phase = phase;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public T getPayload() {
    return payload;
  }

  public void setPayload(T payload) {
    this.payload = payload;
  }

  public Boolean getForce() {
    return force;
  }

  public void setForce(Boolean force) {
    this.force = force;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (operation != null) {
      sb.append("operation=").append(operation).append(", ");
    }
    sb.append("phase=")
        .append(phase)
        .append("")
        .append(", timestamp=")
        .append(timestamp)
        .append(", payload={")
        .append(payload)
        .append("}");
    return sb.toString();
  }
}
