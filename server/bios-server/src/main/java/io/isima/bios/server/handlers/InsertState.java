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
package io.isima.bios.server.handlers;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.proto.DataProto.InsertRequest;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Getter;

public class InsertState extends ExecutionState {
  // initial parameters
  private UUID eventId;
  private InsertRequest insertRequest; // erasable, can be large
  // TODO(BIOS-4937): Remove this
  @Getter private DataEngine dataEngine;

  // parameters filled during the operation
  private Event event;

  public InsertState(Executor executor, DataEngine dataEngine) {
    super("Insert", executor);
    this.dataEngine = dataEngine;
  }

  public InsertState(
      String tenantName,
      String signalName,
      StreamDesc signalDesc,
      DataEngine dataEngine,
      ExecutionState parent) {
    super("Insert", parent);

    this.tenantName = tenantName;
    this.streamName = signalName;
    this.streamDesc = signalDesc;
    this.dataEngine = dataEngine;
  }

  public void setInitialParams(
      String tenantName, String signalName, UUID eventId, InsertRequest insertRequest) {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(signalName);
    Objects.requireNonNull(eventId);
    Objects.requireNonNull(insertRequest);
    this.tenantName = tenantName;
    this.streamName = signalName;
    this.eventId = eventId;
    this.insertRequest = insertRequest;
  }

  public UUID getEventId() {
    return eventId;
  }

  public InsertRequest getRequest() {
    return insertRequest;
  }

  public InsertState setSignalDesc(StreamDesc signalDesc) {
    addHistory("(setSignal");
    if (this.streamDesc != null) {
      throw new IllegalStateException("signal is set already");
    }
    Objects.requireNonNull(signalDesc);
    this.streamDesc = signalDesc;
    addHistory(")");
    return this;
  }

  public InsertState setEvent(Event event) {
    addHistory("(setEvent");
    if (this.event != null) {
      throw new IllegalStateException("event is set already");
    }
    this.event = event;
    addHistory(")");
    return this;
  }

  public StreamDesc getSignalDesc() {
    if (streamDesc == null) {
      throw new IllegalStateException("signalDesc is not set");
    }
    return streamDesc;
  }

  public Event getEvent(boolean verify) {
    if (event == null && verify) {
      throw new IllegalStateException("record must be set");
    }
    return event;
  }

  /*
  public boolean isEnrichConfigured() {
    // TODO we may want to cache the result
    return signalDesc != null && signalDesc.getEnrich() != null
        && signalDesc.getEnrich().getEnrichments() != null
        && !signalDesc.getEnrich().getEnrichments().isEmpty();
  }
   */
}
