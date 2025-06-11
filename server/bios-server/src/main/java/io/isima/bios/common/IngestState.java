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
package io.isima.bios.common;

import io.isima.bios.data.DataEngine;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventExecution;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.recorder.OperationMetricGroupRecorder;
import io.isima.bios.recorder.OperationMetricsRecorder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import lombok.Getter;

/**
 * Class to carry ingest process state. Service request handler should create this object as per
 * receiving an ingest request.
 */
public class IngestState extends ExecutionState {
  @Getter private final DataEngine dataEngine;

  /** The ingest request object. */
  private IngestRequest input;

  /** Event object to process in this flow. */
  private Event event;

  /** Map used for storing intermediate state. */
  private final Map<String, Object> variables;

  // End state variables ///////////////////////////////////////////////

  /**
   * Fundamental {@link IngestState} constructor.
   *
   * @param recorder Metrics recorder. Set null to disable metrics collection.
   * @param tenant Tenant for which extract should happen
   * @param stream Stream for which extract should happen
   */
  public IngestState(
      OperationMetricsRecorder recorder,
      String tenant,
      String stream,
      Executor executor,
      DataEngine dataEngine) {
    super(EventExecution.INGEST.name(), executor);
    this.tenantName = tenant;
    this.streamName = stream;
    this.metricsRecorder = recorder;
    variables = new HashMap<>();
    // startPreprocess();
    this.dataEngine = dataEngine;
  }

  /**
   * Constructor to make an IngestState object with event.
   *
   * @param event Event to handle.
   * @param recorder Metrics recorder. Set null to disable metrics collection.
   */
  public IngestState(
      Event event,
      OperationMetricGroupRecorder recorder,
      String tenant,
      String stream,
      Executor executor,
      DataEngine dataEngine) {
    super(EventExecution.INGEST.name(), executor);
    this.tenantName = tenant;
    this.streamName = stream;
    this.metricsRecorder = recorder;
    this.event = event;
    variables = new HashMap<>();
    this.dataEngine = dataEngine;
  }

  public IngestRequest getInput() {
    return input;
  }

  public void setInput(IngestRequest input) {
    this.input = input;
  }

  public Event getEvent() {
    return event;
  }

  public void setEvent(Event event) {
    this.event = event;
  }

  public Map<String, Object> getVariables() {
    return variables;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("tenant: ").append(tenantName).append("\n");
    sb.append("stream: ").append(streamName).append("\n");
    if (input != null) {
      sb.append("eventId: ").append(input.getEventId()).append("\n");
      sb.append("eventText: ").append(input.getEventText()).append("\n");
    } else {
      sb.append("eventId: null\n").append("input: null\n");
    }
    sb.append("event: ").append(event).append("\n");
    return sb.toString();
  }
}
