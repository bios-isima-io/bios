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

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.EventExecution;
import io.isima.bios.models.View;
import io.isima.bios.recorder.OperationMetricsRecorder;
import io.isima.bios.req.ExtractRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Class to carry extract process state. Service request handler should create this object as per
 * receiving an extract request.
 */
@Getter
@Setter
public class ExtractState extends QueryExecutionState {
  // State variables ///////////////////////////////////////////////
  /** The ingest request object. */
  private ExtractRequest input;

  /** Map used for storing intermediate state. */
  @Setter(AccessLevel.NONE)
  private final Map<String, Object> variables;

  private boolean indexQueryEnabled;

  private boolean rollupTask;

  /** Special views applied specific to the type of extraction before specified views. */
  private List<View> initialViews = List.of();

  // End state variables ///////////////////////////////////////////////

  @Setter(AccessLevel.NONE)
  private final Supplier<EventFactory> eventFactorySupplier;

  /**
   * Constructor to make an ExtractState object.
   *
   * @param recorder Metrics collector. Set null to disable metrics collection.
   * @param tenant Tenant for which extract should happen
   * @param stream Stream for which extract should happen
   * @param eventFactory provider of event objects
   */
  @Deprecated
  public ExtractState(
      OperationMetricsRecorder recorder,
      String tenant,
      String stream,
      EventFactory eventFactory,
      Executor executor) {
    super(EventExecution.EXTRACT, recorder, tenant, stream, eventFactory, executor);
    variables = new HashMap<>();
    indexQueryEnabled = false;
    rollupTask = false;
    eventFactorySupplier = () -> eventFactory;
  }

  public ExtractState(
      String executionName,
      ExecutionState parentState,
      Supplier<EventFactory> eventFactorySupplier) {
    super(executionName, EventExecution.EXTRACT, eventFactorySupplier.get(), parentState);
    variables = new HashMap<>();
    indexQueryEnabled = false;
    rollupTask = false;
    this.eventFactorySupplier = eventFactorySupplier;
  }

  /**
   * Constructor to make an ExtractState object.
   *
   * @param recorder Metrics recorder. Set null to disable metrics collection.
   * @param tenant Tenant for which extract should happen
   * @param stream Stream for which extract should happen
   */
  @Deprecated
  public ExtractState(
      OperationMetricsRecorder recorder, String tenant, String stream, Executor executor) {
    this(recorder, tenant, stream, new EventFactory() {}, executor);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("tenant: ").append(tenantName).append("\n");
    sb.append("stream: ").append(streamName).append("\n");
    if (input != null) {
      sb.append("startTime: ").append(input.getStartTime()).append("\n");
      sb.append("endTime: ").append(input.getEndTime()).append("\n");
      sb.append("attributes: ").append(input.getAttributes()).append("\n");
    } else {
      sb.append("eventId: null\n").append("input: null\n");
    }
    sb.append("isRollupTask: ").append(rollupTask).append("\n");
    return sb.toString();
  }
}
