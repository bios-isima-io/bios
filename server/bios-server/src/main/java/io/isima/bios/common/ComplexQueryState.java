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

import io.isima.bios.models.ComplexQueryRequest;
import io.isima.bios.models.EventExecution;
import io.isima.bios.recorder.OperationMetricsRecorder;
import io.isima.bios.server.handlers.SelectState;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;

/** Class to carry process state for queries that return multiple result sets. */
@Getter
@Setter
public class ComplexQueryState extends QueryExecutionState {
  private ComplexQueryRequest input;
  private final Supplier<EventFactory> eventFactorySupplier;

  public ComplexQueryState(
      OperationMetricsRecorder recorder,
      String tenant,
      String stream,
      Supplier<EventFactory> eventFactorySupplier,
      Executor executor) {
    super(EventExecution.COMPLEX_QUERY, recorder, tenant, stream, null, executor);
    this.eventFactorySupplier = Objects.requireNonNull(eventFactorySupplier);
  }

  public ComplexQueryState(SelectState state, Supplier<EventFactory> eventFactorySupplier) {
    this(
        state.getMetricsRecorder(),
        state.getTenantName(),
        state.getSignalDesc().getName(),
        eventFactorySupplier,
        state.getExecutor());
  }

  /**
   * Generates a new event factory.
   *
   * <p>The method makes a new factory to create a set of events that shares the same column
   * definitions.
   */
  public EventFactory generateEventFactory() {
    return eventFactorySupplier.get();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("tenant: ").append(tenantName).append("\n");
    sb.append("stream: ").append(streamName).append("\n");
    if (input != null) {
      sb.append("request: ").append(input.toString()).append("\n");
    } else {
      sb.append("request: null\n");
    }
    return sb.toString();
  }
}
