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

import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.data.impl.SketchesExtractor;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventExecution;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.recorder.OperationMetricGroupRecorder;
import io.isima.bios.recorder.OperationMetricsRecorder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.Setter;

/**
 * Class to carry extract process state. Service request handler should create this object as per
 * receiving an extract request.
 */
public class SummarizeState extends QueryExecutionState {
  // State variables ///////////////////////////////////////////////
  /** The ingest request object. */
  private SummarizeRequest input;

  /** Map used for storing intermediate state. */
  protected final Map<String, Object> variables;

  private boolean rollupTask;

  private Map<Long, List<Event>> output;

  /*
   * Indicates that the execution is invoked from the biOS API.
   */
  private boolean isBiosApi;

  /** Compiled filter used only for rollup query post filter. */
  private Map<String, List<FilterTerm>> compiledFilter;

  private Set<String> onlyForFilterAttributes;

  /** Compiled aggregates used by sketches. */
  @Getter @Setter private List<CompiledAggregate> compiledAggregates;

  /** Helpful for debugging. */
  @Setter private SketchesExtractor.Queries queries;

  // End state variables ///////////////////////////////////////////////

  /**
   * Constructor to make an SummarizeState object.
   *
   * @param recorder Metrics collector. Set null to disable metrics collection.
   * @param tenant Tenant for which extract should happen
   * @param stream Stream for which extract should happen
   * @param factory event factory
   * @param executor Executor to be used for the operation
   */
  public SummarizeState(
      OperationMetricsRecorder recorder,
      String tenant,
      String stream,
      EventFactory factory,
      Executor executor) {
    super(EventExecution.SUMMARIZE, recorder, tenant, stream, factory, executor);
    variables = new HashMap<>();
    rollupTask = false;
    isBiosApi = false;
    queryLoggerItem = Optional.empty();
  }

  public SummarizeState(
      String executionName,
      String tenant,
      String stream,
      EventFactory factory,
      ExecutionState parent) {
    super(executionName, EventExecution.SUMMARIZE, factory, parent);
    this.tenantName = tenant;
    this.streamName = stream;
    variables = new HashMap<>();
    rollupTask = false;
    isBiosApi = false;
    queryLoggerItem = Optional.empty();
  }

  /**
   * Constructor to make an SummarizeState object.
   *
   * @param recorder Metrics collector. Set null to disable metrics collection.
   * @param tenant Tenant for which extract should happen
   * @param stream Stream for which extract should happen
   */
  public SummarizeState(
      OperationMetricGroupRecorder recorder, String tenant, String stream, Executor executor) {
    this(recorder, tenant, stream, new EventFactory() {}, executor);
    queryLoggerItem = Optional.empty();
  }

  public SummarizeState(
      String executionName, EventFactory eventFactory, ExecutionState parentState) {
    super(executionName, EventExecution.SUMMARIZE, eventFactory, parentState);
    variables = new HashMap<>();
    rollupTask = false;
    isBiosApi = false;
    queryLoggerItem = Optional.empty();
  }

  public SummarizeRequest getInput() {
    return input;
  }

  public void setInput(SummarizeRequest input) {
    this.input = input;
  }

  public Map<String, Object> getVariables() {
    return variables;
  }

  public boolean isRollupTask() {
    return rollupTask;
  }

  public void setRollupTask(boolean convertOutput) {
    this.rollupTask = convertOutput;
  }

  public void setOutput(Map<Long, List<Event>> output) {
    this.output = output;
  }

  public Map<Long, List<Event>> getOutput() {
    return output;
  }

  public void setBiosApi() {
    isBiosApi = true;
  }

  public boolean isBiosApi() {
    return this.isBiosApi;
  }

  public void setCompiledFilter(Map<String, List<FilterTerm>> interpretedFilter) {
    this.compiledFilter = interpretedFilter;
  }

  public Map<String, List<FilterTerm>> getCompiledFilter() {
    return compiledFilter;
  }

  public void setOnlyForFilterAttributes(Set<String> attributes) {
    this.onlyForFilterAttributes = attributes;
  }

  public Set<String> getOnlyForFilterAttributes() {
    return onlyForFilterAttributes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("tenant: ").append(tenantName).append("\n");
    sb.append("stream: ").append(streamName).append("\n");
    if (input != null) {
      sb.append("request: ").append(input).append("\n");
    } else {
      sb.append("request: null\n");
    }
    sb.append("isRollupTask: ").append(rollupTask).append("\n");
    return sb.toString();
  }
}
