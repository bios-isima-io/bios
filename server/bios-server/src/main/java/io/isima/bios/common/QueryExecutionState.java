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

import io.isima.bios.data.QueryLogger;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.EventExecution;
import io.isima.bios.query.CompiledSortRequest;
import io.isima.bios.recorder.OperationMetricsRecorder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.Setter;
import org.apache.cassandra.cql3.SingleColumnRelation;

public abstract class QueryExecutionState extends ExecutionState {

  @Getter private final EventExecution executionType;

  @Getter private final EventFactory eventFactory;

  /**
   * Filter as a list of CQL relation objects. This is an intermidate form of a filter obtained by
   * parsing the source string. Used by extract and summarize APIs.
   *
   * <p>The type of the list members must be org.apache.cassandra.cql3.Relation, but we use Object
   * here to avoid dependency on Cassandra propagating to V2 components (V2 uses TFOS AdminImpl for
   * tentative BiosServer AdminInternal implementation, that ends up referring to this class.)
   */
  @Getter @Setter private List<SingleColumnRelation> filter;

  @Getter @Setter private boolean validated = false;

  /** Logger item for the query. */
  @Getter protected Optional<QueryLogger.Item> queryLoggerItem;

  /**
   * Compiled sort key, it can be a plain attribute or an aggregate.
   *
   * <p>If the key is a plain attribute, the aggregate function is null.
   */
  @Getter @Setter protected CompiledSortRequest compiledSortRequest;

  /**
   * Whether to limit number of data points to retrieve. Limitation is enabled for regular extract
   * operations. But a feature builder wants to unlimit number of data points, for example.
   */
  @Getter @Setter protected boolean dataLengthLimited = true;

  public QueryExecutionState(
      EventExecution executionType,
      OperationMetricsRecorder recorder,
      String tenant,
      String stream,
      EventFactory eventFactory,
      Executor executor) {
    super(executionType.name(), executor);
    this.executionType = executionType;
    this.tenantName = tenant;
    this.streamName = stream;
    this.metricsRecorder = recorder;
    this.eventFactory = eventFactory;
    queryLoggerItem = Optional.empty();
  }

  public QueryExecutionState(
      String executionName,
      EventExecution executionType,
      EventFactory eventFactory,
      ExecutionState parentState) {
    super(executionName, parentState);
    this.tenantName = parentState.getTenantName();
    this.streamName = parentState.getStreamName();
    this.executionType = executionType;
    this.eventFactory = eventFactory;
    queryLoggerItem = Optional.empty();
  }

  public void setQueryLoggerItem(QueryLogger.Item item) {
    this.queryLoggerItem = Optional.of(item);
  }
}
