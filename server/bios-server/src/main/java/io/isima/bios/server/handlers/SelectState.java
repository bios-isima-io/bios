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
import io.isima.bios.data.QueryLogger;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.SelectResponseRecords;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.recorder.SignalRequestType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Execution state for select operation. */
@Getter
@ToString
public class SelectState extends ExecutionState {

  // Initial immutable parameters //////////////////////////////
  // private final SelectRequest request;
  private final SelectStatement query;
  private final int index;
  private final BiosVersion clientVersion;

  // Parameters built up during the operation //////////////////
  private final DerivedQueryComponents derived;

  /** Response records. */
  private final List<SelectResponseRecords> responses;

  @Getter @Setter protected Optional<QueryLogger.Item> queryLoggerItem;

  /**
   * The constructor.
   *
   * @param executionName Execution name
   * @param tenantName Name of the target tenant
   * @param query The select query
   * @param parent Parent state
   */
  public SelectState(
      String executionName,
      String tenantName,
      SelectStatement query,
      int index,
      BiosVersion clientVersion,
      ExecutionState parent) {
    super(executionName, parent);
    this.tenantName = tenantName;
    this.query = query;
    this.streamName = query.getFrom();
    this.index = index;
    this.clientVersion = clientVersion;
    this.derived = new DerivedQueryComponents();
    this.responses = new ArrayList<>();
    this.queryLoggerItem = Optional.empty();
    this.requestType = SignalRequestType.SELECT;
    logContext.put(String.format("statement[%d]", index), query);
  }

  /**
   * Picks up the first registered select response records.
   *
   * @throws IllegalStateException thrown to indicate that no response is registered yet when the
   *     method is called
   */
  public SelectResponseRecords getResponse() {
    if (responses.isEmpty()) {
      throw new IllegalStateException("Tried to fetch a response while no ones are registered");
    }
    return responses.get(0);
  }

  /** Registers a set of select response records. */
  public void addResponse(SelectResponseRecords response) {
    responses.add(response);
  }

  public StreamDesc getSignalDesc() {
    return streamDesc;
  }
}
