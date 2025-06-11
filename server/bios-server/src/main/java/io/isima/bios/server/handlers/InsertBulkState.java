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
import io.isima.bios.data.DefaultRecord;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.InsertBulkEachResult;
import io.isima.bios.models.proto.DataProto.InsertBulkRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import lombok.Getter;

public class InsertBulkState extends ExecutionState {
  // initial parameters
  @Getter private InsertBulkRequest request; // erasable, can be large

  // parameters filled during the operation
  private List<DefaultRecord> records;
  private boolean hasPartialError;

  // results, the order of entry must be aligned with records.
  @Getter private final List<InsertBulkEachResult> results;

  public InsertBulkState(Executor executor) {
    super("InsertBulk", executor);
    this.hasPartialError = false;
    this.results = new ArrayList<>();
  }

  public InsertBulkState(String tenantName, InsertBulkRequest request, ExecutionState parent) {
    super("InsertBulk", parent);
    this.hasPartialError = false;
    this.results = new ArrayList<>();
    setInitialParams(tenantName, request);
  }

  public InsertBulkState setSignalDesc(StreamDesc signalDesc) {
    addHistory("(setSignal");
    if (this.streamDesc != null) {
      throw new IllegalStateException("signal is set already");
    }
    Objects.requireNonNull(signalDesc);
    this.streamDesc = signalDesc;
    addHistory(")");
    return this;
  }

  public void setInitialParams(String tenantName, InsertBulkRequest request) {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(request);
    this.tenantName = tenantName;
    this.request = request;
  }

  public InsertBulkState setRecords(List<DefaultRecord> records) {
    addHistory("(setRecord");
    if (this.records != null) {
      throw new IllegalStateException("record is set already");
    }
    this.records = records;
    addHistory(")");
    return this;
  }

  public StreamDesc getSignalDesc() {
    if (streamDesc == null) {
      throw new IllegalStateException("signalDesc is not set");
    }
    return streamDesc;
  }

  public List<DefaultRecord> getRecords() {
    if (records == null) {
      throw new IllegalStateException("record must be set");
    }
    return records;
  }

  public void addResult(InsertBulkEachResult result) {
    results.add(result);
  }

  public void markPartialError() {
    hasPartialError = true;
  }

  public boolean partialErrorHappened() {
    return hasPartialError;
  }
}
