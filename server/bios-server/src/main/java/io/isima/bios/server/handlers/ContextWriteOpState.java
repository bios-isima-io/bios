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
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.models.RequestPhase;
import java.util.Objects;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ContextWriteOpState<RequestT, InputDataT> extends ContextOpState {
  // initial parameters
  @Getter @Setter protected Long contextVersion;
  @Getter protected RequestT request;

  @Getter
  @Setter
  @Accessors(chain = true)
  protected RequestPhase phase;

  @Getter protected Long timestamp;

  // parameters filled during the operation
  @Setter protected InputDataT inputData;

  public ContextWriteOpState(String operationName, Executor executor) {
    super(operationName, executor);
  }

  public void setInitialParams(
      String tenantName,
      String contextName,
      Long contextVersion,
      RequestT request,
      RequestPhase phase,
      Long timestamp) {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(contextName);
    Objects.requireNonNull(request);
    this.tenantName = tenantName;
    this.streamName = contextName;
    this.contextVersion = contextVersion;
    this.request = request;
    this.phase = phase;
    this.timestamp = timestamp;
  }

  @Override
  public void setContextDesc(StreamDesc contextDesc) {
    addHistory("(setSignal");
    if (this.streamDesc != null) {
      throw new IllegalStateException("signal is set already");
    }
    Objects.requireNonNull(contextDesc);
    this.streamDesc = contextDesc;
    addHistory(")");
  }

  public InputDataT getInputData(boolean verify) {
    if (inputData == null && verify) {
      throw new IllegalStateException("record must be set");
    }
    return inputData;
  }
}
