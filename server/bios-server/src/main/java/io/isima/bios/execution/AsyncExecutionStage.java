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
package io.isima.bios.execution;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import lombok.ToString;

@ToString(callSuper = true)
public abstract class AsyncExecutionStage<StateT extends ExecutionState> {

  protected final StateT state;
  protected ConcurrentExecutionController executionController;

  protected AsyncExecutionStage(StateT state) {
    this.state = state;
  }

  void setController(ConcurrentExecutionController controller) {
    this.executionController = controller;
  }

  public StateT getState() {
    return state;
  }

  public void addHistory(String stageName) {
    state.addHistory(stageName);
  }

  public void done() {
    state.markDone();
  }

  public Executor getExecutor() {
    return state.getExecutor();
  }

  public abstract CompletionStage<Void> runAsync();
}
