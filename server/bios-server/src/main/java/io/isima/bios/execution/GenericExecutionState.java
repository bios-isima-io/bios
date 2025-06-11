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

import java.util.concurrent.Executor;

// TODO(BIOS-4397): Consolidate this class into ExecutionState
public class GenericExecutionState extends ExecutionState {

  public GenericExecutionState(String executionName, Executor executor) {
    super(executionName, executor);
  }

  public GenericExecutionState(String executionName, ExecutionState parent) {
    super(executionName, parent);
  }

  public GenericExecutionState(String executionName, ExecutionState parent, Executor executor) {
    super(executionName, parent, executor);
  }
}
