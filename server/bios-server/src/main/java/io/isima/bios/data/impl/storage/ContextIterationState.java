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
package io.isima.bios.data.impl.storage;

import io.isima.bios.execution.ExecutionState;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/** Execution state that helps to iterate context entries asynchronously. */
@Getter
@Accessors(chain = true)
public class ContextIterationState extends ExecutionState {

  private AtomicLong rowCounter;
  private AtomicLong entryCounter;

  // consumes a non-deleted primary key when retrieving an entry from the storage
  @Setter private Consumer<List<Object>> primaryKeyConsumer;

  // consumes every entry in the storage
  @Setter private ContextEntryConsumer entryConsumer;

  // executes at the end of each batch, consuming the elapsed time for the batch
  @Setter private Function<Long, CompletionStage<Void>> batchPostProcess;

  // Optionally sets timestamp to retrieve entries with an explicit snapshot time
  @Setter private Long timestamp;

  @Setter private long startTime;

  // Used for building up events using ObjectListEventValue
  @Setter private Map<String, Integer> attributeNameToIndex;

  public ContextIterationState(String executionName, Executor executor) {
    super(executionName, executor);
  }

  public ContextIterationState(String executionName, ExecutionState parentState) {
    super(executionName, parentState);
  }

  /**
   * Enables storage row counter.
   *
   * @return self
   */
  public ContextIterationState enableRowCounter() {
    rowCounter = new AtomicLong(0);
    return this;
  }

  /**
   * Enables non-deleted entry counter.
   *
   * @return self
   */
  public ContextIterationState enableEntryCounter() {
    entryCounter = new AtomicLong(0);
    return this;
  }
}
