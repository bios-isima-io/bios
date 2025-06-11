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
package io.isima.bios.data.impl.maintenance;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@RequiredArgsConstructor
@ToString
public class PostProcessContext implements Comparable {
  private final long requestTime;
  private final long processExecutionTime;
  private final PostProcessSpecifiers specs;
  private final List<DigestSpecifier> processedSpecs = new ArrayList<>();
  private final boolean countTasks;
  private final Executor executor;
  private final PostProcessMonitor monitor;
  // @Setter
  // private List<DigestSpecifier> handledSpecs = List.of();

  private long pickupTime;
  private long startTime;

  public PostProcessContext(
      long requestTime,
      PostProcessSpecifiers specs,
      boolean countTasks,
      Executor executor,
      PostProcessMonitor monitor) {
    this.requestTime = requestTime;
    this.processExecutionTime = specs.getProcessExecutionTime();
    this.specs = Objects.requireNonNull(specs);
    this.countTasks = countTasks;
    this.executor = Objects.requireNonNull(executor);
    this.monitor = Objects.requireNonNull(monitor);
  }

  public void pickedUp() {
    this.pickupTime = System.currentTimeMillis();
    monitor.taskPickedUp(this);
  }

  public void start() {
    this.startTime = System.currentTimeMillis();
  }

  public void done() {
    monitor.taskEnded(this);
  }

  public void doneExceptionally(Throwable t) {
    // TODO(Naoki): Implement this
  }

  /*
  public Context(long startTime, boolean countTasks, StreamDesc streamDesc) {
    this.startTime = startTime;
    this.countTasks = countTasks;
    this.streamDesc = streamDesc;
  }
  */

  @Override
  public int compareTo(Object o) {
    return Long.compare(processExecutionTime, ((PostProcessContext) o).processExecutionTime);
  }
}
