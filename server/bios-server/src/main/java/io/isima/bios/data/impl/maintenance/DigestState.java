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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.execution.ExecutionState;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.Setter;

class DigestState extends ExecutionState {

  @Getter private final long totalStart;
  @Getter @Setter private boolean started;
  @Getter @Setter private long startTime;
  @Getter @Setter private long endTime;
  @Getter @Setter private long scheduleEnd;
  @Getter @Setter private long fetchEventsEnd;
  @Getter @Setter private long accumulatingCountEnd;
  @Getter @Setter private long indexEnd;
  @Getter @Setter private long rollupEnd;
  @Getter @Setter private long sketchEnd;
  @Getter @Setter private int indexedEvents;

  @Getter @Setter private boolean continueProcessing;

  @Getter private List<DigestSpecifier> processedSpecifiers = new ArrayList<>();

  /*
  @Getter
  @Setter
  PostProcessTicket ticket;
  */

  public DigestState(String executionName, StreamDesc stream, long start, Executor executor) {
    super(executionName, executor);
    this.streamDesc = stream;
    totalStart = start;
    started = false;
    indexEnd = 0;
    rollupEnd = 0;
    sketchEnd = 0;
    indexedEvents = 0;
  }
}
