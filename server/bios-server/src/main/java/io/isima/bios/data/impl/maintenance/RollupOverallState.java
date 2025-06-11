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

public class RollupOverallState {
  public RollupOverallState(long start) {
    processedStreams = 0;
    totalEvents = 0;
    this.start = start;
  }

  long start;
  public int processedStreams;
  public int totalEvents;

  public synchronized void incrementProcessedStreams() {
    processedStreams += 1;
  }

  public synchronized void incrementTotalEvents(int increment) {
    totalEvents += increment;
  }
}
