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
package io.isima.bios.vigilantt.processors;

import io.isima.bios.models.Event;

/**
 * Interface to process the incoming event Implementation - MonitoringEventProcessor,
 * RollUpSignalEventProcessor, RawSignalEventProcessor
 */
public interface EventProcessor {

  /** Process an incoming event. */
  void processEvent(Event event);
}
