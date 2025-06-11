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
package io.isima.bios.monitoring.factories;

import io.isima.bios.monitoring.processor.MonitoringEventProcessor;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import io.isima.bios.vigilantt.notifiers.Notifier;
import io.isima.bios.vigilantt.processors.EventProcessor;

/** Factory class to prepare and create an instance of MonitoringEventProcessor. */
public class MonitoringEventProcessorFactory {

  public static EventProcessor getEventProcessor() {
    EventClassifier eventClassifier = MonitoringEventClassifierFactory.getClassifier();
    Notifier notifier = MonitoringNotifierFactory.getNotifier(NotifierType.WEBHOOK);
    return new MonitoringEventProcessor(eventClassifier, notifier);
  }
}
