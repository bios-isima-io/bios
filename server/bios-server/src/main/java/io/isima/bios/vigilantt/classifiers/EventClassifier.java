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
package io.isima.bios.vigilantt.classifiers;

import io.isima.bios.models.Event;

/**
 * Interface to classify an incoming event as anomaly. Exposes a single method to determine whether
 * given event is anomaly Returned object ClassificationInfo contains details of the rules which
 * were violated and event information Implementations - RollingWindowAnomalyDetector,
 * MonitoringDetector
 */
public interface EventClassifier {

  /** Classify whether given event is anomaly. */
  ClassificationInfo classify(Event event);
}
