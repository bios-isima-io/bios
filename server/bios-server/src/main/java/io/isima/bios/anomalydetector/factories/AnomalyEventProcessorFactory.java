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
package io.isima.bios.anomalydetector.factories;

import io.isima.bios.anomalydetector.detectors.AnomalyDetector;
import io.isima.bios.anomalydetector.processor.AnomalyEventProcessor;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.monitoring.factories.MonitoringNotifierFactory;
import io.isima.bios.monitoring.factories.NotifierType;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;

/** Factory for anomaly detector class. */
public class AnomalyEventProcessorFactory {
  private static final int anomalyDetectionWindowLength =
      TfosConfig.getAnomalyDetectorWindowLength();
  private static final String anomalyNotificationUrl = TfosConfig.getAnomalyNotificationUrl();

  public static AnomalyEventProcessor getAnomalyDetector() {
    WebhookNotifier notifier =
        (WebhookNotifier) MonitoringNotifierFactory.getNotifier(NotifierType.WEBHOOK);
    notifier.setWebhookUrl(anomalyNotificationUrl);
    AnomalyDetector anomalyDetector = new AnomalyDetector(anomalyDetectionWindowLength);
    return new AnomalyEventProcessor(anomalyDetector, notifier);
  }
}
