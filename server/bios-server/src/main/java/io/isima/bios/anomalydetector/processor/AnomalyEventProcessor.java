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
package io.isima.bios.anomalydetector.processor;

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.models.v1.AlertNotification;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver class for running anomaly detection on an event. Encapsulates the anomaly detection
 * implementation and notification
 */
public class AnomalyEventProcessor {
  private static final Logger logger = LoggerFactory.getLogger(AnomalyEventProcessor.class);
  private final EventClassifier anomalyDetector;
  private final WebhookNotifier anomalyNotifier;
  private final Integer notificationRetryCount =
      MonitoringSystemConfigs.getMonitoringNotificationRetryCount();
  private final String anomalyAlgortihm = "Rolling Window average";
  private final Boolean isAnomalyNotificationEnabled =
      MonitoringSystemConfigs.getAnomalyNotificationsEnabled();

  public AnomalyEventProcessor(EventClassifier eventClassifier, WebhookNotifier anomalyNotifier) {
    this.anomalyDetector = eventClassifier;
    this.anomalyNotifier = anomalyNotifier;
  }

  public void processEvent(AlertNotification alertNotification) {
    logger.debug("Anomaly detector processing request {}", alertNotification);
    if (anomalyDetector.classify(alertNotification).getIsAnomaly()) {
      if (isAnomalyNotificationEnabled) {
        try {
          NotificationContents notificationContents =
              new NotificationContents(alertNotification.getEvent());
          anomalyNotifier.setWebhookUrl(alertNotification.getAlert().getWebhookUrl());
          anomalyNotifier.sendNotification(notificationContents, "Anomaly", notificationRetryCount);
        } catch (NotificationFailedException e) {
          logger.debug("Anomaly notification failed {}", e.getMessage());
        }
      }
    }
  }
}
