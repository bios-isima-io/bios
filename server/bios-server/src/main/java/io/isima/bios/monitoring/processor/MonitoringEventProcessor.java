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
package io.isima.bios.monitoring.processor;

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.models.Event;
import io.isima.bios.vigilantt.classifiers.ClassificationInfo;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.Notifier;
import io.isima.bios.vigilantt.processors.EventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of EventProcessor for monitoring events. */
public class MonitoringEventProcessor implements EventProcessor {
  private final Logger logger = LoggerFactory.getLogger(MonitoringEventProcessor.class);
  private final EventClassifier eventClassifier;
  private final Notifier notifier;
  private final Integer notificationRetryCount =
      MonitoringSystemConfigs.getMonitoringNotificationRetryCount();
  private final Boolean isMonitoringNotificationEnabled =
      MonitoringSystemConfigs.getMonitoringNotificationsEnabled();

  public MonitoringEventProcessor(EventClassifier eventClassifier, Notifier notifier) {
    this.eventClassifier = eventClassifier;
    this.notifier = notifier;
  }

  @Override
  public void processEvent(Event event) {
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    if (classificationInfo.getIsAnomaly()) {
      try {
        logger.trace("Event {} is an anomaly", event);
        if (isMonitoringNotificationEnabled) {
          NotificationContents notificationContents = new NotificationContents(event);
          logger.trace("Sending notification {}", notificationContents);
          notifier.sendNotification(
              notificationContents, "Monitoring Event", notificationRetryCount);
        }
      } catch (NotificationFailedException e) {
        logger.trace("Notification for event {} failed with exception {}", event, e);
      }
    }
  }
}
