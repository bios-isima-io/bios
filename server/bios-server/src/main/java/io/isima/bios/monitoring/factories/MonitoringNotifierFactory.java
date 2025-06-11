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

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.vigilantt.notifiers.Notifier;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class to create an instance of Notifier. */
public class MonitoringNotifierFactory {
  private static final Logger logger = LoggerFactory.getLogger(MonitoringNotifierFactory.class);

  public static Notifier getNotifier(NotifierType type) {
    if (type.equals(NotifierType.WEBHOOK)) {
      String webhookUrl = MonitoringSystemConfigs.getMonitoringNotificationUrl();
      logger.debug("Creating webhook notifier for webhook url {}", webhookUrl);
      return new WebhookNotifier(webhookUrl);
    } else if (type.equals(NotifierType.EMAIL)) {
      logger.debug("Creating email notifier");
      return new EmailNotifier();
    }
    logger.debug("Creating log notifier");
    return new LogNotifier();
  }
}
