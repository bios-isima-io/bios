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

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.exceptions.NotificationFailedException;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringClientFailureDetector implements Runnable {
  private static final Logger logger =
      LoggerFactory.getLogger(MonitoringClientFailureDetector.class);
  private final Map<String, Long> dataReceivedTimestampMap;
  private final WebhookNotifier webhookNotifier;
  List<String> violatedRules = Collections.singletonList("Vigilantt client failed");
  private final int numRetries = MonitoringSystemConfigs.getMonitoringNotificationRetryCount();
  private final Boolean isMonitoringNotificationEnabled =
      MonitoringSystemConfigs.getMonitoringNotificationsEnabled();
  private final long fiveMinutesAsMillis = 300000;

  public MonitoringClientFailureDetector(
      Map<String, Long> dataReceivedTimestampMap, WebhookNotifier notifier) {
    this.dataReceivedTimestampMap = dataReceivedTimestampMap;
    this.webhookNotifier = notifier;
  }

  @Override
  public void run() {
    logger.trace("Starting monitoring client failure detector");
    List<String> failingClientMachines = getFailingClientMachines();
    for (String key : failingClientMachines) {
      try {
        logger.trace("Vigilantt client on machine {} has stopped", key);
        if (isMonitoringNotificationEnabled) {
          Map<String, Object> attributes = new HashMap<>();
          attributes.put("machineId", key);
          Event event = new EventJson();
          event.setAttributes(attributes);
          NotificationContents notificationContents = new NotificationContents(event);
          webhookNotifier.sendNotification(notificationContents, "Client Failure", numRetries);
        }
      } catch (NotificationFailedException e) {
        logger.trace("Monitoring failure notification failed");
      }
    }
  }

  public List<String> getFailingClientMachines() {
    List<String> result = new ArrayList<>();
    for (String key : dataReceivedTimestampMap.keySet()) {
      if (System.currentTimeMillis() - dataReceivedTimestampMap.get(key) > fiveMinutesAsMillis) {
        result.add(key);
      }
    }
    return result;
  }
}
