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
package io.isima.bios.monitoring.notifiers;

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.vigilantt.models.NotificationContents;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringWebhookNotificationTest {
  private final String webhookPermanentUrl = MonitoringSystemConfigs.getMonitoringNotificationUrl();
  private final Integer notificationRetryCount =
      MonitoringSystemConfigs.getMonitoringNotificationRetryCount();
  private WebhookNotifier webhookNotifier;

  @Before
  public void setUp() {
    webhookNotifier = new WebhookNotifier(webhookPermanentUrl);
  }

  @After
  public void tearDown() {}

  @Test
  public void testNotificationNoExceptionWithValidUrl() throws Exception {
    NotificationContents notificationContents = constructSampleNotificationContents();
    webhookNotifier.sendNotification(notificationContents, "Test", notificationRetryCount);
    Thread.sleep(5000);
    Assert.assertTrue(true);
  }

  private NotificationContents constructSampleNotificationContents() {
    final String nuclearWeaponAttribute = "numNuclearWeapons";
    final Integer nuclearWeaponCount = 0;
    Event event = new EventJson();
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(nuclearWeaponAttribute, nuclearWeaponCount);
    event.setAttributes(attributes);
    NotificationContents notificationContents = new NotificationContents(event);
    notificationContents.setAlertName("nuclearWeaponRule");
    notificationContents.setSignalName("weapons");
    notificationContents.setFeatureName("weaponsFeature");
    notificationContents.setCondition("numNuclearWeapons > 1");
    return notificationContents;
  }
}
