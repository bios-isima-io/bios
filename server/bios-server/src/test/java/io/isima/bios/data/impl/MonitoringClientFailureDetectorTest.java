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
package io.isima.bios.data.impl;

import io.isima.bios.common.MonitoringSystemConfigs;
import io.isima.bios.data.impl.maintenance.MonitoringClientFailureDetector;
import io.isima.bios.vigilantt.notifiers.WebhookNotifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringClientFailureDetectorTest {
  private Map<String, Long> dataReceivedTimestampMap;
  private WebhookNotifier webhookNotifier =
      new WebhookNotifier(MonitoringSystemConfigs.getMonitoringNotificationUrl());
  private MonitoringClientFailureDetector detector;

  @Before
  public void setUp() {
    dataReceivedTimestampMap = new HashMap<>();
    detector = new MonitoringClientFailureDetector(dataReceivedTimestampMap, webhookNotifier);
  }

  @After
  public void tearDown() {}

  @Test
  public void testFailingMachineIsIdentifiedCorrectly() throws Exception {
    String failingMachine = "failingMachine";
    long tenMinutesAgo = System.currentTimeMillis() - 600000;
    dataReceivedTimestampMap.put(failingMachine, tenMinutesAgo);

    List<String> failingMachinesIdentified = detector.getFailingClientMachines();
    Assert.assertNotNull(failingMachinesIdentified);
    Assert.assertTrue(failingMachinesIdentified.contains(failingMachine));
  }

  @Test
  public void testNormalMachineIsIdentifiedCorrectly() throws Exception {
    String normalMachine = "normalMachine";
    long twoMinutesAgo = System.currentTimeMillis() - 120000;
    dataReceivedTimestampMap.put(normalMachine, twoMinutesAgo);

    List<String> failingMachinesIdentified = detector.getFailingClientMachines();
    Assert.assertNotNull(failingMachinesIdentified);
    Assert.assertFalse(failingMachinesIdentified.contains(normalMachine));
  }
}
