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
package io.isima.bios.models;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollupTest {

  private final TimeInterval timeInterval = new TimeInterval(5, TimeunitType.MINUTE);
  private final String rollupName = "test";
  private final Rollup firstRollup = new Rollup();
  private final Rollup secondRollup = new Rollup();
  private List<AlertConfig> firstList;
  private List<AlertConfig> secondList;
  private AlertConfig firstAlert = new AlertConfig();
  private AlertConfig secondAlert = new AlertConfig();

  @Before
  public void setUp() {
    firstRollup.setHorizon(timeInterval);
    firstRollup.setInterval(timeInterval);
    firstRollup.setName(rollupName);
    secondRollup.setHorizon(timeInterval);
    secondRollup.setInterval(timeInterval);
    secondRollup.setName(rollupName);
    firstAlert.setName("testAlert");
    firstAlert.setCondition("test_condition");
    firstAlert.setWebhookUrl("test_webhook");
    secondAlert.setName("testAlertSecond");
    secondAlert.setCondition("test_condition_second");
    secondAlert.setWebhookUrl("test_webhook_second");

    firstList = new ArrayList<>();
    secondList = new ArrayList<>();
  }

  @After
  public void tearDown() {}

  @Test
  public void testEqualAlertList() throws Exception {
    firstList.add(firstAlert);
    secondList.add(firstAlert);
    firstRollup.setAlerts(firstList);
    secondRollup.setAlerts(secondList);
    Assert.assertEquals(firstRollup, secondRollup);
  }

  @Test
  public void testDifferentAlertList() throws Exception {
    firstList.add(firstAlert);
    secondList.add(secondAlert);
    firstRollup.setAlerts(firstList);
    secondRollup.setAlerts(secondList);
    Assert.assertNotEquals(firstRollup, secondRollup);
  }
}
