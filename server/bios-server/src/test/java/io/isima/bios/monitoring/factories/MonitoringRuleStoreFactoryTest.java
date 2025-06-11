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

import io.isima.bios.monitoring.store.MonitoringRuleStore;
import io.isima.bios.vigilantt.models.Rule;
import io.isima.bios.vigilantt.store.InMemoryRuleStore;
import io.isima.bios.vigilantt.store.RuleStore;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringRuleStoreFactoryTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testMonitoringRuleStoreObjectCreation() throws Exception {
    RuleStore ruleStore = MonitoringRuleStoreFactory.getRuleStore(StoreType.MONITORING);
    Assert.assertNotNull(ruleStore);
    Assert.assertTrue(ruleStore instanceof MonitoringRuleStore);
    List<Rule> ruleList = ruleStore.getAllRules();
    Assert.assertNotNull(ruleList);
    Assert.assertEquals(8, ruleList.size());
  }

  @Test
  public void testInMemoryRuleStoreObjectCreation() throws Exception {
    RuleStore ruleStore = MonitoringRuleStoreFactory.getRuleStore(StoreType.INMEMORY);
    Assert.assertNotNull(ruleStore);
    Assert.assertTrue(ruleStore instanceof InMemoryRuleStore);
  }
}
