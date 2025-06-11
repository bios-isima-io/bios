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

import io.isima.bios.monitoring.classifier.CompiledRule;
import io.isima.bios.monitoring.classifier.MonitoringEventClassifier;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringEventClassifierFactoryTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testMonitoringEventClassifierObjectCreation() throws Exception {
    EventClassifier eventClassifier = MonitoringEventClassifierFactory.getClassifier();
    Assert.assertNotNull(eventClassifier);
    Assert.assertTrue(eventClassifier instanceof MonitoringEventClassifier);

    MonitoringEventClassifier monitoringEventClassifier =
        (MonitoringEventClassifier) eventClassifier;
    List<CompiledRule> compiledRuleList = monitoringEventClassifier.getCompiledRuleList();
    Assert.assertNotNull(compiledRuleList);
    Assert.assertEquals(8, compiledRuleList.size());
  }
}
