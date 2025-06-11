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
package io.isima.bios.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.isima.bios.models.EventExecution;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SummarizeStateTest {

  private static String previousConf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    previousConf = System.getProperty(TfosConfig.METRICS_ENABLED);
    System.setProperty(TfosConfig.METRICS_ENABLED, "false");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (previousConf != null) {
      System.setProperty(TfosConfig.METRICS_ENABLED, previousConf);
    } else {
      System.clearProperty(TfosConfig.METRICS_ENABLED);
    }
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testConstructor() {
    SummarizeState state = new SummarizeState(null, "dummyTenant", "dummyStream", null);

    assertEquals(EventExecution.SUMMARIZE, state.getExecutionType());
    assertEquals("dummyTenant", state.getTenantName());
    assertEquals("dummyStream", state.getStreamName());
    assertNull(state.getMetricsRecorder());
    assertNotNull(state.variables);
  }
}
