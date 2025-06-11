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
package io.isima.bios.monitoring.processors;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.monitoring.factories.MonitoringEventProcessorFactory;
import io.isima.bios.vigilantt.processors.EventProcessor;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MonitoringEventProcessorTest {
  private final EventProcessor monitoringEventProcessor =
      MonitoringEventProcessorFactory.getEventProcessor();
  private Event event;

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  /**
   * This test is for debugging purposes and will be disabled by default
   *
   * @throws Exception
   */
  @Test
  @Ignore
  public void testAnomalousEventNotification() throws Exception {
    event = constructSampleAnomalousEvent();
    monitoringEventProcessor.processEvent(event);
    Thread.sleep(5000);
  }

  /**
   * This test is for debugging purposes and will be disabled by default
   *
   * @throws Exception
   */
  @Test
  @Ignore
  public void testNonAnomalousEventNotification() throws Exception {
    event = constructSampleNonAnomalousEvent();
    monitoringEventProcessor.processEvent(event);
    Thread.sleep(5000);
  }

  private Event constructSampleAnomalousEvent() {
    Event newEvent = new EventJson();
    final String numOpenFileDescriptors = "open_fd_count";
    final String numMaxAllowedFileDescriptors = "max_fd_count";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(numOpenFileDescriptors, 90);
    attributes.put(numMaxAllowedFileDescriptors, 100);
    newEvent.setAttributes(attributes);
    return newEvent;
  }

  private Event constructSampleNonAnomalousEvent() {
    Event newEvent = new EventJson();
    final String numOpenFileDescriptors = "open_fd_count";
    final String numMaxAllowedFileDescriptors = "max_fd_count";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(numOpenFileDescriptors, 30);
    attributes.put(numMaxAllowedFileDescriptors, 100);
    newEvent.setAttributes(attributes);
    return newEvent;
  }
}
