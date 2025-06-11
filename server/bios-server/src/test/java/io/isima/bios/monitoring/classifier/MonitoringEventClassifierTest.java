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
package io.isima.bios.monitoring.classifier;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.monitoring.factories.MonitoringEventClassifierFactory;
import io.isima.bios.vigilantt.classifiers.ClassificationInfo;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MonitoringEventClassifierTest {
  private final EventClassifier eventClassifier = MonitoringEventClassifierFactory.getClassifier();
  private final Event event = new EventJson();

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testCpuLoadRule() throws Exception {
    final String cpuLoad = "cpu_load";
    final String numCores = "num_cores";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(cpuLoad, 3);
    attributes.put(numCores, 1);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(cpuLoad, 1.5);
    attributes.put(numCores, 1);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testHighJvmMemoryUsageRule() throws Exception {
    final String jvmMemoryUsed = "jvm_used_memory";
    final String jvmTotalMemory = "jvm_total_memory";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(jvmMemoryUsed, 90);
    attributes.put(jvmTotalMemory, 100);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(jvmMemoryUsed, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testHighJvmHeapMemoryUsageRule() throws Exception {
    final String jvmHeapMemoryUsed = "heap_used_memory";
    final String jvmHeapTotalMemory = "heap_total_memory";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(jvmHeapMemoryUsed, 90);
    attributes.put(jvmHeapTotalMemory, 100);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(jvmHeapMemoryUsed, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testRootDiskUsageRule() throws Exception {
    final String rootDiskUsage = "root_disk_usage";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(rootDiskUsage, 90);
    event.setAttributes(attributes);

    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(rootDiskUsage, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testDataDiskUsageRule() throws Exception {
    final String dataDiskUsage = "data_disk_usage";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(dataDiskUsage, 90);
    event.setAttributes(attributes);

    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(dataDiskUsage, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testDbHeapMemoryUsageRule() throws Exception {
    final String dbHeapMemoryUsed = "db_memory_usage";
    final String dbHeapMemoryCommitted = "db_memory";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(dbHeapMemoryUsed, 90);
    attributes.put(dbHeapMemoryCommitted, 100);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(dbHeapMemoryUsed, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testDbOsMemoryUsageRule() throws Exception {
    final String dbOsMemoryUsed = "db_physical_memory_used";
    final String dbOsMemory = "db_physical_memory";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(dbOsMemoryUsed, 90);
    attributes.put(dbOsMemory, 100);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(dbOsMemoryUsed, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }

  @Test
  public void testOpenFileDescriptorRule() throws Exception {
    final String numOpenFileDescriptors = "open_fd_count";
    final String numMaxAllowedFileDescriptors = "max_fd_count";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(numOpenFileDescriptors, 90);
    attributes.put(numMaxAllowedFileDescriptors, 100);
    event.setAttributes(attributes);
    ClassificationInfo classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertTrue(classificationInfo.getIsAnomaly());

    attributes.put(numOpenFileDescriptors, 40);
    classificationInfo = eventClassifier.classify(event);
    Assert.assertNotNull(classificationInfo);
    Assert.assertFalse(classificationInfo.getIsAnomaly());
  }
}
