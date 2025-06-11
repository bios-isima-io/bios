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
package io.isima.bios.metrics;

public enum CassandraMetricsBeanAttribute {
  COUNT("Count"),
  VALUE("Value"),
  MIN("Min"),
  MAX("Max"),
  MEAN("Mean"),
  PROCESS_CPU_LOAD("ProcessCpuLoad"),
  MAX_FD_COUNT("MaxFileDescriptorCount"),
  OPEN_FD_COUNT("OpenFileDescriptorCount"),
  HEAP_MEM_USAGE("HeapMemoryUsage"),
  NON_HEAP_MEM_USAGE("NonHeapMemoryUsage"),
  TOTAL_PHYSICAL_MEMORY("TotalPhysicalMemorySize"),
  FREE_PHYSICAL_MEMORY("FreePhysicalMemorySize");

  private String beanAttr;

  CassandraMetricsBeanAttribute(String beanAttr) {
    this.setBeanAttr(beanAttr);
  }

  public String getBeanAttr() {
    return beanAttr;
  }

  private void setBeanAttr(String beanAttr) {
    this.beanAttr = beanAttr;
  }
}
