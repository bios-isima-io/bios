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

import java.util.List;

public class MetricsSnapshot {
  private long timestamp;
  private String node;
  private String version;
  private List<MetricRecord> metricRecords;

  public MetricsSnapshot() {}
  ;

  public MetricsSnapshot(
      long timestamp, String node, String version, List<MetricRecord> metricRecords) {
    this.timestamp = timestamp;
    this.node = node;
    this.version = version;
    this.metricRecords = metricRecords;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getNode() {
    return node;
  }

  public String getVersion() {
    return version;
  }

  public List<MetricRecord> getMetricRecords() {
    return metricRecords;
  }
}
