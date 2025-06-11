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

import java.util.ArrayList;
import java.util.List;

public class MetricSnapshotsInFile {
  private List<MetricsSnapshot> metricsSnapshots;

  public MetricSnapshotsInFile() {
    metricsSnapshots = new ArrayList<>();
  }

  public MetricSnapshotsInFile(List<MetricsSnapshot> metricsSnapshots) {
    this.metricsSnapshots = metricsSnapshots;
  }

  public List<MetricsSnapshot> getMetricsSnapshots() {
    return metricsSnapshots;
  }

  public void setMetricsSnapshots(List<MetricsSnapshot> metricsSnapshots) {
    this.metricsSnapshots = metricsSnapshots;
  }

  public void addMetricSnapshot(MetricsSnapshot metricsSnapshot) {
    this.metricsSnapshots.add(metricsSnapshot);
  }
}
