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

/** POJO to get insight metrics. */
public class InsightMetrics {

  private String name = "";
  private Integer count = 0;
  private Double min = 0D;
  private Double max = 0D;
  private Double mean = 0D;
  private Double stdev = 0D;
  private Double variance = 0D;

  public InsightMetrics(
      String name,
      Integer count,
      Double min,
      Double max,
      Double mean,
      Double stdev,
      Double variance) {
    this.name = name;
    this.count = count;
    this.min = min;
    this.max = max;
    this.mean = mean;
    this.stdev = stdev;
    this.variance = variance;
  }

  public String getName() {
    return name;
  }

  public Integer getCount() {
    return count;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  public Double getMean() {
    return mean;
  }

  public Double getStdev() {
    return stdev;
  }

  public Double getVariance() {
    return variance;
  }
}
