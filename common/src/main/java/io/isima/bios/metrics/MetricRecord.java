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

public class MetricRecord {
  private String operationName;
  private String subOperationName;
  private String tenant;
  private String stream;
  private String streamType;
  private int count;
  private long min;
  private long max;
  private long sum;

  public MetricRecord() {}
  ;

  public MetricRecord(
      String operationName,
      String subOperationName,
      String tenant,
      String stream,
      String streamType,
      int count,
      long min,
      long max,
      long sum) {
    this.operationName = operationName;
    this.subOperationName = subOperationName;
    this.tenant = tenant;
    this.stream = stream;
    this.streamType = streamType;
    this.count = count;
    this.min = min;
    this.max = max;
    this.sum = sum;
  }

  public String getOperationName() {
    return operationName;
  }

  public String getSubOperationName() {
    return subOperationName;
  }

  public String getTenant() {
    return tenant;
  }

  public String getStream() {
    return stream;
  }

  public String getStreamType() {
    return streamType;
  }

  public int getCount() {
    return count;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long getSum() {
    return sum;
  }
}
