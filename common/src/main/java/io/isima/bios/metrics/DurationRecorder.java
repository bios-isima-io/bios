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

import io.isima.bios.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** Class that records metrics information for tenant/stream/operation for metrics report. */
public class DurationRecorder {

  private Stats statsActive;
  private Stats statsStandby;

  private final MetricsOperation operation;
  private final String tenant;
  private final String stream;
  private final String streamType;
  private final TimeUnit sourceTimeUnit;
  private final TimeUnit reportTimeUnit;

  /**
   * @param operation Metrics operation.
   * @param tenant Tenant name in the recorder
   * @param stream Stream name in the recorder
   * @param streamType Type of Stream
   * @param sourceTimeUnit Time Unit of source duration data.
   * @param reportTimeUnit Time unit of duration values in reports.
   */
  public DurationRecorder(
      MetricsOperation operation,
      String tenant,
      String stream,
      String streamType,
      TimeUnit sourceTimeUnit,
      TimeUnit reportTimeUnit) {
    this.operation = operation;
    this.tenant = tenant;
    this.stream = stream;
    this.streamType = streamType;
    this.sourceTimeUnit = sourceTimeUnit;
    this.reportTimeUnit = reportTimeUnit;
    this.statsActive = new Stats();
    this.statsStandby = new Stats();
  }

  /**
   * Push a duration value to the recorder.
   *
   * @param latency Duration value to accumulate
   * @param dataBytesIn Size of input data
   */
  public synchronized void consumeIn(long latency, long dataBytesIn) {
    statsActive.push(latency, dataBytesIn, 0L);
  }

  public void consumeError(long latency, long dataBytesIn) {
    statsActive.pushError(latency, dataBytesIn);
  }

  /**
   * Push a duration value to the recorder.
   *
   * @param latency Duration value to accumulate
   * @param dataBytesOut Size of input data
   */
  public synchronized void consumeOut(long latency, long dataBytesOut) {
    statsActive.push(latency, 0L, dataBytesOut);
  }

  /**
   * Push a duration value to the recorder.
   *
   * @param latency Duration value to accumulate
   */
  public synchronized void consume(long latency) {
    statsActive.push(latency, 0L, 0L);
  }

  /**
   * Flush accumulated metrics to standBy stat object and reset active stat.
   *
   * <p>Assumption here is that checkpoint call is made only by a single thread. A volatile write is
   * still necessary so that this thread 'sees' what other threads has written to statsActive.
   */
  public synchronized void checkpoint() {
    Stats temp = statsStandby;
    statsStandby = statsActive;
    temp.init();
    statsActive = temp;
  }

  /**
   * Reports metrics accumulated in the histogram.
   *
   * @param logger Logger to be used for logging
   * @param applicationVersion version info
   * @return a formatted string containing the report
   */
  public String report(Logger logger, final String applicationVersion) {
    logger.trace(
        "Reporting Metrics for tenant "
            + tenant
            + ", Stream : "
            + stream
            + ", Report is : "
            + statsStandby.report());

    return String.format(
        "%d,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d",
        System.currentTimeMillis(),
        Utils.getNodeName(),
        applicationVersion,
        operation.getOperationName(),
        operation.getSubOperationName(),
        tenant,
        stream,
        streamType,
        statsStandby.getCount(),
        statsStandby.getMin(),
        statsStandby.getMax(),
        statsStandby.getSum());
  }

  /**
   * Constructs MetricRecord object from this durationRecorder
   *
   * @return MetricRecord object
   */
  public MetricRecord captureMetricRecord() {
    return new MetricRecord(
        operation.getOperationName(),
        operation.getSubOperationName(),
        tenant,
        stream,
        streamType,
        statsStandby.getCount(),
        statsStandby.getMin(),
        statsStandby.getMax(),
        statsStandby.getSum());
  }

  /**
   * Method to return the time unit of source data.
   *
   * @return Source time unit
   */
  public TimeUnit getSourceTimeUnit() {
    return sourceTimeUnit;
  }

  /** Method to return the time unit of duration values in reports. */
  public TimeUnit getReportTimeUnit() {
    return reportTimeUnit;
  }

  /**
   * Method to return the metrics operation.
   *
   * @return metrics operation
   */
  public MetricsOperation getOperation() {
    return operation;
  }

  /**
   * Method to return the tenant values in reports.
   *
   * @return tenant name
   */
  public String getTenant() {
    return tenant;
  }

  /**
   * Method to return the stream values in reports.
   *
   * @return stream name
   */
  public String getStream() {
    return stream;
  }

  /** Method to return the stream type */
  public String getStreamType() {
    return streamType;
  }

  /**
   * To get in memory stat to get metrics
   *
   * @return stats
   */
  public Stats getStandByStat() {
    return statsStandby;
  }
}
