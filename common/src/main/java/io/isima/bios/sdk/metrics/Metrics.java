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
package io.isima.bios.sdk.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.metrics.DurationRecorder;
import io.isima.bios.metrics.MetricRecord;
import io.isima.bios.metrics.MetricSnapshotsInFile;
import io.isima.bios.metrics.MetricsOperation;
import io.isima.bios.metrics.MetricsSnapshot;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TFOS Metrics component.
 *
 * <p>The class is responsible to collect metrics values on demand and to report metrics
 * periodically.
 */
public class Metrics {
  private static final Logger logger = LoggerFactory.getLogger(Metrics.class);

  // TODO(Naoki): Organize client configuration
  public static final String CLIENT_METRICS_ENABLED = "com.tieredfractals.sdk.metrics.enabled";
  private static final String METRIC_DESTINATION = "com.tieredfractals.sdk.metrics.destination";
  private static final String FILE = "file";
  private static final String SERVER = "server";
  private static final String METRIC_DESTINATION_DIR =
      "com.tieredfractals.sdk.metrics.destination.dir";
  private static final String DEFAULT_METRIC_DESTINATION_DIR = "/tmp/metrics";
  private static final String JAVA_SDK_VERSION = BuildVersion.VERSION;
  private static final String FILENAME_DELIMITER = "_";
  private static final String METRIC_FILE_EXTENSION = ".json";
  private static final String ATTRIBUTE_SEPARATOR = ",";

  // TODO(BB-1133): Move these static members to a normal member of the class.

  private static final boolean enabled;

  private static final String metricDestination;

  private static final String metricDestinationDir;

  private static final ScheduledExecutorService reportScheduler;

  private static final ScheduledFuture<?> theTask;

  private static final String MAP_KEY_DELIMETER = ".";

  private static final Map<String, List<DurationRecorder>> recordersMap;

  private static final Set<MetricsReportConsumer> reportConsumers;

  /*
   * Metric snapshot interval is set to 5 minutes
   * This means that 12 snapshots are taken per hour or 288 snapshots per day
   * Metric files are partitioned by date
   * One metric snapshot is about 5KB in size
   * Maximum daily file size would be 5 * 288 = ~2MB
   */
  private static final int REPORT_INTERVAL_SECOND = 300;

  private static ObjectMapper mapper = BiosObjectMapperProvider.get();

  static {
    recordersMap = new ConcurrentHashMap<>();
    reportScheduler = Executors.newScheduledThreadPool(1);
    enabled = Boolean.valueOf(System.getProperty(CLIENT_METRICS_ENABLED, Boolean.FALSE.toString()));
    metricDestination = String.valueOf(System.getProperty(METRIC_DESTINATION, FILE));
    metricDestinationDir =
        String.valueOf(System.getProperty(METRIC_DESTINATION_DIR, DEFAULT_METRIC_DESTINATION_DIR));
    if (enabled) {
      logger.info("Metrics report enabled");
      theTask =
          reportScheduler.scheduleAtFixedRate(
              () -> report(), REPORT_INTERVAL_SECOND, REPORT_INTERVAL_SECOND, TimeUnit.SECONDS);
    } else {
      theTask = null;
    }

    // TODO(BB-1133): Eliminate problems here because of the static nature of this class.
    // 1. Stop doing the concurrent hash set hack. When the injection goes to the constructor,
    // consumers can be a simple array list. The bag is read only once it's constructed.
    // 2. We don't need this instance when the metrics is disabled, so we should put null in case
    // the metrics is disabled. But I don't want to put defensive code everywhere in this class.
    // It's unnecessary in the first place if this class is not static.
    reportConsumers = new ConcurrentHashMap<MetricsReportConsumer, Void>().keySet();
  }

  public static boolean isEnabled() {
    return enabled;
  }

  public static boolean isReportToServerEnabled() {
    return metricDestination.equals(SERVER);
  }

  /**
   * Inject a metric reports consumer.
   *
   * @param reportConsumer The consumer to inject.
   */
  // TODO(BB-1133): This should be done in the constructor of the class ideally, otherwise the
  // first report may happen before the consumer is injected. It's unlikely to happen but
  // possibly to happen technically. Then we need to consider the case. We don't want such an
  // unnecessary complexity.
  public static void injectReportConsumer(MetricsReportConsumer reportConsumer) {
    reportConsumers.add(reportConsumer);
  }

  /** shuts down the service. */
  // TODO(BB-1133): We should modularize the Metrics rather than having everything in static
  // storage.
  public static void shutdown() {
    if (theTask != null) {
      theTask.cancel(true);
    }
    reportScheduler.shutdownNow();
  }

  private static void report() {
    if (metricDestination.equals(FILE)) {
      dumpMetricsToFile();
    }
    reportToConsumers();
  }

  private static void dumpMetricsToFile() {
    try {
      logger.debug("Writing metrics to file");
      Path metricDestinationRootDirectory = Paths.get(metricDestinationDir);
      if (!Files.exists(metricDestinationRootDirectory)) {
        logger.debug(
            "Metrics root directory doesn't exist. Creating metrics root directory {}",
            metricDestinationDir);
        Files.createDirectory(metricDestinationRootDirectory);
      }
      String currentDateMetricFile = getCurrentDateAsString() + METRIC_FILE_EXTENSION;
      Path pathForCurrentDateMetricFile = Paths.get(metricDestinationDir, currentDateMetricFile);
      writeMetricsToFile(pathForCurrentDateMetricFile.toFile());
    } catch (IOException e) {
      logger.error("Exception while flushing metrics report to filesystem ", e);
    }
  }

  private static void writeMetricsToFile(File file) {
    logger.debug("Writing metric to file {}", file.toPath());
    MetricSnapshotsInFile metricSnapshotsInFile = null;
    try {
      String fileContent = new String(Files.readAllBytes(file.toPath()));
      metricSnapshotsInFile = mapper.readValue(fileContent, MetricSnapshotsInFile.class);
    } catch (FileNotFoundException e) {
      logger.debug("File {} does not exit and will be created", file.toPath());
    } catch (Exception e) {
      logger.error("Error while reading from metrics file {}. Exception", file.toPath(), e);
    }

    try (FileWriter fileWriter = new FileWriter(file)) {
      BufferedWriter writer = new BufferedWriter(fileWriter);
      List<MetricRecord> metricRecords = new ArrayList<>();
      for (Entry<String, List<DurationRecorder>> recorders : recordersMap.entrySet()) {
        for (DurationRecorder recorder : recorders.getValue()) {
          recorder.checkpoint();
          MetricRecord metricRecord = recorder.captureMetricRecord();
          metricRecords.add(metricRecord);
        }
      }
      MetricsSnapshot metricsSnapshot =
          new MetricsSnapshot(
              System.currentTimeMillis(), Utils.getNodeName(), JAVA_SDK_VERSION, metricRecords);
      if (metricSnapshotsInFile == null) {
        List<MetricsSnapshot> metricsSnapshots = new ArrayList<>();
        metricsSnapshots.add(metricsSnapshot);
        metricSnapshotsInFile = new MetricSnapshotsInFile(metricsSnapshots);
      } else {
        metricSnapshotsInFile.getMetricsSnapshots().add(metricsSnapshot);
      }
      writer.write(mapper.writeValueAsString(metricSnapshotsInFile));
      logger.debug("Closing file connection to file {}", file.toPath().toString());
      writer.close();
    } catch (Throwable t) {
      logger.error(
          "Error while writing metrics to file {}. Exception {}", file.toPath().toString(), t);
    }
  }

  private static String getCurrentDateAsString() {
    Calendar rightNow = Calendar.getInstance();
    return rightNow.get(Calendar.YEAR)
        + FILENAME_DELIMITER
        + rightNow.get(Calendar.MONTH)
        + FILENAME_DELIMITER
        + rightNow.get(Calendar.DAY_OF_MONTH);
  }

  private static void reportToConsumers() {
    if (!reportConsumers.isEmpty()) {
      logger.debug("Sending metric report to TFOS server");
      Map<String, List<String>> reports = new HashMap<>();
      for (Entry<String, List<DurationRecorder>> recorders : recordersMap.entrySet()) {
        for (DurationRecorder recorder : recorders.getValue()) {
          recorder.checkpoint();
          List<String> metricRecordItems = reports.get(recorder.getTenant());
          if (metricRecordItems == null) {
            metricRecordItems = new ArrayList<>();
          }
          addMetricEventForTenant(metricRecordItems, recorder);
          reports.put(recorder.getTenant(), metricRecordItems);
        }
      }
      reportConsumers.forEach((consumer) -> consumer.apply(reports));
    }
  }

  private static void addMetricEventForTenant(
      List<String> metricEventList, DurationRecorder recorder) {
    StringBuffer event = new StringBuffer();
    event.append(recorder.getStream()).append(ATTRIBUTE_SEPARATOR);
    event.append(recorder.getStreamType()).append(ATTRIBUTE_SEPARATOR);
    event.append(recorder.getOperation().getOperationName()).append(ATTRIBUTE_SEPARATOR);
    event.append(recorder.getOperation().getSubOperationName()).append(ATTRIBUTE_SEPARATOR);
    event.append(recorder.getStandByStat().getCount()).append(ATTRIBUTE_SEPARATOR);
    event.append(recorder.getStandByStat().getDataBytesIn());
    metricEventList.add(event.toString());
  }

  /**
   * To add a new metrics recorder.
   *
   * @param tenant Tenant Name
   * @param stream Stream Name
   * @param operation Metrics Operation
   */
  public static DurationRecorder addRecorder(
      final String tenant,
      final String stream,
      final String streamType,
      final MetricsOperation operation) {
    final String mapKey = tenant.toLowerCase() + MAP_KEY_DELIMETER + stream.toLowerCase();
    final DurationRecorder newRecorder;
    List<DurationRecorder> recorders = recordersMap.getOrDefault(mapKey, new ArrayList<>());
    for (DurationRecorder recorder : recorders) {
      if (Objects.equals(recorder.getOperation(), operation)) {
        return recorder;
      }
    }
    newRecorder =
        new DurationRecorder(
            operation, tenant, stream, streamType, TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS);
    recorders.add(newRecorder);
    recordersMap.put(mapKey, recorders);
    return newRecorder;
  }

  /**
   * To remove a recorder from metrics recorder.
   *
   * @param tenant Tenant Name
   * @param stream Stream Name
   */
  public static void removeRecorders(final String tenant, final String stream) {
    recordersMap.remove(tenant.toLowerCase() + MAP_KEY_DELIMETER + stream.toLowerCase());
  }

  /**
   * Collect overall metrics.
   *
   * @param operation Metrics Operation
   * @param tenant Tenant Name
   * @param stream Stream Name
   * @param value Metric Value
   */
  public static void collect(
      final MetricsOperation operation,
      final String tenant,
      final String stream,
      final long value) {
    if (null != tenant && null != stream) {
      final String mapKey = tenant.toLowerCase() + MAP_KEY_DELIMETER + stream.toLowerCase();
      if (recordersMap.containsKey(mapKey)) {
        List<DurationRecorder> recorderList = getRecorder(tenant, stream);
        if (null == recorderList || recorderList.isEmpty()) {
          return;
        }
        for (DurationRecorder recorder : recorderList) {
          if (Objects.equals(recorder.getOperation(), operation)) {
            recorder.consume(value);
            break;
          }
        }
      }
    }
  }

  /**
   * To get list of recorder for given tenant and stream.
   *
   * @param tenant Tenant Name
   * @param stream Stream Name
   * @return List of DurationRecorder
   */
  private static List<DurationRecorder> getRecorder(final String tenant, final String stream) {
    return recordersMap.get(tenant.toLowerCase() + MAP_KEY_DELIMETER + stream.toLowerCase());
  }
}
