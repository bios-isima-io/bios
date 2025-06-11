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
package io.isima.bios.data.impl.maintenance;

import static java.lang.Boolean.TRUE;
import static org.apache.logging.log4j.core.net.Severity.DEBUG;
import static org.apache.logging.log4j.core.net.Severity.ERROR;
import static org.apache.logging.log4j.core.net.Severity.INFO;
import static org.apache.logging.log4j.core.net.Severity.WARNING;

import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.utils.StringUtils;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.core.net.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PostProcessMonitor {
  private static final Logger logger = LoggerFactory.getLogger(PostProcessMonitor.class);

  private static final String PROP_KEY_OVERLOAD_THRESHOLD = "prop.rollupOverloadThreshold";
  private static final long DEFAULT_OVERLOAD_THRESHOLD = 10000;

  private static final String PROP_KEY_DELAY_THRESHOLD = "prop.rollupDelayReportThreshold";
  private static final long DEFAULT_DELAY_THRESHOLD = 600000;

  private static final long SUMMARY_REPORT_INTERVAL = 60000;

  private static final int MAX_OP_NAME_LENGTH;

  static {
    int longest = 0;
    for (var op : PostProcessOperation.values()) {
      longest = Math.max(longest, op.name().length());
    }
    MAX_OP_NAME_LENGTH = longest;
  }

  // dependency components
  private final SharedProperties sharedProperties;

  // log parameters
  private final String executorName;
  private final int numThreads;
  private long overloadDetectedTime = 0;
  private long lastReportTime;

  // overload check
  private final AtomicLong numRequestedTasks = new AtomicLong(0);
  private final AtomicLong numLoadedTasks = new AtomicLong(0);
  private final AtomicLong maxConcurrency = new AtomicLong(0);

  // operation metrics
  private final Map<PostProcessOperation, AtomicReference<Metrics>> metrics =
      new ConcurrentHashMap<>();

  // rollup lag check
  private final Map<String, ProcessedRollupInfo> processed = new ConcurrentHashMap<>();

  // @Getter
  // @Setter
  // private Set<StreamId> latestFastTrackSignals = Set.of();

  public PostProcessMonitor(
      SharedProperties sharedProperties, String executorName, int numThreads) {
    this.sharedProperties = sharedProperties;
    this.executorName = executorName;
    this.numThreads = numThreads;
    lastReportTime = System.currentTimeMillis();
  }

  public PostProcessContext taskScheduling(
      PostProcessSpecifiers specs, boolean countTask, Executor executor) {
    numRequestedTasks.incrementAndGet();
    return new PostProcessContext(System.currentTimeMillis(), specs, countTask, executor, this);
  }

  public void taskPickedUp(PostProcessContext context) {
    numRequestedTasks.decrementAndGet();
    final var concurrency = numLoadedTasks.incrementAndGet();
    while (true) {
      final var current = maxConcurrency.get();
      if (concurrency <= current || maxConcurrency.compareAndSet(current, concurrency)) {
        break;
      }
    }
    final var metricsRef =
        metrics.computeIfAbsent(
            context.getSpecs().getOp(),
            (op) -> new AtomicReference<>(new Metrics(0, 0, 0, 0, 0, 0, false)));
    while (true) {
      final var currentMetrics = metricsRef.get();
      final Metrics nextMetrics = currentMetrics.incrementConcurrency();
      if (metricsRef.compareAndSet(currentMetrics, nextMetrics)) {
        break;
      }
    }
  }

  public void taskEnded(PostProcessContext context) {
    final var now = System.currentTimeMillis();
    numLoadedTasks.decrementAndGet();
    final var specs = context.getSpecs();
    final var metricsRef = metrics.get(specs.getOp());
    assert metricsRef != null;
    final long elapsed = now - context.getStartTime();
    while (true) {
      final var currentMetrics = metricsRef.get();
      final var lagInStartingTask =
          context.getStartTime() > 0 ? context.getStartTime() - context.getPickupTime() : 0;
      final Metrics nextMetrics = currentMetrics.update(elapsed, lagInStartingTask, -1);
      if (metricsRef.compareAndSet(currentMetrics, nextMetrics)) {
        break;
      }
    }
    final var processedSpecs = context.getProcessedSpecs();
    final var streamDesc = specs.getStreamDesc();
    final var tenantName = streamDesc.getParent().getName();
    if (processedSpecs != null && !processedSpecs.isEmpty() && streamDesc != null) {
      for (var spec : processedSpecs) {
        final var streamId = tenantName + "/" + spec.getName() + "/" + spec.getVersion();
        final var lag =
            context.getStartTime() > 0 ? context.getStartTime() - context.getRequestTime() : 0;
        processed.put(streamId, new ProcessedRollupInfo(streamDesc, spec, now, lag));
      }
    }
  }

  public void check() {
    long overloadThreshold =
        sharedProperties.getPropertyCached(PROP_KEY_OVERLOAD_THRESHOLD, DEFAULT_OVERLOAD_THRESHOLD);
    boolean overloadReported = false;
    for (var entry : metrics.entrySet()) {
      final var op = entry.getKey();
      final var metricsRef = entry.getValue();
      Metrics currentMetrics = metricsRef.get();
      Metrics clearMetrics = new Metrics(0, 0, 0, 0, currentMetrics.concurrency, 0, false);
      while (!metricsRef.compareAndSet(currentMetrics, clearMetrics)) {
        currentMetrics = metricsRef.get();
      }
      final long handledTasks = currentMetrics.count;
      final long avgTime = handledTasks > 0 ? currentMetrics.sumTime / handledTasks : 0;
      final long lagInStartingTask = currentMetrics.lagInStartingTaskMax;
      boolean isOverloaded = lagInStartingTask > overloadThreshold;
      var severity = DEBUG;
      if (System.currentTimeMillis() < overloadDetectedTime + 60000) {
        overloadReported = true;
        if (isOverloaded) {
          severity = WARNING;
        } else {
          severity = INFO;
        }
      } else if (!isOverloaded) {
        overloadDetectedTime = 0;
      }
      if (overloadDetectedTime == 0 && isOverloaded) {
        severity = ERROR;
        overloadDetectedTime = System.currentTimeMillis();
        overloadReported = true;
      }
      logOverloadStatus(
          severity,
          executorName,
          op,
          currentMetrics.concurrency,
          currentMetrics.maxConcurrency,
          handledTasks,
          lagInStartingTask,
          numThreads,
          avgTime,
          currentMetrics.maxTime);
    }
    if (overloadReported) {
      logger.warn(
          "[{}] Overall concurrency: {}, max concurrency: {}",
          executorName,
          numLoadedTasks.get(),
          maxConcurrency.get());
    } else {
      logger.debug(
          "[{}] Overall concurrency: {}, max concurrency: {}",
          executorName,
          numLoadedTasks.get(),
          maxConcurrency.get());
    }
    maxConcurrency.set(0);
    checkLags();
  }

  public void checkLags() {
    final var laggedFeatures = new TreeMap<String, String>();
    final var laggedTenants = new TreeSet<String>();
    final var now = System.currentTimeMillis();
    int maxNameLength = 0;
    int numFeatures = 0;
    String status = "green";
    long worstDelay = 0;
    final long delayThreshold =
        sharedProperties.getPropertyCached(PROP_KEY_DELAY_THRESHOLD, DEFAULT_DELAY_THRESHOLD);
    for (var entry : processed.entrySet()) {
      final var name = entry.getKey();
      final var info = entry.getValue();
      final var spec = info.spec;

      final var admin = BiosModules.getAdminInternal();
      final var tenantName = info.streamDesc.getParent().getName();
      final var streamName = info.streamDesc.getName();
      final var streamVersion = info.streamDesc.getVersion();
      final var streamDesc = admin.getStreamOrNull(tenantName, streamName, streamVersion);
      if (streamDesc == null || streamDesc.isDeleted() || streamDesc.getIsLatestVersion() != TRUE) {
        continue;
      }
      ++numFeatures;

      Long interval = null;
      final var rollup = streamDesc.findRollup(spec.getName());
      if (rollup != null) {
        interval = Long.valueOf(rollup.getInterval().getValueInMillis());
      } else if (streamDesc.getFeatures() != null) {
        for (var feature : streamDesc.getFeatures()) {
          final String indexName =
              AdminUtils.makeContextIndexStreamName(streamName, feature.getName());
          if (indexName.equalsIgnoreCase(spec.getName())) {
            interval = feature.getFeatureInterval();
            break;
          }
        }
      }
      if (interval == null) {
        interval = 300000L;
      }

      final var delay = now - spec.getDoneUntil() - interval;
      worstDelay = Math.max(worstDelay, delay);
      if (delay > delayThreshold) {
        laggedTenants.add(tenantName);
        final var message =
            String.format(
                "%s [%s : %s] lag=%s",
                streamDesc.getType().name().toLowerCase(),
                StringUtils.tsToIso8601(spec.getDoneSince()),
                StringUtils.tsToIso8601(spec.getDoneUntil()),
                StringUtils.shortReadableDuration(delay));
        laggedFeatures.put(name, message);
        maxNameLength = Math.max(maxNameLength, name.length());
        status = "red";
      }
    }
    final var isReportTime = now - lastReportTime > SUMMARY_REPORT_INTERVAL;
    boolean lagsReported = false;
    if (!laggedFeatures.isEmpty() && isReportTime) {
      final var sb =
          new StringBuilder(
              String.format(
                  "Rollup [%s] has features that lag behind in tenants %s; worstDelay=%s:\n",
                  executorName, laggedTenants, StringUtils.shortReadableDuration(worstDelay)));
      final var joiner = new StringJoiner("\n");
      for (var item : laggedFeatures.entrySet()) {
        final var name = item.getKey();
        final var info = item.getValue();
        final var spaces = new String(new char[maxNameLength - name.length()]).replace("\0", " ");
        joiner.add(String.format("    %s%s - %s", name, spaces, info));
      }
      sb.append(joiner);
      logger.error(sb.toString());
      lagsReported = true;
    }
    if (lagsReported || isReportTime) {
      logger.info(
          "Rollup [{}] coverage; status={}, numFeatures={}, lagged={}, worstDelay={}, thr={}",
          executorName,
          status,
          numFeatures,
          laggedFeatures.size(),
          StringUtils.shortReadableDuration(worstDelay),
          StringUtils.shortReadableDuration(delayThreshold));
      lastReportTime = now;
    }
  }

  /**
   * Stops tracking specified stream.
   *
   * <p>Should be called when a stream is modified or deleted.
   */
  public void clearStream(StreamDesc streamDesc) {
    final var streamIdToRemove = streamDesc.getStreamId();
    processed
        .entrySet()
        .removeIf((entry) -> entry.getValue().streamDesc.getStreamId().equals(streamIdToRemove));
  }

  private void logOverloadStatus(
      Severity severity, String executorName, PostProcessOperation op, Object... args) {
    final var opName = op.name();
    final var spaces =
        new String(new char[MAX_OP_NAME_LENGTH - opName.length()]).replace("\0", " ");
    final var message =
        String.format(
            "[{}] Task {}%s %s; loadedTasks={}, maxConcurrency={}, handledTasks={},"
                + " lag={}, threads={}, avgTime={}, maxTime={}",
            spaces, severity == WARNING || severity == ERROR ? "overloaded" : "          ");
    final var params = new Object[args.length + 2];
    params[0] = executorName;
    params[1] = op;
    for (int i = 0; i < args.length; ++i) {
      params[i + 2] = args[i];
    }

    switch (severity) {
      case ERROR:
        logger.error(message, params);
        break;
      case WARNING:
        logger.warn(message, params);
        break;
      case INFO:
        logger.info(message, params);
        break;
      case DEBUG:
        logger.debug(message, params);
        break;
    }
  }

  @AllArgsConstructor
  @RequiredArgsConstructor
  private static class Metrics {
    public final long count;
    public final long sumTime;
    public final long maxTime;

    public final long lagInStartingTaskMax;
    public final long concurrency;
    public final long maxConcurrency;

    public final boolean hasDetails;

    public long scheduleSum;
    public long scheduleMax;

    public long countSum;
    public long countMax;

    public long indexSum;
    public long indexMax;

    public long rollupSum;
    public long rollupMax;

    public long sketchSum;
    public long sketchMax;

    public Metrics update(long elapsedTime, long lagInStartingTask, long concurrencyDelta) {
      final var nextConcurrency = concurrency + concurrencyDelta;
      final var nextMaxConcurrency = Math.max(maxConcurrency, nextConcurrency);
      return new Metrics(
          count + 1,
          sumTime + elapsedTime,
          Math.max(maxTime, elapsedTime),
          Math.max(lagInStartingTaskMax, lagInStartingTask),
          nextConcurrency,
          nextMaxConcurrency,
          false);
    }

    public Metrics update(
        long elapsedTime,
        long scheduleTime,
        long countTime,
        long indexTime,
        long rollupTime,
        long sketchTime,
        long lagInStartingTask,
        long concurrencyDelta) {
      final var nextConcurrency = concurrency + concurrencyDelta;
      final var nextMaxConcurrency = Math.max(maxConcurrency, nextConcurrency);
      return new Metrics(
          count + 1,
          sumTime + elapsedTime,
          Math.max(maxTime, elapsedTime),
          Math.max(lagInStartingTaskMax, lagInStartingTask),
          nextConcurrency,
          nextMaxConcurrency,
          true,
          scheduleSum + scheduleTime,
          Math.max(scheduleMax, scheduleTime),
          countSum + countTime,
          Math.max(countMax, countTime),
          indexSum + indexTime,
          Math.max(indexMax, indexTime),
          rollupSum + rollupTime,
          Math.max(rollupMax, rollupTime),
          sketchSum + sketchTime,
          Math.max(sketchMax, sketchTime));
    }

    public Metrics incrementConcurrency() {
      final var nextConcurrency = concurrency + 1;
      final var nextMaxConcurrency = Math.max(maxConcurrency, nextConcurrency);
      return new Metrics(
          count,
          sumTime,
          maxTime,
          lagInStartingTaskMax,
          nextConcurrency,
          nextMaxConcurrency,
          hasDetails,
          scheduleSum,
          scheduleMax,
          countSum,
          countMax,
          indexSum,
          indexMax,
          rollupSum,
          rollupMax,
          sketchSum,
          sketchMax);
    }
  }

  @AllArgsConstructor
  private static class ProcessedRollupInfo {
    public final StreamDesc streamDesc;
    public final DigestSpecifier spec;
    public final long processedTime;
    public final long lagInStartingTask;
  }
}
