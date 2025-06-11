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
package io.isima.bios.export;

import io.isima.bios.framework.BiosModules;
import io.isima.bios.maintenance.MaintenanceWorker;
import io.isima.bios.maintenance.ServiceStatus;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker thread that pushes data periodically. */
public class DataExportWorker extends MaintenanceWorker {
  private static final Logger logger = LoggerFactory.getLogger(DataExportWorker.class);

  private static final int EXPORT_INTERVAL_SECONDS = 300;
  private static final int NUM_CYCLES_BEFORE_EMPTYING = 2;

  private final Map<String, ExportSink> exportRegistry;
  private final List<ExportSink> activeSinks;
  private final Set<ExportSink> closingSinks;

  private long cyclesSoFar = 0L;
  private volatile boolean shuttingDown = false;

  public DataExportWorker(ServiceStatus serviceStatus, BiosModules parentModules) {
    super(
        EXPORT_INTERVAL_SECONDS,
        EXPORT_INTERVAL_SECONDS,
        TimeUnit.SECONDS,
        serviceStatus,
        parentModules);
    this.exportRegistry = new ConcurrentHashMap<>();
    this.activeSinks = new ArrayList<>();
    this.closingSinks = ConcurrentHashMap.newKeySet();
  }

  @Override
  public void runMaintenance() {
    try {
      export();
    } catch (Exception e) {
      // as of now do not crash the server on export data errors
      logger.warn("Unable to export data " + e.getMessage());
    }
  }

  /**
   * shuts down the export worker.
   *
   * <p>Maintenance thread is assumed to be stopped
   */
  public void shutdown() {
    boolean interrupted = false;
    try {
      shuttingDown = true;
      int numTries = 0;
      export();
      while (!activeSinks.isEmpty() && numTries++ < 500) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
        export();
      }
    } finally {
      if (interrupted) {
        // preserve the interrupt flag
        Thread.currentThread().interrupt();
      }
    }
  }

  public void registerDataProvider(
      DataProvider<ByteBuffer> dataProvider, DataExporter<ByteBuffer> exporter) {
    exportRegistry.computeIfAbsent(
        toKey(
            dataProvider.getTenantName(),
            dataProvider.getSignalName(),
            dataProvider.getSignalVersion()),
        (k) -> new ExportSink(dataProvider, exporter));
  }

  public void deregisterDataProvider(DataProvider<ByteBuffer> dataProvider) {
    final var sink =
        exportRegistry.remove(
            toKey(
                dataProvider.getTenantName(),
                dataProvider.getSignalName(),
                dataProvider.getSignalVersion()));
    if (sink != null) {
      closingSinks.add(sink);
    }
  }

  private void export() {
    if (exportRegistry.isEmpty() && activeSinks.isEmpty() && closingSinks.isEmpty()) {
      return;
    }

    // Exporting data should not run while the service is in maintenance mode.
    // The server is in maintenance mode during failure recovery (cache reloading), upgrading, etc.
    if (serviceStatus.isInMaintenance()) {
      return;
    }

    final Set<ExportSink> passiveSinks = new HashSet<>(exportRegistry.values());
    passiveSinks.addAll(closingSinks);

    final var each = activeSinks.listIterator();
    while (each.hasNext()) {
      final var sink = each.next();
      if (sink.isFlushing()) {
        passiveSinks.remove(sink);
      } else {
        sink.clear();
        each.remove();
        passiveSinks.add(sink);
      }
    }

    cyclesSoFar++;
    for (final var sink : passiveSinks) {
      sink.consumePendingBatch();
      if (cyclesSoFar % NUM_CYCLES_BEFORE_EMPTYING == 0
          || closingSinks.contains(sink)
          || shuttingDown) {
        if (sink.emptyToSink()) {
          activeSinks.add(sink);
        }
      }
    }
    closingSinks.clear();
  }

  private static String toKey(String tenantName, String signalName, long version) {
    return signalName + ":" + tenantName + ":" + version;
  }

  private static final class ExportSink implements DataSink<ByteBuffer> {
    private final DataProvider<ByteBuffer> dataProvider;
    private final DataExporter<ByteBuffer> dataExporter;
    private final AtomicLong flushComplete;

    // flush Counter is only incremented by export worker
    private long flushPending;

    private ExportSink(DataProvider<ByteBuffer> dataProvider, DataExporter<ByteBuffer> exporter) {
      this.dataProvider = dataProvider;
      this.dataProvider.registerDataSink(this);
      this.dataExporter = exporter;
      this.flushComplete = new AtomicLong(0);
      this.flushPending = 0;
    }

    private void consumePendingBatch() {
      dataProvider.getDataPipe().drainPipe();
    }

    private boolean emptyToSink() {
      if (dataProvider.flushDataToSink()) {
        this.flushPending++;
        return true;
      }
      return false;
    }

    private void clear() {
      this.dataProvider.sinkFlushed();
      dataProvider.getDataPipe().clearDrain();
    }

    private boolean isFlushing() {
      return flushComplete.get() != flushPending;
    }

    @Override
    public void onData(ByteBuffer data, long timestamp) {
      this.dataExporter
          .exportDataAsync(data, timestamp)
          .whenComplete(
              (v, t) -> {
                if (t != null) {
                  logger.warn(
                      "Exception while exporting {}:{}:{}",
                      dataProvider.getTenantName(),
                      dataProvider.getSignalName(),
                      dataProvider.getSignalVersion());
                }
                flushComplete.incrementAndGet();
              });
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExportSink sink = (ExportSink) o;
      return dataProvider.equals(sink.dataProvider);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataProvider);
    }
  }
}
