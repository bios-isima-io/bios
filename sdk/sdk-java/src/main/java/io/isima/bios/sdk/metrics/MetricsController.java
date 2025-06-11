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

import io.isima.bios.metrics.DurationRecorder;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Immutable and reentrant abstractions for interactions to the metrics layers. */
public class MetricsController {
  private static final int TIMESTAMPS_BUFFER_SIZE = 8 * 16; // 16 long integers
  private static final MetricsHandle NULL_HANDLE;
  private static final BaseCollector DUMMY_COLLECTOR;
  public static final String STREAM_TYPE_SIGNAL = "Signal";

  static {
    NULL_HANDLE = nullHandle();
    DUMMY_COLLECTOR = new BaseCollector((x) -> null);
  }

  private final DurationRecorder[] metricsRecorders;
  private final String tenantName;
  private final String signalName;
  private final String signalType;
  private final Map<OperationType, BaseCollector> metricCollectors;

  public MetricsController(
      String tenant, String signal, String type, Set<OperationType> allowedSet) {
    this.metricsRecorders = new DurationRecorder[ClientMetricsOperation.values().length];
    this.tenantName = tenant;
    this.signalName = signal;
    this.signalType = type;
    this.metricCollectors = registerMetrics(allowedSet);
  }

  public enum OperationType {
    INSERT,
    SELECT,
    NONE
  }

  /**
   * Use this handle so that if metrics is disabled in the middle of an operation, it will not cause
   * thread safety issues.
   */
  public interface MetricsHandle {
    long beginTime();

    ByteBuffer timeStampBuffer();

    OperationType operation();

    boolean isSync();
  }

  public static Set<OperationType> getOperationsForSignal() {
    Set<OperationType> allowed = new HashSet<>();
    allowed.add(OperationType.SELECT);
    allowed.add(OperationType.INSERT);
    return allowed;
  }

  /**
   * Begins a metric collection.
   *
   * <p>This is independent of a tenant or a signal as there operations that are multi-signal such
   * as multi-select.
   *
   * @param operationType operation type
   * @return a handle
   */
  public static MetricsHandle begin(
      OperationType operationType, boolean syncOp, LongSupplier supplier) {
    if (Metrics.isEnabled()) {
      final ByteBuffer metricTimestamps = ByteBuffer.allocateDirect(TIMESTAMPS_BUFFER_SIZE);
      return metricsHandle(supplier.getAsLong(), metricTimestamps, operationType, syncOp);
    } else {
      return NULL_HANDLE;
    }
  }

  // this records inside collectAsync only if original operation is async. Known from handle.
  public void endAsync(MetricsHandle metricsHandle, long completed) {
    if (metricsHandle.timeStampBuffer() != null) {
      BaseCollector collector =
          metricCollectors.getOrDefault(metricsHandle.operation(), DUMMY_COLLECTOR);
      collector.collectAsync(metricsHandle, completed);
    }
  }

  // this records inside collectAsync only if original operation is async. Known from handle.
  public void endSync(MetricsHandle metricsHandle, long completed) {
    if (metricsHandle.timeStampBuffer() != null) {
      BaseCollector collector =
          metricCollectors.getOrDefault(metricsHandle.operation(), DUMMY_COLLECTOR);
      collector.collectSync(metricsHandle, completed);
    }
  }

  public void end(MetricsHandle metricsHandle, long completed) {
    if (metricsHandle.timeStampBuffer() != null) {
      BaseCollector collector =
          metricCollectors.getOrDefault(metricsHandle.operation(), DUMMY_COLLECTOR);
      collector.collect(metricsHandle, completed);
    }
  }

  DurationRecorder getRecorder(ClientMetricsOperation operation) {
    return metricsRecorders[operation.ordinal()];
  }

  private Map<OperationType, BaseCollector> registerMetrics(Set<OperationType> allowedSet) {
    Map<OperationType, BaseCollector> collectorMap = new HashMap<>();
    if (allowedSet.contains(OperationType.INSERT)) {
      // use old names for now
      collectorMap.put(
          OperationType.INSERT, new InsertCollector((o) -> metricsRecorders[o.ordinal()]));
      for (var op : InsertCollector.INSERT_METRICS_OPERATION) {
        registerMetricsRecorder(op);
      }
    }
    if (allowedSet.contains(OperationType.SELECT)) {
      collectorMap.put(
          OperationType.SELECT, new SelectCollector((o) -> metricsRecorders[o.ordinal()]));
      for (var op : SelectCollector.SELECT_METRICS_OPERATION) {
        registerMetricsRecorder(op);
      }
    }
    return Collections.unmodifiableMap(collectorMap);
  }

  private void registerMetricsRecorder(ClientMetricsOperation operation) {
    DurationRecorder recorder = Metrics.addRecorder(tenantName, signalName, signalType, operation);
    metricsRecorders[operation.ordinal()] = recorder;
  }

  private static MetricsHandle metricsHandle(
      long begin, ByteBuffer metricTimestamps, OperationType operationType, boolean syncOp) {
    return new MetricsHandle() {
      @Override
      public long beginTime() {
        return begin;
      }

      @Override
      public ByteBuffer timeStampBuffer() {
        return metricTimestamps;
      }

      @Override
      public OperationType operation() {
        return operationType;
      }

      @Override
      public boolean isSync() {
        return syncOp;
      }
    };
  }

  private static MetricsHandle nullHandle() {
    return new MetricsHandle() {
      @Override
      public long beginTime() {
        return 0;
      }

      @Override
      public ByteBuffer timeStampBuffer() {
        return null;
      }

      @Override
      public OperationType operation() {
        return OperationType.NONE;
      }

      @Override
      public boolean isSync() {
        return false;
      }
    };
  }

  private static class BaseCollector {
    private final Function<ClientMetricsOperation, DurationRecorder> metricsRecorder;

    BaseCollector(Function<ClientMetricsOperation, DurationRecorder> metricsRecorder) {
      this.metricsRecorder = metricsRecorder;
    }

    /**
     * Assumes filled up by CSDK on little endian systems.
     *
     * <p>Will not work on Big endian machines. Actually better if CSDK stores timestamps always in
     * network byte order (so that it will work on all machines). Filed Jira BB-1027 for this.
     *
     * @param timestamps metric timestamp buffer (typically filled by CSDK)
     * @return long timestamp.
     */
    long fetchTimestamp(ByteBuffer timestamps) {
      long value = 0;
      int shift = 0;
      for (int i = 0; i < 8; ++i) {
        byte element = timestamps.get();
        value += ((long) (element & 0xff)) << shift;
        shift += 8;
      }
      return value;
    }

    void collectMetrics(ClientMetricsOperation operation, long duration) {
      final DurationRecorder recorder = metricsRecorder.apply(operation);
      if (recorder != null) {
        recorder.consume(duration);
      }
    }

    void collectAsync(MetricsHandle handle, long completed) {}

    void collectSync(MetricsHandle handle, long completed) {}

    void collect(MetricsHandle handle, long completed) {}
  }

  private static final class InsertCollector extends BaseCollector {
    private static final ClientMetricsOperation[] INSERT_METRICS_OPERATION = {
      ClientMetricsOperation.INGEST_SYNC,
      ClientMetricsOperation.INGEST_ASYNC,
      ClientMetricsOperation.INGEST_REQUEST_DISPATCH,
      ClientMetricsOperation.INGEST_REQUEST_SUBMIT,
      ClientMetricsOperation.INGEST_SERVER_PROCESS,
      ClientMetricsOperation.INGEST_RECEIVE_RESPONSE,
      ClientMetricsOperation.INGEST_COMPLETION_DISPATCH,
      ClientMetricsOperation.INGEST_COMPLETION_PROCESS,
    };

    private InsertCollector(Function<ClientMetricsOperation, DurationRecorder> metricsRecorder) {
      super(metricsRecorder);
    }

    @Override
    protected void collectAsync(MetricsHandle handle, long completed) {
      if (!handle.isSync()) {
        collectMetrics(ClientMetricsOperation.INGEST_ASYNC, completed - handle.beginTime());
      }
    }

    @Override
    protected void collectSync(MetricsHandle handle, long completed) {
      if (handle.isSync()) {
        collectMetrics(ClientMetricsOperation.INGEST_SYNC, completed - handle.beginTime());
      }
    }

    @Override
    protected void collect(MetricsHandle handle, long completed) {
      long begin = handle.beginTime();
      ByteBuffer metricTimestamps = handle.timeStampBuffer();
      metricTimestamps.clear();
      final long requestDispatch = fetchTimestamp(metricTimestamps);
      collectMetrics(ClientMetricsOperation.INGEST_REQUEST_DISPATCH, requestDispatch - begin);
      final long requestSubmit = fetchTimestamp(metricTimestamps);
      collectMetrics(ClientMetricsOperation.INGEST_REQUEST_SUBMIT, requestSubmit - requestDispatch);
      final long serverProcess = fetchTimestamp(metricTimestamps);
      collectMetrics(ClientMetricsOperation.INGEST_SERVER_PROCESS, serverProcess - requestSubmit);
      final long receiveResponse = fetchTimestamp(metricTimestamps);
      collectMetrics(
          ClientMetricsOperation.INGEST_RECEIVE_RESPONSE, receiveResponse - serverProcess);
      final long completionDispatch = metricTimestamps.getLong();
      collectMetrics(
          ClientMetricsOperation.INGEST_COMPLETION_DISPATCH, completionDispatch - receiveResponse);
      collectMetrics(
          ClientMetricsOperation.INGEST_COMPLETION_PROCESS, completed - completionDispatch);
    }
  }

  private static final class SelectCollector extends BaseCollector {
    private static final ClientMetricsOperation[] SELECT_METRICS_OPERATION = {
      ClientMetricsOperation.EXTRACT_SYNC,
      ClientMetricsOperation.EXTRACT_ASYNC,
      ClientMetricsOperation.EXTRACT_REQUEST_DISPATCH,
      ClientMetricsOperation.EXTRACT_REQUEST_SUBMIT,
      ClientMetricsOperation.EXTRACT_SERVER_PROCESS,
      ClientMetricsOperation.EXTRACT_RECEIVE_RESPONSE,
      ClientMetricsOperation.EXTRACT_COMPLETION_DISPATCH,
      ClientMetricsOperation.EXTRACT_COMPLETION_PROCESS,
    };

    private SelectCollector(Function<ClientMetricsOperation, DurationRecorder> metricsRecorder) {
      super(metricsRecorder);
    }

    @Override
    void collectAsync(MetricsHandle handle, long completed) {
      if (!handle.isSync()) {
        collectMetrics(ClientMetricsOperation.EXTRACT_ASYNC, completed - handle.beginTime());
      }
    }

    @Override
    void collectSync(MetricsHandle handle, long completed) {
      if (handle.isSync()) {
        collectMetrics(ClientMetricsOperation.EXTRACT_SYNC, completed - handle.beginTime());
      }
    }

    @Override
    void collect(MetricsHandle handle, long completed) {
      long begin = handle.beginTime();
      ByteBuffer metricTimestamps = handle.timeStampBuffer();
      metricTimestamps.clear();
      final long requestDispatch = fetchTimestamp(metricTimestamps);
      collectMetrics(ClientMetricsOperation.EXTRACT_REQUEST_DISPATCH, requestDispatch - begin);
      final long requestSubmit = fetchTimestamp(metricTimestamps);
      collectMetrics(
          ClientMetricsOperation.EXTRACT_REQUEST_SUBMIT, requestSubmit - requestDispatch);
      final long serverProcess = fetchTimestamp(metricTimestamps);
      collectMetrics(ClientMetricsOperation.EXTRACT_SERVER_PROCESS, serverProcess - requestSubmit);
      final long receiveResponse = fetchTimestamp(metricTimestamps);
      collectMetrics(
          ClientMetricsOperation.EXTRACT_RECEIVE_RESPONSE, receiveResponse - serverProcess);
      final long completionDispatch = metricTimestamps.getLong();
      collectMetrics(
          ClientMetricsOperation.EXTRACT_COMPLETION_DISPATCH, completionDispatch - receiveResponse);
      collectMetrics(
          ClientMetricsOperation.EXTRACT_COMPLETION_PROCESS, completed - completionDispatch);
    }
  }
}
