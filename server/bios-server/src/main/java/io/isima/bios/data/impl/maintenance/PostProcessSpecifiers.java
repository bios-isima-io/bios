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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.models.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Post process specifiers that is used as a reply from the post process scheduler. */
@Getter
@EqualsAndHashCode
@ToString
public class PostProcessSpecifiers implements Comparable {
  private final PostProcessOperation op;

  /** Target stream (signal or context) */
  @Setter StreamDesc streamDesc;

  /** Target tenant for CLEANUP_TENANT */
  private TenantDesc tenantDesc;

  /** Time range to process if the processing events are in time domain. */
  @Setter Range timeRange;

  /** Shortest interval to process if the target events are in time domain. */
  @Setter long shortestInterval;

  @Setter boolean isFastTrackRequired;

  @Setter long rollupPoint;

  @Setter long processExecutionTime = 0; // "process immediately" if 0

  @Setter boolean toDoInfer;

  @Setter long lastInferenceTime;

  /** Indexing specifiers. */
  private final List<DigestSpecifier> indexes = new ArrayList<>();

  /** Rollup (summarizing) specifiers. */
  // TODO(BIOS-6727): Consolidate this into indexes
  private final List<DigestSpecifier> rollups = new ArrayList<>();

  private final List<DigestSpecifier> contextFeatures = new ArrayList<>();

  private final List<DataSketchSpecifier> sketches = new ArrayList<>();

  // used for calculating context derivative
  private ContextDerivativeCalculator contextDerivativeCalculator;

  public static PostProcessSpecifiers inferSignalTags(StreamDesc signalDesc, long executionTime) {
    final var specs = new PostProcessSpecifiers(PostProcessOperation.INFER_SIGNAL_TAGS);
    specs.setStreamDesc(signalDesc);
    specs.setProcessExecutionTime(executionTime);
    specs.setLastInferenceTime(executionTime);
    specs.setToDoInfer(true);
    return specs;
  }

  public static PostProcessSpecifiers calculateContextDerivative(
      StreamDesc contextDesc, ContextDerivativeCalculator calculator) {
    Objects.requireNonNull(contextDesc);
    Objects.requireNonNull(calculator);
    final var specs = immediateTask(PostProcessOperation.DERIVE_CONTEXT, contextDesc);
    specs.contextDerivativeCalculator = calculator;
    return specs;
  }

  public static PostProcessSpecifiers scheduleRollup(StreamDesc streamDesc) {
    return immediateTask(PostProcessOperation.SCHEDULE_ROLLUP, streamDesc);
  }

  public static PostProcessSpecifiers scheduleSignalTagInference(StreamDesc streamDesc) {
    return immediateTask(PostProcessOperation.SCHED_TAG_INFERENCE, streamDesc);
  }

  public static PostProcessSpecifiers cancel(StreamDesc streamDesc) {
    return immediateTask(PostProcessOperation.CANCEL, streamDesc);
  }

  public static PostProcessSpecifiers cancelAll() {
    return immediateTask(PostProcessOperation.CANCEL_ALL, null);
  }

  private static PostProcessSpecifiers immediateTask(
      PostProcessOperation op, StreamDesc streamDesc) {
    final var specs = new PostProcessSpecifiers(op);
    specs.setStreamDesc(streamDesc);
    specs.setProcessExecutionTime(0);
    return specs;
  }

  public PostProcessSpecifiers() {
    op = PostProcessOperation.ROLLUP;
  }

  private PostProcessSpecifiers(PostProcessOperation op) {
    this.op = op;
  }

  public void addIndex(DigestSpecifier specifier) {
    indexes.add(specifier);
  }

  public void addRollup(DigestSpecifier specifier) {
    rollups.add(specifier);
  }

  public void addContextFeature(DigestSpecifier specifier) {
    contextFeatures.add(specifier);
  }

  public void addSketch(DataSketchSpecifier specifier) {
    sketches.add(specifier);
  }

  public boolean isEmpty() {
    return indexes.isEmpty() && rollups.isEmpty() && sketches.isEmpty();
  }

  public boolean hasTasks() {
    return op != PostProcessOperation.ROLLUP || !isEmpty();
  }

  @Override
  public int compareTo(Object o) {
    return Long.compare(processExecutionTime, ((PostProcessSpecifiers) o).processExecutionTime);
  }
}
