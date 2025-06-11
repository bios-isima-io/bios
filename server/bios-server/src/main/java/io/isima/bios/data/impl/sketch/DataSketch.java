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
package io.isima.bios.data.impl.sketch;

import io.isima.bios.data.impl.models.FunctionResult;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@ToString
public abstract class DataSketch {
  protected final InternalAttributeType attributeType;
  protected final DataSketchType sketchType;

  public static DataSketch createSketch(
      DataSketchType sketchType, InternalAttributeType attributeType) {
    return createSketch(sketchType, attributeType, null, 0);
  }

  public static DataSketch createSketch(
      DataSketchType sketchType, InternalAttributeType attributeType, byte[] data, long count) {
    if (sketchType == null) {
      throw new IllegalArgumentException();
    }

    switch (sketchType) {
      case MOMENTS:
        assert (attributeType.isAddable());
        assert (data == null);
        return new SketchMoments(attributeType);
      case QUANTILES:
        assert (attributeType.isAddable());
        return new SketchQuantiles(attributeType, data);
      case DISTINCT_COUNT:
        return new SketchDistinctCount(attributeType, data, count);
      case SAMPLE_COUNTS:
        return new SketchSampleCounts(attributeType, data, count);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static DataSketch createMergedSketch(
      DataSketchType sketchType, Collection<DataSketch> sketchesToMerge) {
    assert (!sketchesToMerge.isEmpty());
    final var attributeType = sketchesToMerge.iterator().next().attributeType;
    switch (sketchType) {
      case QUANTILES:
        return new SketchQuantiles(attributeType, sketchesToMerge);
      case DISTINCT_COUNT:
        return new SketchDistinctCount(attributeType, sketchesToMerge);
      case SAMPLE_COUNTS:
        return new SketchSampleCounts(attributeType, sketchesToMerge);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static Set<SketchSummaryColumn> columnsNeededForFunction(MetricFunction function) {
    return SketchActor.get(function.getDataSketchType()).columnsNeededForFunction(function);
  }

  public static boolean isSupportedFunction(MetricFunction function) {
    if (function.getDataSketchType() == DataSketchType.NONE) {
      return false;
    }
    return true;
  }

  public static FunctionResult evaluate(
      MetricFunction function,
      AttributeDesc attributeDesc,
      SingleAttributeSketches singleAttributeSketches,
      ExecutionState state) {
    return SketchActor.get(function.getDataSketchType())
        .evaluate(function, attributeDesc, singleAttributeSketches, state);
  }

  // Types of methods:
  // 1. If a default implementation can be used by multiple child classes, it is implemented here.
  // 2. If only a subset of child classes need to implement a method, a stub implementation that
  //    throws UnsupportedOperationException is implemented here.
  // 3. If every child class needs to implement the method, it is declared as abstract here.

  public FunctionResult evaluate(
      MetricFunction function, AttributeDesc attributeDesc, ExecutionState state) {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getHeader() {
    final SketchHeader header = new SketchHeader(1);
    return header.toBytes();
  }

  public ByteBuffer getData() {
    throw new UnsupportedOperationException();
  }

  /** Update the sketch with the given value. This should also increment in the internal count. */
  public abstract void update(Object attributeValue);

  /** Adds calculated sketch information to the provided sketchSummary object. */
  public abstract void populateSummary(SketchSummary sketchSummary);

  /**
   * This is the count of items added to this sketch - either directly via update() on this sketch,
   * or indirectly via update() on sketches merged and/or deserialized to build this sketch. This is
   * different from distinctCount.
   */
  public abstract long getCount();
}
