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

import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.QUANTILES_SLOT_BEGIN;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import io.isima.bios.data.impl.models.FunctionResult;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SketchQuantiles extends DataSketch {
  static final Logger logger = LoggerFactory.getLogger(SketchQuantiles.class);
  public static final int COMPRESSION = 200;
  private final TDigest tdigest;

  public SketchQuantiles(InternalAttributeType attributeType, byte[] data) {
    super(attributeType, DataSketchType.QUANTILES);
    if (data != null) {
      tdigest = MergingDigest.fromBytes(ByteBuffer.wrap(data));
    } else {
      tdigest = TDigest.createMergingDigest(COMPRESSION);
    }
  }

  public SketchQuantiles(
      InternalAttributeType attributeType, Collection<DataSketch> sketchesToMerge) {
    super(attributeType, DataSketchType.QUANTILES);
    // MergingDigest seems to have a bug where if the sketch being merged into has
    // some data in it, then the min and lower percentiles get messed up. To avoid this,
    // create a fresh empty sketch to merge into (rather than reusing one of the existing
    // sketches).
    tdigest = TDigest.createMergingDigest(COMPRESSION);
    final var tdigests =
        sketchesToMerge.stream()
            .map(sketch -> ((SketchQuantiles) sketch).tdigest)
            .collect(Collectors.toList());
    tdigest.add(tdigests);
  }

  @Override
  public FunctionResult evaluate(
      MetricFunction function, AttributeDesc attributeDesc, ExecutionState state) {
    assert (function.getDataSketchType() == sketchType);
    assert (tdigest.size() > 0);

    final double percentile;
    if (function == MetricFunction.MEDIAN) {
      percentile = 50.0;
    } else {
      percentile = SketchSummaryColumn.valueOf(function.name()).getPercentile();
    }
    return new FunctionResult(tdigest.quantile(percentile / 100.0));
  }

  @Override
  public ByteBuffer getData() {
    final ByteBuffer buffer = ByteBuffer.allocate(tdigest.smallByteSize());
    tdigest.asSmallBytes(buffer);

    buffer.flip();
    return buffer;
  }

  @Override
  public void update(Object attributeValue) {
    if (attributeValue == null) {
      return;
    }
    final double value = ((Number) attributeValue).doubleValue();
    tdigest.add(value);
  }

  @Override
  public void populateSummary(SketchSummary sketchSummary) {
    for (int i = 0; i < SketchSummaryColumn.QUANTILES_NUM_SLOTS; i++) {
      sketchSummary.getColumnValues()[QUANTILES_SLOT_BEGIN + i] =
          tdigest.quantile(
              SketchSummaryColumn.columnForSlot(QUANTILES_SLOT_BEGIN + i).getPercentile() / 100.0);
    }
    sketchSummary.setCount(tdigest.size());
  }

  @Override
  public long getCount() {
    return tdigest.size();
  }

  /** Used for testing only. */
  public double quantile(double percentile) {
    return tdigest.quantile(percentile / 100.0);
  }

  static class Actor extends SketchActor {
    static final Logger logger = LoggerFactory.getLogger(Actor.class);

    public Actor() {
      super(DataSketchType.QUANTILES);
    }

    @Override
    public Set<SketchSummaryColumn> columnsNeededForFunction(MetricFunction function) {
      assert (function.getDataSketchType() == myDataSketchType);
      final var columns = new HashSet<SketchSummaryColumn>();
      if (function == MetricFunction.MEDIAN) {
        columns.add(SketchSummaryColumn.P50);
      } else {
        columns.add(SketchSummaryColumn.valueOf(function.name()));
      }
      return columns;
    }
  }
}
