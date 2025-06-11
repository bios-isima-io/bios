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

import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.DISTINCT_COUNT_SLOT_BEGIN;

import io.isima.bios.data.impl.models.FunctionResult;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.nio.ByteBuffer;
import java.util.Collection;
import lombok.Getter;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.memory.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SketchDistinctCount extends DataSketch {
  static final Logger logger = LoggerFactory.getLogger(SketchDistinctCount.class);
  public static final int LG_K = 12;
  @Getter private long count;
  private final CpcSketch cpcSketch;

  public SketchDistinctCount(InternalAttributeType attributeType, byte[] data, long count) {
    super(attributeType, DataSketchType.DISTINCT_COUNT);
    if (data != null) {
      this.count = count;
      cpcSketch = CpcSketch.heapify(Memory.wrap(data));
    } else {
      this.count = 0;
      cpcSketch = new CpcSketch(LG_K);
    }
  }

  public SketchDistinctCount(
      InternalAttributeType attributeType, Collection<DataSketch> sketchesToMerge) {
    super(attributeType, DataSketchType.DISTINCT_COUNT);
    final CpcUnion cpcUnion = new CpcUnion(LG_K);
    for (final var sketch : sketchesToMerge) {
      count += sketch.getCount();
      cpcUnion.update(((SketchDistinctCount) sketch).cpcSketch);
    }
    cpcSketch = cpcUnion.getResult();
  }

  @Override
  public FunctionResult evaluate(
      MetricFunction function, AttributeDesc attributeDesc, ExecutionState state) {
    assert (count > 0);
    return new FunctionResult(evaluateInternal(function));
  }

  private double evaluateInternal(MetricFunction function) {
    assert (function.getDataSketchType() == sketchType);
    switch (function) {
      case DISTINCTCOUNT:
        return cpcSketch.getEstimate();
      case DCLB1:
        return cpcSketch.getLowerBound(1);
      case DCUB1:
        return cpcSketch.getUpperBound(1);
      case DCLB2:
        return cpcSketch.getLowerBound(2);
      case DCUB2:
        return cpcSketch.getUpperBound(2);
      case DCLB3:
        return cpcSketch.getLowerBound(3);
      case DCUB3:
        return cpcSketch.getUpperBound(3);
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public ByteBuffer getData() {
    final ByteBuffer buffer = ByteBuffer.wrap(cpcSketch.toByteArray());
    return buffer;
  }

  @Override
  public void update(Object attributeValue) {
    if (attributeValue == null) {
      return;
    }
    switch (attributeType) {
      case LONG:
        cpcSketch.update((Long) attributeValue);
        break;
      case DOUBLE:
        cpcSketch.update((Double) attributeValue);
        break;
      case STRING:
        cpcSketch.update((String) attributeValue);
        break;
      case ENUM:
        cpcSketch.update((Integer) attributeValue);
        break;
      default:
        throw new UnsupportedOperationException();
    }
    count++;

    return;
  }

  @Override
  public void populateSummary(SketchSummary sketchSummary) {
    for (int i = 0; i < SketchSummaryColumn.DISTINCT_COUNT_NUM_SLOTS; i++) {
      final var function =
          MetricFunction.forValueCaseInsensitive(
              SketchSummaryColumn.columnForSlot(DISTINCT_COUNT_SLOT_BEGIN + i).name());
      sketchSummary.getColumnValues()[DISTINCT_COUNT_SLOT_BEGIN + i] = evaluateInternal(function);
    }
    sketchSummary.setCount(count);
  }

  static class Actor extends SketchActor {
    public Actor() {
      super(DataSketchType.DISTINCT_COUNT);
    }
  }
}
