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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class SketchActor {
  private static final Map<DataSketchType, SketchActor> actorMap;

  protected final DataSketchType myDataSketchType;

  public SketchActor(DataSketchType myDataSketchType) {
    this.myDataSketchType = myDataSketchType;
  }

  static {
    // Register actors for all sketch types.
    actorMap =
        Map.of(
            DataSketchType.MOMENTS, new SketchMoments.Actor(),
            DataSketchType.QUANTILES, new SketchQuantiles.Actor(),
            DataSketchType.DISTINCT_COUNT, new SketchDistinctCount.Actor(),
            DataSketchType.SAMPLE_COUNTS, new SketchSampleCounts.Actor());
  }

  public Set<SketchSummaryColumn> columnsNeededForFunction(MetricFunction function) {
    assert (function.getDataSketchType() == myDataSketchType);
    final var columns = new HashSet<SketchSummaryColumn>();
    columns.add(SketchSummaryColumn.valueOf(function.name()));
    return columns;
  }

  /**
   * Evaluate the requested metric function, given the input sketch summaries or raw sketches. The
   * implementation here is applicable to sketches that need blobs in order to merge sketches. It is
   * not applicable to Moments sketch, which overrides this implementation.
   *
   * @return 1. null if there aren't any sketches available in a given output time window, or 2.
   *     Double.NaN if there are not sufficient data points to compute the function - this can
   *     happen if we have processed the stream data and computed sketches, and we see that there
   *     are not sufficient data points (e.g. most functions need a minimum of 1 data point;
   *     skewness and kurtosis need 2 data points). 3. Valid result of computing the function if
   *     there are sufficient data points.
   */
  public FunctionResult evaluate(
      MetricFunction function,
      AttributeDesc attributeDesc,
      SingleAttributeSketches singleAttributeSketches,
      ExecutionState state) {
    assert (function.getDataSketchType() == myDataSketchType);

    // We may get either a single summary row or potentially multiple blobs.
    // If it is a single summary row, use it directly - there is no need to merge raw sketches.
    // However if the function requires a blob (i.e. not present in the summary row),
    // then we cannot just use the summary row - need to do the full processing.
    if ((singleAttributeSketches.getSummaryMap().size() == 1) && function.presentInSummaryRow()) {
      final var sketchSummary = singleAttributeSketches.getSummaryMap().firstEntry().getValue();

      switch (function) {
        case MEDIAN:
          return new FunctionResult(
              sketchSummary.getColumnValues()[SketchSummaryColumn.P50.getSlot()]);
        case NUMSAMPLES:
          return new FunctionResult(sketchSummary.getNumSamples());
        case SAMPLINGFRACTION:
          return new FunctionResult(sketchSummary.getSamplingFraction());
        case SAMPLECOUNTS:
          // For this function we need the sketch blob because sketch summary does not contain
          // all the information we need.
          break;
        default:
          return new FunctionResult(
              sketchSummary
                  .getColumnValues()[SketchSummaryColumn.valueOf(function.name()).getSlot()]);
      }
    }

    // Retrieve the raw sketches if any and merge them.

    final var singleTypeSketches =
        singleAttributeSketches.getSketchTypesMap().get(myDataSketchType);
    if ((singleTypeSketches == null) || (singleTypeSketches.getRawSketchMap().size() == 0)) {
      // We don't have any sketches available for this time window.
      return null;
    }

    final DataSketch resultSketch;
    // If we have not already processed all the raw sketches, process them now.
    if (!singleTypeSketches.isRawSketchMapProcessed()) {
      final var sketchesToMerge = new ArrayList<DataSketch>();
      for (final var sketchContents : singleTypeSketches.getRawSketchMap().values()) {
        if (sketchContents.getCount() == 0) {
          continue;
        }
        final var header = SketchHeader.fromBytes(sketchContents.getHeader());
        assert (header.getVersion() == 1);
        assert (attributeDesc != null);
        final DataSketch sketch =
            DataSketch.createSketch(
                myDataSketchType,
                attributeDesc.getAttributeType(),
                sketchContents.getData(),
                sketchContents.getCount());
        sketchesToMerge.add(sketch);
      }
      if (sketchesToMerge.size() == 0) {
        // We have the sketches, but there are no data points in them.
        resultSketch = null;
      } else if (sketchesToMerge.size() == 1) {
        // There is only one sketch with data points. Use it directly without a need to merge.
        resultSketch = sketchesToMerge.get(0);
      } else {
        // Merge multiple sketches.
        resultSketch = DataSketch.createMergedSketch(myDataSketchType, sketchesToMerge);
      }
      singleTypeSketches.setMergedSketch(resultSketch);
      singleTypeSketches.setRawSketchMapProcessed(true);
    } else {
      resultSketch = singleTypeSketches.getMergedSketch();
    }
    // We have processed all the raw sketches at this point.

    if (resultSketch == null) {
      // There are no data points.
      return this.getResultWithNoDataPoints(function);
    }

    // There are sufficient data points; evaluate the function using the potentially merged sketch.
    return resultSketch.evaluate(function, attributeDesc, state);
  }

  protected FunctionResult getResultWithNoDataPoints(MetricFunction function) {
    return new FunctionResult(Double.NaN);
  }

  public static SketchActor get(DataSketchType dataSketchType) {
    return actorMap.get(dataSketchType);
  }
}
