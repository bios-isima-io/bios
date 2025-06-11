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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class SketchMoments extends DataSketch {
  static final Logger logger = LoggerFactory.getLogger(SketchMoments.class);

  private double min = Double.POSITIVE_INFINITY;
  private double max = Double.NEGATIVE_INFINITY;
  private long count = 0;
  private double sum = 0;
  private double sum2 = 0;
  private double sum3 = 0;
  private double sum4 = 0;

  public SketchMoments(InternalAttributeType attributeType) {
    super(attributeType, DataSketchType.MOMENTS);
  }

  @Override
  public void update(Object attributeValue) {
    if (attributeValue == null) {
      return;
    }
    final double value = ((Number) attributeValue).doubleValue();

    min = Math.min(min, value);
    max = Math.max(max, value);
    count++;
    sum += value;
    double moment = value * value;
    sum2 += moment;
    moment *= value;
    sum3 += moment;
    moment *= value;
    sum4 += moment;
    return;
  }

  @Override
  public void populateSummary(SketchSummary sketchSummary) {
    sketchSummary.setMin(min);
    sketchSummary.setMax(max);
    sketchSummary.setCount(count);
    sketchSummary.setSum(sum);
    sketchSummary.setSum2(sum2);
    sketchSummary.setSum3(sum3);
    sketchSummary.setSum4(sum4);
  }

  static class Actor extends SketchActor {
    static final Logger logger = LoggerFactory.getLogger(Actor.class);

    public Actor() {
      super(DataSketchType.MOMENTS);
    }

    @Override
    @SuppressWarnings("checkstyle:FallThrough")
    public Set<SketchSummaryColumn> columnsNeededForFunction(MetricFunction function) {
      assert (function.getDataSketchType() == myDataSketchType);
      final var columns = new HashSet<SketchSummaryColumn>();
      switch (function) {
        case SUM:
        case COUNT:
        case MIN:
        case MAX:
        case SUM2:
        case SUM3:
        case SUM4:
          columns.add(SketchSummaryColumn.valueOf(function.name()));
          break;

        // For the below items, fall-through is intentional.
        case KURTOSIS:
          columns.add(SketchSummaryColumn.SUM4);
        case SKEWNESS:
          columns.add(SketchSummaryColumn.SUM3);
        case VARIANCE:
        case STDDEV:
          columns.add(SketchSummaryColumn.SUM2);
        case AVG:
          columns.add(SketchSummaryColumn.SUM);
          columns.add(SketchSummaryColumn.COUNT);
          break;

        default:
          throw new UnsupportedOperationException(
              "Columns requested for unsupported function: " + function);
      }
      return columns;
    }

    /**
     * This method is structured to calculate just the items necessary to satisfy the given
     * aggregate function, and avoid excess processing as well as code duplication. This may be a
     * little less readable as a consequence.
     */
    @Override
    public FunctionResult evaluate(
        MetricFunction function,
        AttributeDesc attributeDesc,
        SingleAttributeSketches singleAttributeSketches,
        ExecutionState state) {
      assert (function.getDataSketchType() == myDataSketchType);
      final var sketchSummaries = singleAttributeSketches.getSummaryMap().values();

      if (sketchSummaries.size() == 0) {
        return null;
      }

      switch (function) {
        case COUNT:
          return new FunctionResult(getCount(sketchSummaries));
        case SUM:
          return new FunctionResult(getSumOf(sketchSummaries, i -> i.getSum()));
        case SUM2:
          return new FunctionResult(getSumOf(sketchSummaries, i -> i.getSum2()));
        case SUM3:
          return new FunctionResult(getSumOf(sketchSummaries, i -> i.getSum3()));
        case SUM4:
          return new FunctionResult(getSumOf(sketchSummaries, i -> i.getSum4()));
        case MIN:
          return new FunctionResult(getMin(sketchSummaries));
        case MAX:
          return new FunctionResult(getMax(sketchSummaries));

        // Moments that need more calculation.
        default:
          return new FunctionResult(calculateMoment(function, sketchSummaries));
      }
    }

    private Double getMin(Collection<SketchSummary> sketchSummaries) {
      assert (!sketchSummaries.isEmpty());
      final var optionalMin =
          sketchSummaries.stream().min(Comparator.comparingDouble(SketchSummary::getMin));
      final double min = optionalMin.isPresent() ? optionalMin.get().getMin() : 0;
      if (min == Double.POSITIVE_INFINITY) {
        return Double.NaN;
      } else {
        return min;
      }
    }

    private Double getMax(Collection<SketchSummary> sketchSummaries) {
      assert (!sketchSummaries.isEmpty());
      final var optionalMax =
          sketchSummaries.stream().max(Comparator.comparingDouble(SketchSummary::getMax));
      final double max = optionalMax.isPresent() ? optionalMax.get().getMax() : 0;
      if (max == Double.NEGATIVE_INFINITY) {
        return Double.NaN;
      } else {
        return max;
      }
    }

    private long getCount(Collection<SketchSummary> sketchSummaries) {
      return sketchSummaries.stream().mapToLong(i -> i.getCount()).sum();
    }

    private double getSumOf(
        Collection<SketchSummary> sketchSummaries, ToDoubleFunction<SketchSummary> mapper) {
      return sketchSummaries.stream().mapToDouble(mapper).sum();
    }

    @SuppressWarnings("checkstyle:FallThrough")
    private Double calculateMoment(
        MetricFunction function, Collection<SketchSummary> sketchSummaries) {
      final long count;
      final double sum;
      double sum2 = 0;
      double sum3 = 0;
      double sum4 = 0;

      // First summarize the sums from multiple time windows.
      switch (function) {
        // For the below items, fall-through is intentional.
        case KURTOSIS:
          sum4 = getSumOf(sketchSummaries, i -> i.getSum4());
        case SKEWNESS:
          sum3 = getSumOf(sketchSummaries, i -> i.getSum3());
        case VARIANCE:
        case STDDEV:
          sum2 = getSumOf(sketchSummaries, i -> i.getSum2());
        case AVG:
          sum = getSumOf(sketchSummaries, i -> i.getSum());
          count = getCount(sketchSummaries);
          break;
        default:
          throw new UnsupportedOperationException("Unexpected function %s" + function);
      }

      final double result;
      try {
        result = calculateMomentInternal(function, count, sum, sum2, sum3, sum4);
      } catch (ArithmeticException e) {
        logger.debug("Caught exception in calculateMoment.", e);
        return Double.NaN;
      }
      return result;
    }

    /**
     * We are unable to use Apache Commons library to calculate Skewness and Kurtosis because they
     * only accept the full array of values, not pre-computed sums of powers. So we implement it
     * ourselves, using formulas from: https://en.wikipedia.org/wiki/Moment_(mathematics)
     */
    private double calculateMomentInternal(
        MetricFunction function, long count, double sum, double sum2, double sum3, double sum4) {
      final double mean = sum / count;
      if (function == MetricFunction.AVG) {
        return mean;
      }

      final double eSum2 = sum2 / count; // e = expected value.
      final double mean2 = mean * mean;
      final double variance = eSum2 - mean2;
      if (function == MetricFunction.VARIANCE) {
        return variance;
      }

      final double stddev = Math.sqrt(variance);
      if (function == MetricFunction.STDDEV) {
        return stddev;
      }

      final double eSum3 = sum3 / count;
      final double mean3 = mean2 * mean;
      final double stddev3 = stddev * stddev * stddev;
      final double centralMoment3 = eSum3 - (3 * mean * eSum2) + (2 * mean3);
      final double skewness = centralMoment3 / stddev3;
      if (function == MetricFunction.SKEWNESS) {
        return skewness;
      }

      // MetricFunction.KURTOSIS is the only case left.
      final double eSum4 = sum4 / count;
      final double mean4 = mean3 * mean;
      final double stddev4 = stddev3 * stddev;
      final double centralMoment4 = eSum4 - (4 * mean * eSum3) + (6 * mean2 * eSum2) - (3 * mean4);
      final double kurtosis = centralMoment4 / stddev4;
      return kurtosis;
    }
  }
}
