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
package io.isima.bios.models;

import static io.isima.bios.models.DataSketchType.DISTINCT_COUNT;
import static io.isima.bios.models.DataSketchType.MOMENTS;
import static io.isima.bios.models.DataSketchType.NONE;
import static io.isima.bios.models.DataSketchType.QUANTILES;
import static io.isima.bios.models.DataSketchType.SAMPLE_COUNTS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.proto.DataProto;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Currently supported metric functions. */
@Getter
@RequiredArgsConstructor
public enum MetricFunction {
  SUM(DataProto.MetricFunction.SUM, UnitModifier.NO_CHANGE, MOMENTS),
  COUNT(DataProto.MetricFunction.COUNT, UnitModifier.DIMENSIONLESS, MOMENTS),
  MIN(DataProto.MetricFunction.MIN, UnitModifier.NO_CHANGE, MOMENTS),
  MAX(DataProto.MetricFunction.MAX, UnitModifier.NO_CHANGE, MOMENTS),
  LAST(DataProto.MetricFunction.LAST, UnitModifier.NOT_APPLICABLE, NONE),
  AVG(DataProto.MetricFunction.AVG, UnitModifier.NO_CHANGE, MOMENTS),
  VARIANCE(DataProto.MetricFunction.VARIANCE, UnitModifier.SQUARED, MOMENTS),
  STDDEV(DataProto.MetricFunction.STDDEV, UnitModifier.NO_CHANGE, MOMENTS),
  SKEWNESS(DataProto.MetricFunction.SKEWNESS, UnitModifier.DIMENSIONLESS, MOMENTS),
  KURTOSIS(DataProto.MetricFunction.KURTOSIS, UnitModifier.DIMENSIONLESS, MOMENTS),
  SUM2(DataProto.MetricFunction.SUM2, UnitModifier.SQUARED, MOMENTS),
  SUM3(DataProto.MetricFunction.SUM3, UnitModifier.CUBED, MOMENTS),
  SUM4(DataProto.MetricFunction.SUM4, UnitModifier.FOURTH_POWER, MOMENTS),
  MEDIAN(DataProto.MetricFunction.MEDIAN, UnitModifier.NO_CHANGE, QUANTILES),
  P0_01(DataProto.MetricFunction.P0_01, UnitModifier.NO_CHANGE, QUANTILES),
  P0_1(DataProto.MetricFunction.P0_1, UnitModifier.NO_CHANGE, QUANTILES),
  P1(DataProto.MetricFunction.P1, UnitModifier.NO_CHANGE, QUANTILES),
  P10(DataProto.MetricFunction.P10, UnitModifier.NO_CHANGE, QUANTILES),
  P25(DataProto.MetricFunction.P25, UnitModifier.NO_CHANGE, QUANTILES),
  P50(DataProto.MetricFunction.P50, UnitModifier.NO_CHANGE, QUANTILES),
  P75(DataProto.MetricFunction.P75, UnitModifier.NO_CHANGE, QUANTILES),
  P90(DataProto.MetricFunction.P90, UnitModifier.NO_CHANGE, QUANTILES),
  P99(DataProto.MetricFunction.P99, UnitModifier.NO_CHANGE, QUANTILES),
  P99_9(DataProto.MetricFunction.P99_9, UnitModifier.NO_CHANGE, QUANTILES),
  P99_99(DataProto.MetricFunction.P99_99, UnitModifier.NO_CHANGE, QUANTILES),
  DISTINCTCOUNT(DataProto.MetricFunction.DISTINCTCOUNT, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCLB1(DataProto.MetricFunction.DCLB1, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCUB1(DataProto.MetricFunction.DCUB1, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCLB2(DataProto.MetricFunction.DCLB2, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCUB2(DataProto.MetricFunction.DCUB2, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCLB3(DataProto.MetricFunction.DCLB3, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  DCUB3(DataProto.MetricFunction.DCUB3, UnitModifier.DIMENSIONLESS, DISTINCT_COUNT),
  NUMSAMPLES(DataProto.MetricFunction.NUMSAMPLES, UnitModifier.DIMENSIONLESS, SAMPLE_COUNTS),
  SAMPLINGFRACTION(
      DataProto.MetricFunction.SAMPLINGFRACTION, UnitModifier.DIMENSIONLESS, SAMPLE_COUNTS),
  SAMPLECOUNTS(DataProto.MetricFunction.SAMPLECOUNTS, UnitModifier.NOT_APPLICABLE, SAMPLE_COUNTS),
  SYNOPSIS(DataProto.MetricFunction.SYNOPSIS, UnitModifier.NOT_APPLICABLE, NONE);

  private final DataProto.MetricFunction proto;

  private final UnitModifier unitModifier;

  private final DataSketchType dataSketchType;

  private static final Map<DataProto.MetricFunction, MetricFunction> mapFromProto =
      createMapFromProto();

  private static Map<DataProto.MetricFunction, MetricFunction> createMapFromProto() {
    final var map = new HashMap<DataProto.MetricFunction, MetricFunction>();
    for (final var value : values()) {
      map.put(value.proto, value);
    }
    return map;
  }

  public static MetricFunction fromProto(DataProto.MetricFunction proto) {
    return mapFromProto.get(proto);
  }

  /** See comments in ResponseShape enums for more details. */
  public ResponseShape getResponseShape() {
    switch (this) {
      case SAMPLECOUNTS:
        return ResponseShape.RESULT_SET;
      case SYNOPSIS:
        return ResponseShape.MULTIPLE_RESULT_SETS;
      default:
        return ResponseShape.VALUE;
    }
  }

  public boolean requiresAttribute() {
    switch (this) {
      case COUNT:
      case SYNOPSIS:
        return false;
      default:
        return true;
    }
  }

  public DataProto.MetricFunction toProto() {
    return proto;
  }

  public boolean presentInSummaryRow() {
    return this != SAMPLECOUNTS;
  }

  public boolean requiresSketch() {
    switch (this) {
      case SUM:
      case COUNT:
      case MIN:
      case MAX:
      case LAST:
      case DISTINCTCOUNT:
      case AVG:
        return false;
      default:
        return true;
    }
  }

  @JsonCreator
  public static MetricFunction forValueCaseInsensitive(String value) {
    return MetricFunction.valueOf(value.toUpperCase());
  }

  @JsonValue
  public String stringify() {
    return name();
  }
}
