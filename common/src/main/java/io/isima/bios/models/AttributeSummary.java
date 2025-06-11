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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Currently supported metric functions. */
@Getter
@RequiredArgsConstructor
public enum AttributeSummary {
  NONE(null),
  SUM(MetricFunction.SUM),
  MIN(MetricFunction.MIN),
  MAX(MetricFunction.MAX),
  AVG(MetricFunction.AVG),
  VARIANCE(MetricFunction.VARIANCE),
  STDDEV(MetricFunction.STDDEV),
  SKEWNESS(MetricFunction.SKEWNESS),
  KURTOSIS(MetricFunction.KURTOSIS),
  MEDIAN(MetricFunction.MEDIAN),
  P0_01(MetricFunction.P0_01),
  P0_1(MetricFunction.P0_1),
  P1(MetricFunction.P1),
  P10(MetricFunction.P10),
  P25(MetricFunction.P25),
  P50(MetricFunction.P50),
  P75(MetricFunction.P75),
  P90(MetricFunction.P90),
  P99(MetricFunction.P99),
  P99_9(MetricFunction.P99_9),
  P99_99(MetricFunction.P99_99),
  DISTINCTCOUNT(MetricFunction.DISTINCTCOUNT),
  WORD_CLOUD(null),
  TIMESTAMP_LAG(null);

  private final MetricFunction metricFunctionForSingleValue;

  @JsonCreator
  static AttributeSummary forValueCaseInsensitive(String value) {
    return AttributeSummary.valueOf(value.toUpperCase());
  }

  @JsonValue
  String stringify() {
    return this.toString();
  }
}
