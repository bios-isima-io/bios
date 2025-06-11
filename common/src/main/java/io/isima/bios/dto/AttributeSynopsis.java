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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.isima.bios.models.AttributeOrigin;
import io.isima.bios.models.AttributeSummary;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.PositiveIndicator;
import io.isima.bios.models.UnitDisplayPosition;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttributeSynopsis {
  private String attributeName;
  private Long lastUpdated;
  private Long durationSecs;
  private Long timeWindowSizeSecs;
  private AttributeType attributeType;
  private AttributeOrigin attributeOrigin;
  private String unitDisplayName;
  private UnitDisplayPosition unitDisplayPosition;
  private PositiveIndicator positiveIndicator;
  private AttributeSummary firstSummary;
  private AttributeSummary secondSummary;
  private List<Long> startTime;
  private List<Long> count;
  private List<Number> distinctCount;
  private List<Number> sum;
  private List<Number> avg;
  private List<Number> min;
  private List<Number> max;
  private List<Number> stddev;
  private List<Number> skewness;
  private List<Number> kurtosis;
  private List<Number> p1;
  private List<Number> p25;
  private List<Number> median;
  private List<Number> p75;
  private List<Number> p99;
  private List<Object> samples;
  private List<Long> sampleCounts;
  private List<Long> sampleLengths;
  private Number distinctCountCurrent;
  private Object firstSummaryCurrent;
  private Object secondSummaryCurrent;
}
