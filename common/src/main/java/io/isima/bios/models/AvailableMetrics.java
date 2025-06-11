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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@JsonInclude(Include.NON_NULL)
public class AvailableMetrics {
  private List<Metric> metrics = new ArrayList<Metric>();
  private List<Signal> signals = new ArrayList<Signal>();
  private List<Context> contexts = new ArrayList<Context>();

  @Getter
  @Setter
  @ToString
  @NoArgsConstructor
  @JsonInclude(Include.NON_NULL)
  public static class Metric {
    private MetricFunction metricFunction;
    private UnitModifier unitModifier;
  }

  @Getter
  @Setter
  @ToString
  @NoArgsConstructor
  @JsonInclude(Include.NON_NULL)
  public static class Signal {
    private String signalName;
    private List<MetricFunction> mainMetrics = new ArrayList<MetricFunction>();
    private List<Attribute> attributes = new ArrayList<Attribute>();
  }

  @Getter
  @Setter
  @ToString
  @NoArgsConstructor
  @JsonInclude(Include.NON_NULL)
  public static class Context {
    private String contextName;
    private List<MetricFunction> mainMetrics = new ArrayList<MetricFunction>();
    private List<Attribute> attributes = new ArrayList<Attribute>();
  }

  @Getter
  @Setter
  @ToString
  @NoArgsConstructor
  @JsonInclude(Include.NON_NULL)
  public static class Attribute {
    private String attributeName;
    // The default .toString() of AttributeType creates ALL_CAPS names, but the convention we
    // follow is PascalCase, which is created by .stringify(). So, we are using a string here.
    private String attributeType;
    private Unit unit;
    private String unitDisplayName;
    private UnitDisplayPosition unitDisplayPosition;
    private PositiveIndicator positiveIndicator;
    private List<MetricFunction> mainMetrics = new ArrayList<MetricFunction>();
    private List<MetricFunction> commonMetrics = new ArrayList<MetricFunction>();
    private List<MetricFunction> remainingMetrics = new ArrayList<MetricFunction>();
  }
}
