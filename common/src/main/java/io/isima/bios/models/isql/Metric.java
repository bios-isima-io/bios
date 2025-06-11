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
package io.isima.bios.models.isql;

import io.isima.bios.models.MetricFunction;

/** Handles various supported metric functions such as SUM, COUNT etc. */
public interface Metric {
  /**
   * Gets the metric function.
   *
   * @return metric function
   */
  MetricFunction getFunction();

  /**
   * Gets the "of" attribute for this metric.
   *
   * @return of attribute
   */
  String getOf();

  /**
   * Gets the Alias.
   *
   * @return alias
   */
  String getAs();

  /**
   * Computes the actual output attribute name. Final name for the attribute depends on whether
   * alias {@link this#getAs()} is specified or not.
   *
   * @return output attribute name
   */
  String getOutputAttributeName();

  /**
   * Specifies count metric.
   *
   * @return an extensible specifier that allows further specification of optional properties of the
   *     metric
   */
  static MetricFinalSpecifier count() {
    return ISqlBuilderProvider.getBuilderProvider().getMetricSpecifier(MetricFunction.COUNT);
  }

  /**
   * Specifies sum metric.
   *
   * @param of attribute to aggregate
   * @return an extensible specifier that allows further specification of optional properties of the
   *     metric
   */
  static MetricFinalSpecifier sum(String of) {
    Metric.Builder bldr =
        ISqlBuilderProvider.getBuilderProvider().getMetricSpecifier(MetricFunction.SUM);
    return bldr.of(of);
  }

  /**
   * Specifies min metric.
   *
   * @param of attribute to aggregate
   * @return an extensible specifier that allows further specification of optional properties of the
   *     metric
   */
  static MetricFinalSpecifier min(String of) {
    Metric.Builder bldr =
        ISqlBuilderProvider.getBuilderProvider().getMetricSpecifier(MetricFunction.MIN);
    return bldr.of(of);
  }

  /**
   * Specifies max metric.
   *
   * @param of attribute to aggregate
   * @return an extensible specifier that allows further specification of optional properties of the
   *     metric
   */
  static MetricFinalSpecifier max(String of) {
    Metric.Builder bldr =
        ISqlBuilderProvider.getBuilderProvider().getMetricSpecifier(MetricFunction.MAX);
    return bldr.of(of);
  }

  /**
   * Specifies last metric function.
   *
   * @param of attribute to aggregate
   * @return an extensible specifier that allows further specification of optional properties of the
   *     metric
   */
  static MetricFinalSpecifier last(String of) {
    Metric.Builder bldr =
        ISqlBuilderProvider.getBuilderProvider().getMetricSpecifier(MetricFunction.LAST);
    return bldr.of(of);
  }

  interface MetricFinalSpecifier {
    /**
     * Optional alias for the metric.
     *
     * @param as alias name
     * @return specifier for further specification, if any
     */
    MetricFinalSpecifier as(String as);
  }

  interface Builder extends MetricFinalSpecifier {
    /**
     * Records mandatory attribute name for the metric aggregation.
     *
     * @param of name of the attribute
     * @return specifier for further specification
     */
    MetricFinalSpecifier of(String of);
  }
}
