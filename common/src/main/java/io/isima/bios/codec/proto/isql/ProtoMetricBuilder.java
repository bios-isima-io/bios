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
package io.isima.bios.codec.proto.isql;

import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.proto.DataProto;

/** Builds an independent protobuf metric. */
public class ProtoMetricBuilder implements Metric.Builder {
  private final DataProto.Metric.Builder metricBuilder;
  private final BasicBuilderValidator validator;

  /** Protobuf Metric Builder. */
  public ProtoMetricBuilder(BasicBuilderValidator validator, MetricFunction function) {
    this.metricBuilder = DataProto.Metric.newBuilder();
    this.metricBuilder.setFunction(function.toProto());
    this.validator = validator;
  }

  @Override
  public Metric.MetricFinalSpecifier as(String as) {
    validator.validateStringParam(as, "metric::as");
    metricBuilder.setAs(as);
    return this;
  }

  @Override
  public Metric.MetricFinalSpecifier of(String of) {
    validator.validateStringParam(of, "metric::of");
    metricBuilder.setOf(of);
    return this;
  }

  DataProto.Metric buildProto() {
    return metricBuilder.build();
  }

  BasicBuilderValidator getValidator() {
    return this.validator;
  }
}
