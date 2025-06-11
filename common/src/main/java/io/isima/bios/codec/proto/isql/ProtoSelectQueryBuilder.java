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

import io.isima.bios.codec.proto.wrappers.ProtoSelectQuery;
import io.isima.bios.common.Constants;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.ISqlStatement.ContextSelectBuilder;
import io.isima.bios.models.isql.ISqlStatement.SelectBuilder;
import io.isima.bios.models.isql.ISqlStatement.SnappedTimeRangeSelectBuilder;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.proto.DataProto;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Builds a protobuf based Select Query object. */
public class ProtoSelectQueryBuilder
    implements SelectBuilder,
        SnappedTimeRangeSelectBuilder,
        SelectStatement.FromContext<
            SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>,
        SelectStatement.LinkedBuilder<SelectBuilder, SnappedTimeRangeSelectBuilder> {
  private final DataProto.SelectQuery.Builder queryBuilder;
  private final DataProto.Dimensions.Builder dimensionsBuilder;
  private final DataProto.Window.Builder windowBuilder;
  private final BasicBuilderValidator validator;

  /**
   * Protobuf based query builder as the query parameters are directly filled into the protobuf
   * query message object.
   *
   * @param validator Validator methods
   * @param attributes Attributes
   * @param metrics Metrics
   */
  public ProtoSelectQueryBuilder(
      BasicBuilderValidator validator,
      List<String> attributes,
      List<Metric.MetricFinalSpecifier> metrics) {
    this.queryBuilder = DataProto.SelectQuery.newBuilder();
    this.dimensionsBuilder = DataProto.Dimensions.newBuilder();
    this.windowBuilder = DataProto.Window.newBuilder();
    this.validator = validator;

    if (attributes != null) {
      this.queryBuilder.setAttributes(
          DataProto.AttributeList.newBuilder().addAllAttributes(attributes));
    }
    if (metrics != null) {
      this.queryBuilder.addAllMetrics(
          metrics.stream()
              .map(x -> ((ProtoMetricBuilder) x).buildProto())
              .collect(Collectors.toList()));

      // Tell the server that extra attributes aren't necessary
      if (!this.queryBuilder.hasAttributes()) {
        this.queryBuilder.setAttributes(DataProto.AttributeList.newBuilder());
      }
    }
  }

  @Override
  public ContextSelectBuilder fromContext(String fromContext) {
    final var numMetrics = this.queryBuilder.getMetricsCount();
    if (this.queryBuilder.hasAttributes() || numMetrics > 0) {
      if (numMetrics > 0) {
        if (this.queryBuilder.hasAttributes()
            && !this.queryBuilder.getAttributes().getAttributesList().isEmpty()) {
          throw new IllegalArgumentException(
              "Aggregation queries not supported for context select except 'count(*)'");
        }
        final var metric = this.queryBuilder.getMetrics(0);
        if (numMetrics > 1 || !metric.getFunction().equals(DataProto.MetricFunction.COUNT)) {
          throw new IllegalArgumentException("Only count() metric supported for context query");
        }
        return new DefaultContextSelectBuilder(
            validator, fromContext, Collections.singletonList(Constants.COUNT_METRIC));
      } else {
        return new DefaultContextSelectBuilder(
            validator, fromContext, this.queryBuilder.getAttributes().getAttributesList());
      }
    } else {
      return new DefaultContextSelectBuilder(validator, fromContext);
    }
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      from(String from) {
    validator.validateStringParam(from, "from");
    this.queryBuilder.setFrom(from);
    return this;
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      distinct() {
    this.queryBuilder.setDistinct(true);
    return this;
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      where(String where) {
    validator.validateStringParam(where, "where");
    this.queryBuilder.setWhere(where);
    return this;
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      groupBy(String... attributes) {
    validator.validateStringArray(attributes, "attributes");
    for (String attribute : attributes) {
      this.dimensionsBuilder.addDimensions(attribute);
    }
    this.queryBuilder.setGroupBy(this.dimensionsBuilder.build());
    this.dimensionsBuilder.clear();
    return this;
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      orderBy(OrderBy.OrderByFinalSpecifier specifier) {
    validator.validateObject(specifier, "orderBy");
    if (specifier instanceof ProtoOrderByBuilder) {
      ProtoOrderByBuilder concreteBuilder = (ProtoOrderByBuilder) specifier;
      this.queryBuilder.setOrderBy(concreteBuilder.buildProto());
    }
    return this;
  }

  @Override
  public SelectStatement.QueryCondition<
          ISqlStatement.SelectBuilder, ISqlStatement.SnappedTimeRangeSelectBuilder>
      limit(int limit) {
    validator.validatePositive(limit, "limit");
    this.queryBuilder.setLimit(limit);
    return this;
  }

  @Override
  public SelectStatement.SnappedTimeRange<SnappedTimeRangeSelectBuilder> window(
      Window.WindowFinalSpecifier<?> specifier) {
    validator.validateObject(specifier, "window");
    if (specifier instanceof TumblingWindow.WindowFinalSpecifier) {
      ProtoTumblingWindowBuilder concreteBuilder = (ProtoTumblingWindowBuilder) specifier;
      this.queryBuilder.addWindows(
          windowBuilder
              .setWindowType(DataProto.WindowType.TUMBLING_WINDOW)
              .setTumbling(concreteBuilder.buildProto()));
    } else if (specifier instanceof SlidingWindow.WindowFinalSpecifier) {
      ProtoSlidingWindowBuilder concreteBuilder = (ProtoSlidingWindowBuilder) specifier;
      this.queryBuilder.addWindows(
          windowBuilder
              .setWindowType(DataProto.WindowType.SLIDING_WINDOW)
              .setSliding(concreteBuilder.buildProto()));
    }
    this.windowBuilder.clear();
    return this;
  }

  @Override
  public SelectBuilder timeRange(long originMillis, long deltaMillis) {
    validator.validateTimeRange(originMillis, deltaMillis);
    if (deltaMillis > 0) {
      this.queryBuilder.setStartTime(originMillis);
      this.queryBuilder.setEndTime(originMillis + deltaMillis);
    } else {
      this.queryBuilder.setEndTime(originMillis);
      this.queryBuilder.setStartTime(originMillis + deltaMillis);
    }
    return this;
  }

  @Override
  public SnappedTimeRangeSelectBuilder snappedTimeRange(long originTime, long delta) {
    setSnappedTimeRange(originTime, delta, 0);
    return this;
  }

  @Override
  public SnappedTimeRangeSelectBuilder snappedTimeRange(OffsetDateTime originTime, Duration delta) {
    setSnappedTimeRange(originTime, delta, null);
    return this;
  }

  @Override
  public SnappedTimeRangeSelectBuilder snappedTimeRange(
      long originTime, long delta, long snapStepSize) {
    setSnappedTimeRange(originTime, delta, snapStepSize);
    return this;
  }

  @Override
  public SnappedTimeRangeSelectBuilder snappedTimeRange(
      OffsetDateTime originTime, Duration delta, Duration snapStepSize) {
    setSnappedTimeRange(originTime, delta, snapStepSize);
    return this;
  }

  @Override
  public SelectStatement build() {
    return new ProtoSelectQuery(queryBuilder.build());
  }

  @Override
  public SelectBuilder onTheFly() {
    enableOnTheFly();
    return this;
  }

  private void setTimeRange(OffsetDateTime originTime, Duration delta) {
    validator.validateObject(originTime, "originTime");
    validator.validateObject(delta, "delta");
    final long startTimeMillis = originTime.toInstant().toEpochMilli();
    final long deltaMillis = delta.toMillis();
    timeRange(startTimeMillis, deltaMillis);
  }

  private void setSnappedTimeRange(
      OffsetDateTime originTime, Duration delta, Duration snapStepSize) {
    validator.validateObject(originTime, "originTime");
    validator.validateObject(delta, "delta");
    final long originTimeMillis = originTime.toInstant().toEpochMilli();
    final long deltaMillis = delta.toMillis();
    setSnappedTimeRange(
        originTimeMillis, deltaMillis, snapStepSize != null ? snapStepSize.toMillis() : 0);
  }

  private void setSnappedTimeRange(long originMillis, long deltaMillis, long snapStepSize) {
    validator.validateTimeRange(originMillis, deltaMillis);
    if (snapStepSize <= 0) {
      final var window = queryBuilder.getWindowsList().get(0);
      switch (window.getWindowType()) {
        case TUMBLING_WINDOW:
          snapStepSize = window.getTumbling().getWindowSizeMs();
          break;
        case SLIDING_WINDOW:
          snapStepSize = window.getSliding().getSlideInterval();
          break;
        default:
          throw new IllegalArgumentException(
              "Tumbling window or sliding window must be set to specify a snapped time range");
      }
    }
    if (snapStepSize <= 0) {
      throw new IllegalArgumentException("Snap step size must be a positive integer");
    }
    long snappedOrigin = originMillis / snapStepSize * snapStepSize;
    timeRange(snappedOrigin, deltaMillis);
  }

  private void enableOnTheFly() {
    queryBuilder.setOnTheFly(true);
  }
}
