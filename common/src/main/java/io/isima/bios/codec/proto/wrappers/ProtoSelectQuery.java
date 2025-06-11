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
package io.isima.bios.codec.proto.wrappers;

import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.isql.WindowType;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ProtoSelectQuery implements SelectStatement {
  private final DataProto.SelectQuery query;
  private final ProtoOrderBy orderBy;
  private final List<ProtoMetric> metrics;
  // currently only support a single window
  private final Window<?> window;

  public ProtoSelectQuery(DataProto.SelectQuery query) {
    this.query = query;
    if (query.hasOrderBy()) {
      this.orderBy = new ProtoOrderBy(query.getOrderBy());
    } else {
      this.orderBy = null;
    }
    metrics = query.getMetricsList().stream().map(ProtoMetric::new).collect(Collectors.toList());
    if (query.getWindowsCount() > 0) {
      // currently only one window supported;
      // assumed to be taken care in client/server side validation and api building
      window = windowFn.apply(query.getWindows(0));
    } else {
      window = null;
    }
  }

  @Override
  public long getStartTime() {
    return query.getStartTime();
  }

  @Override
  public long getEndTime() {
    return query.getEndTime();
  }

  @Override
  public boolean isDistinct() {
    return query.getDistinct();
  }

  @Override
  public List<String> getSelectAttributes() {
    return query.hasAttributes() ? query.getAttributes().getAttributesList() : null;
  }

  @Override
  public List<? extends Metric> getMetrics() {
    return metrics.isEmpty() ? null : metrics;
  }

  @Override
  public String getFrom() {
    return query.getFrom();
  }

  @Override
  public String getWhere() {
    return query.getWhere().isEmpty() ? null : query.getWhere();
  }

  @Override
  public List<String> getGroupBy() {
    return query.hasGroupBy() ? query.getGroupBy().getDimensionsList() : null;
  }

  @Override
  public OrderBy getOrderBy() {
    return orderBy;
  }

  @Override
  public Window<?> getOneWindow() {
    return window;
  }

  @Override
  public <W extends Window<W>> Window<W> getOneWindow(Class<W> clazz) {
    if (window != null) {
      if (clazz.isAssignableFrom(window.getClass())) {
        return (Window<W>) window;
      } else {
        throw new IllegalArgumentException(
            String.format("Wrong Window type. Expected is %s", window.getWindowType()));
      }
    }
    return null;
  }

  @Override
  public boolean hasLimit() {
    return query.hasLimit();
  }

  @Override
  public int getLimit() {
    return query.getLimit();
  }

  @Override
  public boolean hasWindow() {
    return window != null;
  }

  @Override
  public boolean hasOrderBy() {
    return query.hasOrderBy();
  }

  @Override
  public boolean isOnTheFly() {
    return query.getOnTheFly();
  }

  @Override
  public ResponseShape getResponseShape() {
    for (final var metric : metrics) {
      if (metric.getFunction().getResponseShape() == ResponseShape.MULTIPLE_RESULT_SETS) {
        return ResponseShape.MULTIPLE_RESULT_SETS;
      }
    }
    return ResponseShape.RESULT_SET;
  }

  public DataProto.SelectQuery toProto() {
    return query;
  }

  /**
   * Following proto wrapper classes are deliberately made private inner classes to hide this
   * implementation as it is expected that clients of these classes access and drills down through
   * the {@link SelectStatement} interface.
   *
   * <p>These classes are 'static' inner classes by design as these classes has no reason to access
   * parent class data members, making it easier in the future to make these implementation public
   * if necessary.
   */
  private static final class ProtoMetric implements Metric {
    private final DataProto.Metric metric;

    private volatile String outputName = null;

    private ProtoMetric(DataProto.Metric metric) {
      this.metric = metric;
    }

    @Override
    public MetricFunction getFunction() {
      return MetricFunction.fromProto(metric.getFunction());
    }

    @Override
    public String getOf() {
      return metric.getOf().isEmpty() ? null : metric.getOf();
    }

    @Override
    public String getAs() {
      return metric.getAs().isEmpty() ? null : metric.getAs();
    }

    @Override
    public String getOutputAttributeName() {
      if (outputName == null) {
        this.outputName = metric.getAs().isEmpty() ? formattedWithOf(getOf()) : metric.getAs();
      }
      return outputName;
    }

    private String formattedWithOf(String of) {
      return String.format(
          "%s(%s)", metric.getFunction().name().toLowerCase(), (of == null) ? "" : of);
    }

    @Override
    public String toString() {
      final var sb = new StringBuilder(formattedWithOf(getOf()));
      if (getAs() != null) {
        sb.append(" AS ").append(getAs());
      }
      return sb.toString();
    }
  }

  private static final class ProtoOrderBy implements OrderBy {
    private final DataProto.OrderBy orderBy;

    private ProtoOrderBy(DataProto.OrderBy orderBy) {
      this.orderBy = orderBy;
    }

    @Override
    public String getBy() {
      return orderBy.getBy();
    }

    @Override
    public boolean isDescending() {
      return orderBy.getReverse();
    }

    @Override
    public boolean isCaseSensitive() {
      return orderBy.getCaseSensitive();
    }

    @Override
    public String toString() {
      final var by = getBy();
      final var ascDesc = isDescending() ? " DESC" : "";
      final var ignoreCase = isCaseSensitive() ? " CASE INSENSITIVE" : "";
      return String.format("%s%s%s", by, ascDesc, ignoreCase);
    }
  }

  private static final Function<DataProto.Window, Window<?>> windowFn =
      (window) -> {
        switch (window.getWindowType()) {
          case SLIDING_WINDOW:
            final DataProto.SlidingWindow sliding = window.getSliding();
            return new SlidingWindow() {
              @Override
              public long getSlideIntervalMillis() {
                return sliding.getSlideInterval();
              }

              @Override
              public int getNumberOfWindowSlides() {
                return window.getSliding().getWindowSlides();
              }

              @Override
              public WindowType getWindowType() {
                return WindowType.SLIDING_WINDOW;
              }

              @Override
              public SlidingWindow getWindow() {
                return this;
              }

              @Override
              public String toString() {
                return String.format(
                    "SLIDING WINDOW %d SLIDES %d",
                    getSlideIntervalMillis() * getNumberOfWindowSlides(), getSlideIntervalMillis());
              }
            };

          case TUMBLING_WINDOW:
            final DataProto.TumblingWindow tumbling = window.getTumbling();
            return new TumblingWindow() {
              @Override
              public long getWindowSizeMillis() {
                return tumbling.getWindowSizeMs();
              }

              @Override
              public WindowType getWindowType() {
                return WindowType.TUMBLING_WINDOW;
              }

              @Override
              public TumblingWindow getWindow() {
                return this;
              }

              @Override
              public String toString() {
                return String.format("TUMBLING WINDOW %d", getWindowSizeMillis());
              }
            };

          default:
            throw new RuntimeException("Unknown window type");
        }
      };

  @Override
  public String toString() {
    final var sb = new StringBuilder("\"SELECT ");
    if (isDistinct()) {
      sb.append(" DISTINCT ");
    }
    final var select = new ArrayList<String>();
    final var selectAttributes = getSelectAttributes();
    if (selectAttributes != null) {
      selectAttributes.forEach((attribute) -> select.add(attribute));
    }
    final var metrics = getMetrics();
    if (metrics != null) {
      metrics.forEach((metric) -> select.add(metric.toString()));
    }
    sb.append(String.join(", ", select));
    sb.append(" FROM ").append(getFrom());
    if (getWhere() != null) {
      sb.append(" WHERE ").append(getWhere());
    }
    if (getGroupBy() != null) {
      sb.append(" GROUP BY ").append(String.join(", ", getGroupBy()));
    }
    if (hasOrderBy()) {
      sb.append(" ORDER BY ").append(getOrderBy());
    }
    if (hasLimit()) {
      sb.append(" LIMIT ").append(getLimit());
    }
    if (hasWindow()) {
      sb.append(" ").append(getOneWindow());
    }
    sb.append(" SINCE ")
        .append(
            String.format("%s (%d)", StringUtils.tsToIso8601Millis(getStartTime()), getStartTime()))
        .append(" UNTIL ")
        .append(
            String.format("%s (%d)", StringUtils.tsToIso8601Millis(getEndTime()), getEndTime()));
    if (isOnTheFly()) {
      sb.append(" ON THE FLY");
    }
    sb.append("\"");
    return sb.toString();
  }
}
