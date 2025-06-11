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
package io.isima.bios.data.impl;

import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.req.Aggregate;
import io.isima.bios.req.ExtractRequest;
import io.isima.bios.req.MutableAggregate;
import io.isima.bios.req.View;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Extract request adaptation to use inside extract engine on the server side.
 *
 * <p>In the future this adapter class may be removed when the extract and summarize engine natively
 * support new style select queries.
 */
public class ExtractRequestAdapter implements ExtractRequest {
  private final SelectStatement adaptee;
  private final List<AggregateAdapter> aggregates;
  private final List<View> views;
  private final DerivedQueryComponents derived;
  private final BiosVersion clientVersion;

  private static final BiosVersion VERSION_1_0_50 = new BiosVersion(1, 0, 50, false);

  public ExtractRequestAdapter(
      SelectStatement query, DerivedQueryComponents derived, BiosVersion clientVersion) {
    this.adaptee = query;
    this.derived = derived;
    this.clientVersion = clientVersion;
    List<? extends Metric> metrics = query.getMetrics();
    if (metrics == null) {
      aggregates = null;
    } else {
      aggregates = metrics.stream().map(AggregateAdapter::new).collect(Collectors.toList());
    }

    final var temp = new ArrayList<View>();
    if (query.getGroupBy() != null) {
      temp.add(new ViewAdapterGroupBy(query.getGroupBy(), null));
    }
    if (query.hasOrderBy()) {
      temp.add(new ViewAdapterOrderBy(query.getOrderBy()));
    }
    views = Collections.unmodifiableList(temp);
  }

  @Override
  public Long getStartTime() {
    return this.adaptee.getStartTime();
  }

  @Override
  public Long getEndTime() {
    return this.adaptee.getEndTime();
  }

  @Override
  public List<String> getAttributes() {
    return this.adaptee.getSelectAttributes();
  }

  @Override
  public List<? extends Aggregate> getAggregates() {
    return this.aggregates;
  }

  @Override
  public List<View> getViews() {
    return views;
  }

  @Override
  public Long getStreamVersion() {
    return null;
  }

  @Override
  public String getFilter() {
    return adaptee.getWhere();
  }

  @Override
  public Integer getLimit() {
    if (clientVersion.compareTo(VERSION_1_0_50) > 0) {
      if (adaptee.hasLimit()) {
        return adaptee.getLimit();
      }
    } else {
      // On older client code, only populate limit if it is non-zero.
      if (adaptee.getLimit() != 0) {
        return adaptee.getLimit();
      }
    }

    return null;
  }

  @Override
  public boolean isOnTheFly() {
    return adaptee.isOnTheFly();
  }

  private final class AggregateAdapter implements MutableAggregate {
    private final MetricFunction fn;
    private final Metric metric;

    private volatile String by;
    private volatile String outputName;

    private AggregateAdapter(Metric metric) {
      this.fn = metric.getFunction();
      this.metric = metric;
      String derivedOf = derived.getMappedKey(this.metric.getOf());
      if (derivedOf != null) {
        this.by = derivedOf;
      }
    }

    @Override
    public void setBy(String by) {
      if (by != null && !by.isEmpty()) {
        this.by = by;
        // by is changed on the server; output name may have to be recomputed
        this.outputName = null;
      }
    }

    @Override
    public MetricFunction getFunction() {
      return fn;
    }

    @Override
    public String getBy() {
      if (by != null) {
        return by;
      }
      return this.metric.getOf();
    }

    @Override
    public String getAs() {
      return this.metric.getAs();
    }

    @Override
    public String getOutputAttributeName() {
      if (outputName == null) {
        this.outputName =
            this.metric.getAs() != null ? this.metric.getAs() : formattedWithBy(getBy());
      }
      return outputName;
    }

    private String formattedWithBy(String by) {
      return String.format("%s(%s)", fn.name().toLowerCase(), (by == null) ? "" : by);
    }
  }

  private static final class ViewAdapterOrderBy implements View {
    private final OrderBy orderBy;

    private ViewAdapterOrderBy(OrderBy orderBy) {
      this.orderBy = orderBy;
    }

    @Override
    public ViewFunction getFunction() {
      return ViewFunction.SORT;
    }

    @Override
    public String getBy() {
      return orderBy.getBy();
    }

    @Override
    public List<String> getDimensions() {
      return null;
    }

    @Override
    public Boolean getReverse() {
      return orderBy.isDescending();
    }

    @Override
    public Boolean getCaseSensitive() {
      return orderBy.isCaseSensitive();
    }
  }

  private static final class ViewAdapterGroupBy implements View {
    private final List<String> dimensions;
    private final String viewAttr;

    private ViewAdapterGroupBy(List<String> groupBy, String viewAttr) {
      this.dimensions = groupBy;
      this.viewAttr = viewAttr;
    }

    @Override
    public ViewFunction getFunction() {
      return ViewFunction.GROUP;
    }

    @Override
    public String getBy() {
      return viewAttr;
    }

    @Override
    public List<String> getDimensions() {
      return dimensions;
    }

    @Override
    public Boolean getReverse() {
      return false;
    }

    @Override
    public Boolean getCaseSensitive() {
      return false;
    }
  }
}
