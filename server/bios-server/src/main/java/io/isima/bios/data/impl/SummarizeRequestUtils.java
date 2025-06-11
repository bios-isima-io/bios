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

import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.ComplexQueryRequest;
import io.isima.bios.models.Count;
import io.isima.bios.models.Last;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Sort;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.v1.Aggregate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Adaptation of Summarize request. */
public class SummarizeRequestUtils {

  private static final BiosVersion VERSION_1_0_50 = new BiosVersion(1, 0, 50, false);

  public static SummarizeRequest toSummarizeRequest(
      SelectStatement query, DerivedQueryComponents derived, BiosVersion clientVersion)
      throws InvalidValueException {
    SummarizeRequest req = new SummarizeRequest();

    populateRequest(query, derived, req, clientVersion);
    return req;
  }

  public static ComplexQueryRequest toComplexQueryRequest(
      SelectStatement query, DerivedQueryComponents derived, BiosVersion clientVersion)
      throws InvalidValueException {
    ComplexQueryRequest req = new ComplexQueryRequest();

    populateRequest(query, derived, req, clientVersion);
    return req;
  }

  private static void populateRequest(
      SelectStatement query,
      DerivedQueryComponents derived,
      SummarizeRequest req,
      BiosVersion clientVersion)
      throws InvalidValueException {
    req.setStartTime(query.getStartTime());
    req.setSnappedStartTime(query.getStartTime());
    req.setOrigEndTime(query.getEndTime());
    req.setEndTime(derived.getEndTime(query.getEndTime()));

    fillRequestBasedOnWindow(req, query.getOneWindow());

    String filter = query.getWhere();
    if (filter != null) {
      req.setFilter(filter);
    }

    if (clientVersion.compareTo(VERSION_1_0_50) > 0) {
      if (query.hasLimit()) {
        req.setLimit(query.getLimit());
      }
    } else {
      // On older client code, only populate limit if it is non-zero.
      if (query.getLimit() != 0) {
        req.setLimit(query.getLimit());
      }
    }

    if (query.hasOrderBy()) {
      OrderBy orderBy = query.getOrderBy();
      Sort sort = new Sort(orderBy.getBy());
      if (orderBy.isCaseSensitive()) {
        sort.setCaseSensitive(true);
      }
      if (orderBy.isDescending()) {
        sort.setReverse();
      }
      req.setSort(sort);
    }

    // underlying engine code requires mutability.. so create new lists and copy as protobuf
    if (query.getGroupBy() != null) {
      req.setGroup(
          query.getGroupBy().stream().map(derived::getMappedKey).collect(Collectors.toList()));
    }

    List<? extends Metric> metrics = query.getMetrics();
    if (metrics != null) {
      List<Aggregate> list = new ArrayList<>();
      for (Metric metric : metrics) {
        list.add(toAggregate(metric, derived));
      }
      req.setAggregates(list);
    }

    req.setOnTheFly(query.isOnTheFly());
  }

  private static void fillRequestBasedOnWindow(SummarizeRequest req, Window window)
      throws InvalidValueException {
    switch (window.getWindowType()) {
      case SLIDING_WINDOW:
        SlidingWindow sliding = (SlidingWindow) window;
        req.setInterval(sliding.getSlideIntervalMillis());
        req.setHorizon(sliding.getSlideIntervalMillis() * sliding.getNumberOfWindowSlides());
        break;

      case TUMBLING_WINDOW:
        TumblingWindow tumbling = (TumblingWindow) window;
        final long windowSize = tumbling.getWindowSizeMillis();
        req.setInterval(windowSize);
        req.setHorizon(windowSize);
        break;

      default:
        throw new InvalidValueException("Unsupported Window type" + window.getWindowType().name());
    }
  }

  private static Aggregate toAggregate(Metric metric, DerivedQueryComponents derived)
      throws InvalidValueException {
    Aggregate agg;
    switch (metric.getFunction()) {
      case SUM:
        agg = new Sum(derived.getMappedKey(metric.getOf()));
        break;
      case COUNT:
        agg = new Count();
        break;
      case MIN:
        agg = new Min(derived.getMappedKey(metric.getOf()));
        break;
      case MAX:
        agg = new Max(derived.getMappedKey(metric.getOf()));
        break;
      case LAST:
        agg = new Last(derived.getMappedKey(metric.getOf()));
        break;
      default:
        agg = new Aggregate(metric.getFunction(), derived.getMappedKey(metric.getOf()));
        break;
    }
    if (metric.getAs() != null && !metric.getAs().isEmpty()) {
      agg.setAs(metric.getAs());
    }
    return agg;
  }
}
