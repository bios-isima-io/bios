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
package io.isima.bios.query;

import static io.isima.bios.common.Constants.ORDER_BY_TIMESTAMP;

import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.isql.DerivedQueryComponents;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.QueryConfig;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.isql.WindowType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates query per signal. The signal attribute descriptions are stored in an immutable map
 * during construction.
 */
public class TfosQueryValidator implements QueryValidator {
  private static final Logger logger = LoggerFactory.getLogger(TfosQueryValidator.class);

  private final Map<String, AttributeDesc> attributeDescMap;
  private final QueryConfig queryConfig;
  private final StreamType streamType;

  public TfosQueryValidator(StreamConfig streamConfig, QueryConfig config) {
    Objects.requireNonNull(streamConfig);
    Map<String, AttributeDesc> tmpMap = new HashMap<>();
    streamConfig.getAttributes().forEach((a) -> tmpMap.put(a.getName().toLowerCase(), a));
    if (streamConfig.getAdditionalAttributes() != null) {
      streamConfig
          .getAdditionalAttributes()
          .forEach((a) -> tmpMap.put(a.getName().toLowerCase(), a));
    }
    attributeDescMap = Collections.unmodifiableMap(tmpMap);
    queryConfig = config;
    streamType = streamConfig.getType();
  }

  @Override
  public void validate(SelectStatement query, int queryIndex, DerivedQueryComponents derived)
      throws InvalidValueValidatorException,
          ConstraintViolationValidatorException,
          NotImplementedValidatorException {
    final long startTime = query.getStartTime();
    final long endTime = query.getEndTime();

    if (startTime < 0) {
      throw new InvalidValueValidatorException(
          String.format("Query #%d: startTime (%d) cannot be negative", queryIndex, startTime));
    }

    if (startTime > endTime) {
      throw new ConstraintViolationValidatorException(
          String.format(
              "Query #%d : endTime (%d) cannot be earlier than startTime (%d)",
              queryIndex, endTime, startTime));
    }

    final Set<String> attributeKeys = new HashSet<>();
    attributeKeys.add(ORDER_BY_TIMESTAMP);
    final Set<String> domains = new HashSet<>();

    validateComplexQuery(query, queryIndex);
    validateAttributes(query, queryIndex, attributeKeys);
    validateMetrics(query, queryIndex, attributeKeys, domains, derived);
    validateGroupBy(query, queryIndex, attributeKeys, domains, derived);
    validateOrderBy(query, queryIndex, attributeKeys, derived);
    validateLimit(query, queryIndex);
    validateWindow(query, queryIndex, derived);
  }

  private void validateWindow(SelectStatement query, int queryIndex, DerivedQueryComponents derived)
      throws InvalidValueValidatorException,
          ConstraintViolationValidatorException,
          NotImplementedValidatorException {
    if (!query.hasWindow()) {
      return;
    }

    final List<? extends Metric> metrics = query.getMetrics();
    if (metrics == null || metrics.isEmpty()) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Windowed queries must provide a list of metrics to return.", queryIndex));
    }

    if (streamType != StreamType.SIGNAL && streamType != StreamType.METRICS) {
      throw new ConstraintViolationValidatorException(
          String.format(
              "Query #%d: Stream type %s does not support window operations",
              queryIndex, streamType.name()));
    }

    final long startTime = query.getStartTime();
    final long endTime = query.getEndTime();

    final Window<?> window = query.getOneWindow();
    long windowLengthMs;
    long interval;

    switch (window.getWindowType()) {
      case SLIDING_WINDOW:
        final SlidingWindow slidingWindow = (SlidingWindow) window;
        if (slidingWindow.getSlideIntervalMillis() <= 0) {
          throw new InvalidValueValidatorException(
              String.format(
                  "Query #%d: Slide Interval (%d) should be positive",
                  queryIndex, slidingWindow.getSlideIntervalMillis()));
        }
        if (slidingWindow.getNumberOfWindowSlides() <= 0) {
          throw new InvalidValueValidatorException(
              String.format(
                  "Query #%d: Number of slides (%d) must be positive",
                  queryIndex, slidingWindow.getNumberOfWindowSlides()));
        }
        interval = slidingWindow.getSlideIntervalMillis();
        windowLengthMs = interval * slidingWindow.getNumberOfWindowSlides();
        break;

      case TUMBLING_WINDOW:
        final TumblingWindow tumblingWindow = (TumblingWindow) window;
        if (tumblingWindow.getWindowSizeMillis() <= 0) {
          throw new InvalidValueValidatorException(
              String.format(
                  "Query #%d: Tumbling Interval (%d) should be positive",
                  queryIndex, tumblingWindow.getWindowSizeMillis()));
        }
        windowLengthMs = tumblingWindow.getWindowSizeMillis();
        interval = windowLengthMs;
        break;

      default:
        throw new InvalidValueValidatorException(
            String.format(
                "Query #%d: Unsupported window type (%s)",
                queryIndex, window.getWindowType().name()));
    }

    final TimeZone tz = TimeZone.getTimeZone("UTC");

    final int offset = tz.getRawOffset();
    final long earliestCheckpoint =
        (((startTime + offset + interval - 1) / interval) * interval) - offset;
    if (earliestCheckpoint >= endTime) {
      throw new ConstraintViolationValidatorException(
          String.format("Query #%d: Time range should cover at least one window", queryIndex));
    }

    if (earliestCheckpoint + interval - windowLengthMs < 0) {
      throw new ConstraintViolationValidatorException(
          String.format("Query #%d: First window out of time range boundary", queryIndex));
    }

    // check number of summarize points
    final long summarizePoints = (endTime - earliestCheckpoint + interval - 1) / interval;
    final long searchEndTime = (summarizePoints * interval) + earliestCheckpoint;
    if (queryConfig != null) {
      final int hlimit = queryConfig.windowHorizontalLimit();
      if (summarizePoints > hlimit) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: Number of window slides exceeds limitation of %s", queryIndex, hlimit));
      }
    }
    if (derived != null) {
      derived.setEndTime(searchEndTime);
    }
  }

  private void validateLimit(SelectStatement query, int queryIndex)
      throws InvalidValueValidatorException {
    if (query.hasLimit()) {
      if (query.getLimit() < 0) {
        throw new InvalidValueValidatorException(
            String.format(
                "Query #%d: Limit (%d) must be greater than or equal to zero",
                queryIndex, query.getLimit()));
      }
    }
  }

  private void validateOrderBy(
      SelectStatement query,
      int queryIndex,
      Set<String> attributeKeys,
      DerivedQueryComponents derived)
      throws InvalidValueValidatorException, ConstraintViolationValidatorException {
    final OrderBy orderBy = query.getOrderBy();
    if (orderBy == null) {
      return;
    }
    final String by = orderBy.getBy();
    if (by == null || by.isEmpty()) {
      throw new ConstraintViolationValidatorException(
          String.format("Query #%d: OrderBy attribute must be set", queryIndex));
    }

    try {
      final var sortRequest =
          CompiledSortRequest.compile(by, orderBy.isDescending(), orderBy.isCaseSensitive());
      if (derived != null) {
        derived.putComponent(DerivedQueryComponents.COMPILED_SORT, sortRequest);
      }

      final var attributeOrAlias = sortRequest.getAggregate().getBy();
      if (sortRequest.isKeyAttribute()) {
        final List<String> dimensions =
            query.getGroupBy() != null ? query.getGroupBy() : query.hasWindow() ? List.of() : null;
        if (dimensions != null
            && dimensions.stream()
                .noneMatch((dimension) -> dimension.equalsIgnoreCase(attributeOrAlias))) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: Order key attribute %s must present in group by clause",
                  queryIndex, attributeOrAlias));
        }
      }
      if (!attributeOrAlias.isBlank() && !ORDER_BY_TIMESTAMP.equalsIgnoreCase(attributeOrAlias)) {
        final AttributeDesc orderByKey = getAttributeDesc(attributeOrAlias);
        if (orderByKey == null) {
          if (sortRequest.isKeyAggregate()
              || query.getMetrics().stream()
                  .noneMatch((metric) -> attributeOrAlias.equalsIgnoreCase(metric.getAs()))) {
            throw new ConstraintViolationValidatorException(
                String.format(
                    "Query #%d: Order attribute %s does not exist", queryIndex, attributeOrAlias));
          }
        }
        if (orderByKey != null && !orderByKey.getAttributeType().isComparable()) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: Order attribute %s (%s) must be comparable for order by clause",
                  queryIndex, by, orderByKey.getAttributeType().name()));
        }
      }
    } catch (CompiledSortRequest.CompilationException e) {
      throw new InvalidValueValidatorException(
          String.format("Query #%d: Invalid order key: %s", queryIndex, e.getMessage()));
    }
  }

  private void validateGroupBy(
      SelectStatement query,
      int queryIndex,
      Set<String> attributeKeys,
      Set<String> domains,
      DerivedQueryComponents derived)
      throws InvalidValueValidatorException, ConstraintViolationValidatorException {
    final List<String> groupBy = query.getGroupBy();
    final List<String> attributes = query.getSelectAttributes();
    List<String> metrics = new ArrayList<String>();
    if (query.getMetrics() != null) {
      query.getMetrics().forEach((n) -> metrics.add(n.getOutputAttributeName().toLowerCase()));
    }
    if (groupBy == null || groupBy.isEmpty()) {
      if ((attributes != null && !attributes.isEmpty())
          && (metrics != null && !metrics.isEmpty())) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: Cannot select non-GroupBy attribute(s) along with metric(s)",
                queryIndex));
      }
    } else {
      // TODO(BIOS-4981): Enable ME
      // if ((attributes == null || attributes.isEmpty())
      //     && (metrics == null || metrics.isEmpty())) {
      //   throw new ConstraintViolationValidatorException(String
      //       .format(
      //           "Query #%d: Attribute/aggregate(s) must be specified" +
      //           " when group by clause is used",
      //           queryIndex));
      // }
      // verify dimensions
      final var dimensions = new HashSet<String>();
      int index = 0;
      for (String dimension : groupBy) {
        final AttributeDesc desc = getAttributeDesc(dimension);
        if (desc == null) {
          throw new InvalidValueValidatorException(
              String.format(
                  "Query #%d: GroupBy[%d]: Dimension must be one defined in a feature: %s",
                  queryIndex, index, dimension));
        }
        final String key = desc.getName();
        if (dimensions.contains(key.toLowerCase())) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: GroupBy[%d]: Duplicate attribute: %s", queryIndex, index, dimension));
        }
        dimensions.add(key.toLowerCase());
        if (domains.contains(key.toLowerCase())) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: GroupBy[%d]: Aggregating attribute cannot be grouped: %s",
                  queryIndex, index, dimension));
        }
        if (derived != null) {
          derived.addAttributeKey(dimension, key);
        }
        // TODO(BIOS-4981): remove this line
        attributeKeys.add(key.toLowerCase());
        index++;
      }

      // TODO(BIOS-4981) - remove if condition, keep for block
      if (attributes != null && !attributes.isEmpty()) {
        for (String attr : attributes) {
          if (!dimensions.contains(attr.toLowerCase())) {
            throw new ConstraintViolationValidatorException(
                String.format(
                    "Query #%d: attribute: %s is not part of group by clause", queryIndex, attr));
          }
        }
      }
    }
  }

  private void validateMetrics(
      SelectStatement query,
      int queryIndex,
      Set<String> attributeKeys,
      Set<String> domains,
      DerivedQueryComponents derived)
      throws InvalidValueValidatorException, ConstraintViolationValidatorException {
    final List<? extends Metric> metrics = query.getMetrics();
    if (metrics == null || metrics.isEmpty()) {
      return;
    }
    int idx = 0;
    MetricFunction sketchFunction = null;
    for (Metric metric : metrics) {
      final var function = metric.getFunction();
      if (function.requiresSketch()) {
        sketchFunction = function;
      }
      if (function.requiresAttribute() && (metric.getOf() == null)) {
        throw new InvalidValueValidatorException(
            String.format("Query #%d: Function '%s' is missing attribute", queryIndex, function));
      }
      if (metric.getOf() != null) {
        final AttributeDesc desc = getAttributeDesc(metric.getOf());
        if (desc == null) {
          throw new InvalidValueValidatorException(
              String.format("Query #%d: Non existing attribute '%s'", queryIndex, metric.getOf()));
        }
        domains.add(desc.getName().toLowerCase());
        if (derived != null) {
          derived.addAttributeKey(metric.getOf(), desc.getName());
        }
        if (function == MetricFunction.SUM) {
          if (!desc.getAttributeType().isAddable()) {
            throw new ConstraintViolationValidatorException(
                String.format(
                    "Query #%d: Attribute '%s' of type %s for SUM metric is not addable",
                    queryIndex, metric.getOf(), desc.getAttributeType().name()));
          }
        } else if (function != MetricFunction.LAST) {
          if (!desc.getAttributeType().isComparable()) {
            throw new ConstraintViolationValidatorException(
                String.format(
                    "Query #%d: Attribute '%s' of type %s for %s metric is not comparable",
                    queryIndex, metric.getOf(), desc.getAttributeType().name(), function.name()));
          }
        }
      }

      final String key = metric.getOutputAttributeName();
      if (attributeKeys.contains(key.toLowerCase())) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: metrics[%d]: Duplicate attribute key; metric=%s",
                queryIndex, idx, metric));
      }
      if (attributeDescMap.get(key) != null) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: metrics[%d]: Metric attribute key conflicts with an existing attribute;"
                    + " metric=%s",
                queryIndex, idx, metric));
      }
      attributeKeys.add(key.toLowerCase());
    }
    if (sketchFunction != null) {
      logger.debug("sketchFunction {}", sketchFunction.toString());
      if (!query.hasWindow()) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: Function %s requires a windowed query e.g. tumbling window",
                queryIndex, sketchFunction.name()));
      }
      return;
    }
  }

  private void validateAttributes(SelectStatement query, int queryIndex, Set<String> attributeKeys)
      throws ConstraintViolationValidatorException {
    final List<String> attributes = query.getSelectAttributes();
    if (attributes == null || attributes.isEmpty()) {
      if (query.isDistinct()) {
        throw new ConstraintViolationValidatorException(
            String.format(
                "Query #%d: Attribute must be specified when distinct clause is used", queryIndex));
      }
      if (query.getMetrics() == null || query.getMetrics().isEmpty()) {
        // means all attributes
        attributeKeys.addAll(attributeDescMap.keySet());
      }
    } else {
      List<String> groupBy = query.getGroupBy();
      for (String attribute : attributes) {
        if (attributeKeys.contains(attribute)) {
          throw new ConstraintViolationValidatorException(
              String.format("Query #%d: Duplicate attribute: %s", queryIndex, attribute));
        }

        if (query.hasWindow() && (groupBy != null) && (!groupBy.isEmpty())) {
          if (!groupBy.contains(attribute)) {
            throw new ConstraintViolationValidatorException(
                String.format(
                    "Query #%d: output attributes not allowed in projection in window queries",
                    queryIndex));
          }
        }

        if (getAttributeDesc(attribute) == null) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: Attribute '%s' is not in signal attributes", queryIndex, attribute));
        } else {
          attributeKeys.add(attribute.toLowerCase());
        }
      }
      if (query.isDistinct()) {
        // Distinct works with only single attribute since its implemented as group by internally
        // single attribute has be explicitly specified
        if (attributes.size() > 1) {
          throw new ConstraintViolationValidatorException(
              String.format(
                  "Query #%d: Only one attribute allowed with distinct clause", queryIndex));
        }
      }
    }
  }

  private void validateComplexQuery(SelectStatement query, int queryIndex)
      throws NotImplementedValidatorException {
    if (query.getResponseShape() != ResponseShape.MULTIPLE_RESULT_SETS) {
      // Not a complex query.
      return;
    }
    if (query.isDistinct()) {
      throw new NotImplementedValidatorException(
          String.format("Query #%d: Distinct is not supported in complex query.", queryIndex));
    }
    if (query.getWhere() != null) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Where is not supported in complex query; got [%s].",
              queryIndex, query.getWhere()));
    }
    if (query.getGroupBy() != null) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Group by is not supported in complex query; got [%s].",
              queryIndex, query.getGroupBy().toString()));
    }
    if (query.getOrderBy() != null) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Order by is not supported in complex query; got [%s].",
              queryIndex, query.getOrderBy().toString()));
    }
    if (query.getLimit() != 0) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Limit is not supported in complex query; got [%d].",
              queryIndex, query.getLimit()));
    }
    if (!query.hasWindow()) {
      throw new NotImplementedValidatorException(
          String.format("Query #%d: Complex query without a window is not supported.", queryIndex));
    }
    if (query.getOneWindow().getWindowType() != WindowType.TUMBLING_WINDOW) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Complex query does not support window type [%s].",
              queryIndex, query.getOneWindow().getWindowType().name()));
    }
    if (query.getMetrics().size() > 1) {
      throw new NotImplementedValidatorException(
          String.format(
              "Query #%d: Multiple metrics are not supported in complex query, got [%d].",
              queryIndex, query.getMetrics().size()));
    }
  }

  private AttributeDesc getAttributeDesc(String attribute) {
    return attributeDescMap.get(attribute.toLowerCase());
  }
}
