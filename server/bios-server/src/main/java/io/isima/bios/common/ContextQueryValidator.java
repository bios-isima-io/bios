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
package io.isima.bios.common;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SelectOrder;
import io.isima.bios.errors.exception.FilterSyntaxException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.InvalidValueValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.models.Sort;
import io.isima.bios.models.v1.AttributeDesc;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextQueryValidator {
  private static final Logger logger = LoggerFactory.getLogger(ContextQueryValidator.class);

  private final Map<String, AttributeDesc> attributeDescMap;

  public ContextQueryValidator(StreamDesc contextDesc) {
    Objects.requireNonNull(contextDesc);
    Map<String, AttributeDesc> tmpMap = new HashMap<>();
    contextDesc.getAttributes().forEach((a) -> tmpMap.put(a.getName().toLowerCase(), a));
    if (contextDesc.getAdditionalAttributes() != null) {
      contextDesc
          .getAdditionalAttributes()
          .forEach((a) -> tmpMap.put(a.getName().toLowerCase(), a));
    }
    attributeDescMap = Collections.unmodifiableMap(tmpMap);
  }

  public void validate(SelectContextRequest request, ContextQueryState queryState)
      throws InvalidValueValidatorException,
          ConstraintViolationValidatorException,
          NotImplementedValidatorException,
          ApplicationException {
    Objects.requireNonNull(request);

    final var dimensions = new HashSet<String>();
    if (request.getGroupBy() != null) {
      for (var dimensionWithCase : request.getGroupBy()) {
        final var dimension = dimensionWithCase.toLowerCase();
        if (!attributeDescMap.containsKey(dimension)) {
          throw new InvalidValueValidatorException(
              "GroupBy attribute %s not found", dimensionWithCase);
        }
        dimensions.add(dimension);
      }
    }

    final var metricColumns = new HashMap<String, String>();
    boolean hasMetrics = false;
    if (request.getMetrics() != null) {
      for (var metric : request.getMetrics()) {
        final var domain = metric.getOf();
        if (domain != null) {
          final var attrDesc = attributeDescMap.get(domain.toLowerCase());
          if (attrDesc == null) {
            throw new InvalidValueValidatorException(
                "Invalid attribute %s for metric %s(%s)",
                domain, metric.getFunction(), metric.getOf());
          }
          // fix possible case inconsistency
          metric.setOf(attrDesc.getName());
        }
        if (metric.getAs() != null && metric.getAs().isBlank()) {
          throw new InvalidValueValidatorException("Metric alias must not be blank");
        }
        hasMetrics = true;
        final var outName = metric.getOutputAttributeName();
        metricColumns.put(outName.toLowerCase(), outName);
      }
    }

    if (request.getAttributes() != null) {
      for (var attributeWithCase : request.getAttributes()) {
        final var attribute = attributeWithCase.toLowerCase();
        if (!attributeDescMap.containsKey(attribute)) {
          throw new InvalidValueValidatorException("Attribute %s not found", attributeWithCase);
        }
        if ((hasMetrics || !dimensions.isEmpty()) && !dimensions.contains(attribute)) {
          throw new ConstraintViolationValidatorException(
              "Attribute must be a part of group keys when a metric is selected; attribute=%s",
              attributeWithCase);
        }
      }
    }

    final Integer limit = request.getLimit();
    if (limit != null) {
      if (limit < 0) {
        throw new InvalidValueValidatorException("Limit can't be negative");
      }
    }

    final SelectOrder orderBy = request.getOrderBy();
    if (orderBy != null) {
      if ((orderBy.getKeyName() == null) || (orderBy.getKeyName().isEmpty())) {
        throw new InvalidValueValidatorException("Order attribute must not be empty");
      }
      final var normalizedKey = orderBy.getKeyName().toLowerCase();
      final var attrDesc = attributeDescMap.get(normalizedKey);
      Sort sortSpec;
      if (attrDesc == null) {
        final var metricColumn = metricColumns.get(normalizedKey);
        if (metricColumn == null) {
          throw new InvalidValueValidatorException(
              "Order attribute %s not found", orderBy.getKeyName());
        } else {
          sortSpec = new Sort(metricColumn, orderBy.isReverse());
          sortSpec.setCaseSensitive(orderBy.isCaseSensitive());
        }
      } else {
        if (!attrDesc.getAttributeType().isComparable()) {
          throw new InvalidValueValidatorException(
              "Order attribute %s must be comparable for order by clause", orderBy.getKeyName());
        }
        sortSpec = new Sort(attrDesc.getName(), orderBy.isReverse());
        sortSpec.setCaseSensitive(orderBy.isCaseSensitive());
      }
      queryState.setSortSpec(sortSpec);
    }
    String filter = request.getWhere();
    if (filter != null && !filter.isEmpty()) {
      try {
        queryState.setFilter(CassStream.parseFilter(filter).stream().collect(Collectors.toList()));
      } catch (FilterSyntaxException | InvalidFilterException e) {
        throw new InvalidValueValidatorException(
            "Syntax error in where clause: %s", e.getMessage());
      }
    } else {
      queryState.setFilter(List.of());
    }
  }
}
