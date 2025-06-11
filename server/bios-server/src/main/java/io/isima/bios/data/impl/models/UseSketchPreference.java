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
package io.isima.bios.data.impl.models;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.data.impl.sketch.DataSketch;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.InternalAttributeType;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

/** Sketch selection preference indicator. */
@ToString
@Getter
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
public class UseSketchPreference {
  /** Sketch usage recommendation. */
  private boolean useSketch;

  /** Indicates whether sketches are required. */
  private boolean isSketchRequired;

  /** Indicates whether sketches are usable. */
  private boolean isSketchPossible;

  /** Explains why sketches are required. Used for debugging. */
  private String requiredReasons;

  /** Explains wny sketches cannot be used. Used for debugging. */
  private String notPossibleReasons;

  /** Decides the sketch preference for selecting a signal. */
  public static UseSketchPreference decide(
      SelectContextRequest request, ContextQueryState queryState, StreamDesc streamDesc)
      throws InvalidRequestException {
    final var metrics = request.getMetrics();
    if (metrics == null) {
      throw new IllegalArgumentException("request must contain metrics");
    }
    return decideInternal(
        queryState.getAggregates(),
        request.getGroupBy() != null ? request.getGroupBy() : List.of(),
        request.getOrderBy() != null ? request.getOrderBy().getKeyName() : null,
        request.getLimit(),
        request.getWhere(),
        null,
        null,
        streamDesc);
  }

  /** Decides the sketch preference for selecting a context. */
  public static UseSketchPreference decide(SummarizeRequest request, StreamDesc streamDesc)
      throws InvalidRequestException {
    return decideInternal(
        request.getAggregates(),
        request.getGroup(),
        request.getSort() != null ? request.getSort().getBy() : null,
        request.getLimit(),
        request.getFilter(),
        request.getInterval(),
        request.getHorizon(),
        streamDesc);
  }

  /** The common logic to decide sketch preference. */
  private static UseSketchPreference decideInternal(
      List<Aggregate> aggregates,
      List<String> groupBy,
      String orderBy,
      Number limit,
      String where,
      Long interval,
      Long horizon,
      StreamDesc streamDesc)
      throws InvalidRequestException {
    final UseSketchPreference preference = new UseSketchPreference();
    preference.isSketchRequired = false;
    final var requiredReasons = new StringJoiner("; ");
    for (final var aggregate : aggregates) {
      if (aggregate.getFunction().requiresSketch()) {
        preference.isSketchRequired = true;
        requiredReasons.add(String.format("Aggregate %s", aggregate.getFunction()));
      }
    }

    preference.isSketchPossible = true;
    final var notPossibleReasons = new StringJoiner("; ");
    if (!groupBy.isEmpty()) {
      preference.isSketchPossible = false;
      notPossibleReasons.add("Group by: " + groupBy);
    }
    if (orderBy != null) {
      preference.isSketchPossible = false;
      notPossibleReasons.add("Sort by " + orderBy);
    }
    if (limit != null) {
      preference.isSketchPossible = false;
      notPossibleReasons.add("Limit: " + limit);
    }
    if (where != null) {
      preference.isSketchPossible = false;
      notPossibleReasons.add("Filter: " + where);
    }
    if (horizon != null && !horizon.equals(interval)) {
      preference.isSketchPossible = false;
      notPossibleReasons.add(
          String.format("Sliding window: horizon %d != interval %d", horizon, interval));
    }
    for (final var aggregate : aggregates) {
      final var function = aggregate.getFunction();
      if (!DataSketch.isSupportedFunction(function)) {
        preference.isSketchPossible = false;
        notPossibleReasons.add(String.format("Aggregate %s", function));
      }
      if ((aggregate.getBy() != null) && (!aggregate.getBy().isEmpty())) {
        final InternalAttributeType attributeType =
            streamDesc.findAnyAttribute(aggregate.getBy()).getAttributeType();
        if (!attributeType.isSupportedBySketch(function.getDataSketchType())) {
          preference.isSketchPossible = false;
          notPossibleReasons.add(
              String.format(
                  "Aggregate %s on InternalAttributeType %s(%s)",
                  function, attributeType, aggregate.getBy()));
        }
      }
    }
    if (preference.isSketchRequired && !preference.isSketchPossible) {
      throw new InvalidRequestException(
          "Data Sketches do not support this query yet. \n Reasons it is not possible to use "
              + "sketches: [%s]. \n Reasons sketches are required: [%s].",
          notPossibleReasons.toString(), requiredReasons.toString());
    }

    preference.useSketch = preference.isSketchRequired || preference.isSketchPossible;
    preference.requiredReasons = requiredReasons.toString();
    preference.notPossibleReasons = notPossibleReasons.toString();
    return preference;
  }

  public boolean equals(Object o) {
    if (!(o instanceof UseSketchPreference) || o == null) {
      return false;
    }
    final var other = (UseSketchPreference) o;
    return useSketch == other.useSketch
        && isSketchRequired == other.isSketchRequired
        && isSketchPossible == other.isSketchPossible
        && Objects.equals(requiredReasons, other.requiredReasons)
        && Objects.equals(notPossibleReasons, other.notPossibleReasons);
  }

  public int hashCode() {
    return Objects.hash(
        useSketch, isSketchRequired, isSketchPossible, requiredReasons, notPossibleReasons);
  }
}
