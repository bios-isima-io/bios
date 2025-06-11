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

import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.models.proto.DataProto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

@Getter
public class SelectRequest {

  // Builders //////////////////////////////////////////////////////////////////

  /**
   * Builds a {@link SelectRequest} instance from a ProtoBuf representation.
   *
   * @param queryProto Protobuf query message to interpret.
   * @return Built request object
   * @throws InvalidRequestException thrown to indicate that the source query is invalid
   */
  public static SelectRequest fromProto(DataProto.SelectQuery queryProto)
      throws InvalidRequestException {
    try {
      final var builder =
          newBuilder(
              queryProto.getFrom(), new Range(queryProto.getStartTime(), queryProto.getEndTime()));
      if (queryProto.getWindowsCount() > 0) {
        builder.window(new SelectWindow(queryProto.getWindows(0)));
      }
      if (queryProto.hasAttributes()) {
        builder.attributes(queryProto.getAttributes().getAttributesList());
      }
      for (int i = 0; i < queryProto.getMetricsCount(); ++i) {
        try {
          final var metricProto = queryProto.getMetrics(i);
          final MeasurementFunction function =
              MeasurementFunction.forName(metricProto.getFunction().name());
          final String attributeName = metricProto.getOf();
          final Optional<String> optAttributeName =
              attributeName.isBlank() ? Optional.empty() : Optional.of(attributeName);
          final String alias = metricProto.getAs();
          final Optional<String> optAlias = alias.isBlank() ? Optional.empty() : Optional.of(alias);
          final var metric = new SelectMetric(function, optAttributeName, optAlias);
          builder.addMetric(metric);
        } catch (IllegalArgumentException e) {
          throw new InvalidRequestException("metrics[%d]: Invalid metric: %s", i, e.getMessage());
        }
      }
      if (queryProto.hasGroupBy()) {
        builder.dimensions(queryProto.getGroupBy().getDimensionsList());
      }
      if (!queryProto.getWhere().isBlank()) {
        builder.filter(queryProto.getWhere());
      }
      if (queryProto.hasOrderBy()) {
        final var orderProto = queryProto.getOrderBy();
        builder.order(
            new SelectOrder(
                orderProto.getBy(), orderProto.getReverse(), orderProto.getCaseSensitive()));
      }
      builder.limit(queryProto.getLimit());
      return builder.build();
    } catch (IllegalArgumentException e) {
      throw new InvalidRequestException("Invalid select query: %s", e.getMessage());
    }
  }

  /**
   * Builds a {@link SelectRequest} simple global window select request.
   *
   * @param signalName Name of the target signal
   * @param timeRange Time range to extract
   * @return Built instance
   */
  public static SelectRequest simpleExtraction(String signalName, Range timeRange) {
    return newBuilder(signalName, timeRange).build();
  }

  public static Builder newBuilder(String signalName, Range timeRange) {
    return new Builder(signalName, timeRange);
  }

  public static class Builder {
    private final String from;
    private Range timeRange;
    private SelectWindow window;
    private List<String> attributes;
    private final List<SelectMetric> metrics;
    private List<String> dimensions;
    private String filter;
    private SelectOrder order;
    private long limit;

    protected Builder(String signal, Range timeRange) {
      if (signal == null || signal.isBlank()) {
        throw new IllegalArgumentException("Signal name must be set");
      }
      this.from = signal;
      this.timeRange = timeRange;
      this.metrics = new ArrayList<>();
    }

    public Builder window(SelectWindow window) {
      this.window = window;
      return this;
    }

    public Builder attributes(List<String> attributes) {
      this.attributes = attributes;
      return this;
    }

    public Builder addMetric(SelectMetric metric) {
      metrics.add(metric);
      return this;
    }

    public Builder dimensions(List<String> dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder order(SelectOrder order) {
      this.order = order;
      return this;
    }

    public Builder limit(long limitation) {
      this.limit = limitation;
      return this;
    }

    public SelectRequest build() {
      return new SelectRequest(this);
    }
  }

  // Instance implementation /////////////////////////////////////////////////

  private final String from;
  private final Range timeRange;
  private final SelectWindow window;
  private final Optional<List<String>> attributes;
  private final List<SelectMetric> metrics;
  private final List<String> dimensions;
  private final Optional<String> filter;
  private final Optional<SelectOrder> order;
  private final long limit; // zero means no limitation

  private SelectRequest(Builder builder) {
    this.from = builder.from;
    this.timeRange = builder.timeRange;
    this.window = builder.window != null ? builder.window : new SelectWindow();
    this.attributes = Optional.ofNullable(builder.attributes);
    this.metrics = Collections.unmodifiableList(builder.metrics);
    this.dimensions = builder.dimensions != null ? builder.dimensions : List.of();
    this.filter = Optional.ofNullable(builder.filter);
    this.order = Optional.ofNullable(builder.order);
    this.limit = builder.limit;
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{from=")
            .append(from)
            .append(", timeRange=")
            .append(timeRange)
            .append(", window=")
            .append(window);
    return sb.append("}").toString();
  }
}
