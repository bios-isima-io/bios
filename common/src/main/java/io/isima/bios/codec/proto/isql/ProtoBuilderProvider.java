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
import io.isima.bios.models.isql.ISqlBuilderProvider;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.models.isql.ISqlStatement.ContextSelectBuilder;
import io.isima.bios.models.isql.ISqlStatement.SelectBuilder;
import io.isima.bios.models.isql.ISqlStatement.SnappedTimeRangeSelectBuilder;
import io.isima.bios.models.isql.InsertStatement;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.isql.WhereClause;
import java.util.List;

/** Provides Protobuf builders. */
public final class ProtoBuilderProvider extends ISqlBuilderProvider {

  private ProtoBuilderProvider() {}

  /** Configures protobuf builder provider, if this method is called. */
  public static void configureProtoBuilderProvider() {
    ISqlBuilderProvider.setBuilderProvider(new ProtoBuilderProvider());
  }

  @Override
  protected InsertStatement.Into getInsertStatementBuilder() {
    return new ProtoInsertQueryBuilder(new BasicBuilderValidator());
  }

  @Override
  protected SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      getSelectStatementBuilder() {
    return new ProtoSelectQueryBuilder(new BasicBuilderValidator(), null, null);
  }

  @Override
  protected SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      getSelectStatementBuilder(List<String> attributes) {
    return new ProtoSelectQueryBuilder(new BasicBuilderValidator(), attributes, null);
  }

  @Override
  protected SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
      getSelectStatementBuilder(
          List<String> attributes, List<Metric.MetricFinalSpecifier> metrics) {
    return new ProtoSelectQueryBuilder(new BasicBuilderValidator(), attributes, metrics);
  }

  @Override
  protected Metric.Builder getMetricSpecifier(MetricFunction function) {
    return new ProtoMetricBuilder(new BasicBuilderValidator(), function);
  }

  @Override
  protected OrderBy.OrderByFinalSpecifier getOrderBySpecifier(String by) {
    return new ProtoOrderByBuilder(by);
  }

  @Override
  protected SlidingWindow.WindowFinalSpecifier getSlidingWindow(long slideInterval, int numSlides) {
    return new ProtoSlidingWindowBuilder(new BasicBuilderValidator(), slideInterval, numSlides);
  }

  @Override
  protected TumblingWindow.WindowFinalSpecifier getTumblingWindow(long windowSizeMillis) {
    return new ProtoTumblingWindowBuilder(new BasicBuilderValidator(), windowSizeMillis);
  }

  @Override
  protected WhereClause.KeysSpecifier getKeysSpecifier() {
    return new WhereClauseBuilder(new BasicBuilderValidator());
  }
}
