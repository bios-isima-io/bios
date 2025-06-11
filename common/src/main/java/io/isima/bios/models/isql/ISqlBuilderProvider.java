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
package io.isima.bios.models.isql;

import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.isql.ISqlStatement.SelectBuilder;
import io.isima.bios.models.isql.ISqlStatement.SnappedTimeRangeSelectBuilder;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Externalize builder provider so that layers above the builder provider can provide builders
 * without breaking layering rules.
 */
public abstract class ISqlBuilderProvider {
  private static AtomicReference<ISqlBuilderProvider> configuredProvider =
      new AtomicReference<>(null);

  protected abstract InsertStatement.Into getInsertStatementBuilder();

  protected abstract SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
      getSelectStatementBuilder();

  protected abstract SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
      getSelectStatementBuilder(List<String> attributes);

  protected abstract SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
      getSelectStatementBuilder(List<String> attributes, List<Metric.MetricFinalSpecifier> metrics);

  protected abstract Metric.Builder getMetricSpecifier(MetricFunction function);

  protected abstract OrderBy.OrderByFinalSpecifier getOrderBySpecifier(String by);

  protected abstract SlidingWindow.WindowFinalSpecifier getSlidingWindow(
      long slideInterval, int numSlides);

  protected abstract TumblingWindow.WindowFinalSpecifier getTumblingWindow(long windowSizeMillis);

  protected abstract WhereClause.KeysSpecifier getKeysSpecifier();

  static ISqlBuilderProvider getBuilderProvider() {
    ISqlBuilderProvider current = configuredProvider.get();
    return current == null ? new DefaultISqlBuilderProvider() : current;
  }

  protected static void setBuilderProvider(ISqlBuilderProvider provider) {
    // can only configure once, unless reset
    configuredProvider.compareAndSet(null, provider);
  }

  public static void reset() {
    // used only for tests
    configuredProvider.set(null);
  }

  private static class DefaultISqlBuilderProvider extends ISqlBuilderProvider {

    @Override
    protected InsertStatement.Into getInsertStatementBuilder() {
      throw new IllegalStateException("No Insert statement builder configured yet");
    }

    @Override
    protected SelectStatement.FromContext<
            SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
        getSelectStatementBuilder() {
      throw new IllegalStateException("No Select statement builder configured yet");
    }

    @Override
    protected SelectStatement.FromContext<
            SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
        getSelectStatementBuilder(List<String> attributes) {
      throw new IllegalStateException("No Select statement builder configured yet");
    }

    @Override
    protected SelectStatement.FromContext<
            SelectBuilder, SnappedTimeRangeSelectBuilder, ISqlStatement.ContextSelectBuilder>
        getSelectStatementBuilder(
            List<String> attributes, List<Metric.MetricFinalSpecifier> metrics) {
      throw new IllegalStateException("No Select statement builder configured yet");
    }

    @Override
    protected Metric.Builder getMetricSpecifier(MetricFunction function) {
      throw new IllegalStateException("No Metric Specifier configured yet");
    }

    @Override
    protected OrderBy.OrderByFinalSpecifier getOrderBySpecifier(String by) {
      throw new IllegalStateException("No OrderBy Specifier configured yet");
    }

    @Override
    protected SlidingWindow.WindowFinalSpecifier getSlidingWindow(
        long slideInterval, int numSlides) {
      throw new IllegalStateException("No Window Specifier configured yet");
    }

    @Override
    protected TumblingWindow.WindowFinalSpecifier getTumblingWindow(long windowSizeMillis) {
      throw new IllegalStateException("No Window Specifier configured yet");
    }

    @Override
    protected WhereClause.KeysSpecifier getKeysSpecifier() {
      throw new IllegalStateException("No Keys Specifier configured yet");
    }
  }
}
