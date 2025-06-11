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
package io.isima.bios.sdk;

import io.isima.bios.codec.proto.isql.BasicBuilderValidator;
import io.isima.bios.codec.proto.isql.ProtoInsertQueryBuilder;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SelectOrder;
import io.isima.bios.models.GenericMetric;
import io.isima.bios.sdk.impl.SessionImpl;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * This class is public in order to make ISql statements work. However, this class and all of its
 * subclasses are not meant to be used directly by an application. Doing so may cause the
 * application's code to break with newer versions of bios SDK.
 */
public class ISqlHelperDoNotDirectlyUseThisClassOrSubclassesInApplication {
  private static List<String> getNonNullStringList(final List<String> strings) {
    return (strings == null) ? new ArrayList<String>() : strings;
  }

  private static List<String> getNonNullStringList(final String[] strings) {
    return (strings == null) ? new ArrayList<String>() : Arrays.asList(strings);
  }

  public static class From {
    private final List<String> columns;

    From(List<String> columns) {
      this.columns = getNonNullStringList(columns);
    }

    From(String... columns) {
      this.columns = getNonNullStringList(columns);
    }

    public SignalWhere fromSignal(String signalName) {
      BasicBuilderValidator validator = new BasicBuilderValidator();
      validator.validateStringParam(signalName, "signalName");

      var signalSelect = new ISql.SignalSelect();
      signalSelect.setSelectColumns(columns);
      signalSelect.setSignalName(signalName);
      return new SignalWhere(signalSelect);
    }

    public ContextWhere fromContext(String contextName) {
      BasicBuilderValidator validator = new BasicBuilderValidator();
      validator.validateStringParam(contextName, "contextName");

      var contextSelect = new ISql.ContextSelect();
      contextSelect.setSelectColumns(columns);
      contextSelect.setContextName(contextName);
      return new ContextWhere(contextSelect);
    }
  }

  public static class SignalWhere extends SignalGroupBy {
    SignalWhere(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalGroupBy where(String where) {
      signalSelect.setWhere(where);
      return new SignalGroupBy(signalSelect);
    }
  }

  public static class SignalGroupBy extends SignalOrderBy {
    SignalGroupBy(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalOrderBy groupBy(List<String> groupByColumns) {
      signalSelect.setGroupByColumns(groupByColumns);
      return new SignalOrderBy(signalSelect);
    }

    public SignalOrderBy groupBy(String... groupByColumns) {
      signalSelect.setGroupByColumns(getNonNullStringList(groupByColumns));
      return new SignalOrderBy(signalSelect);
    }
  }

  public static class SignalOrderBy extends SignalLimit {
    SignalOrderBy(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalLimit orderBy(
        String orderByColumn, Boolean orderByReverse, Boolean orderByCaseSensitive) {
      signalSelect.setOrderByColumn(orderByColumn);
      signalSelect.setOrderByReverse(orderByReverse);
      signalSelect.setOrderByCaseSensitive(orderByCaseSensitive);
      return new SignalLimit(signalSelect);
    }

    public SignalLimit orderBy(String orderByColumn, Boolean orderByReverse) {
      return orderBy(orderByColumn, orderByReverse, null);
    }

    public SignalLimit orderBy(String orderByColumn) {
      return orderBy(orderByColumn, null);
    }
  }

  public static class SignalLimit extends SignalWindow {
    SignalLimit(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalWindow limit(Integer limit) {
      signalSelect.setLimit(limit);
      return new SignalWindow(signalSelect);
    }
  }

  public static class SignalWindow extends SignalTimeRange {
    SignalWindow(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalTimeRange tumblingWindow(Duration windowSize) {
      if (windowSize != null) {
        signalSelect.setWindowSizeMs(windowSize.toMillis());
        signalSelect.setHopSizeMs(windowSize.toMillis());
      }
      return new SignalTimeRange(signalSelect);
    }

    public SignalTimeRange hoppingWindow(Duration windowSize, Duration hopSize) {
      if (windowSize != null) {
        signalSelect.setWindowSizeMs(windowSize.toMillis());
      }
      if (hopSize != null) {
        signalSelect.setHopSizeMs(hopSize.toMillis());
      }
      return new SignalTimeRange(signalSelect);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class SignalTimeRange {
    protected final ISql.SignalSelect signalSelect;

    public SignalOnTheFly timeRange(
        long originTimeEpochMs, long deltaMs, Long alignmentDurationMs) {
      signalSelect.setOriginTimeEpochMs(originTimeEpochMs);
      signalSelect.setDeltaMs(deltaMs);
      signalSelect.setAlignmentDurationMs(alignmentDurationMs);
      return new SignalOnTheFly(signalSelect);
    }

    public SignalOnTheFly timeRange(long originTimeEpochMs, long deltaMs) {
      return timeRange(originTimeEpochMs, deltaMs, null);
    }

    public SignalOnTheFly timeRange(
        long originTimeEpochMs, Duration delta, Duration alignmentDuration) {
      Long alignmentDurationMs = (alignmentDuration == null) ? null : alignmentDuration.toMillis();
      return timeRange(originTimeEpochMs, delta.toMillis(), alignmentDurationMs);
    }

    public SignalOnTheFly timeRange(long originTimeEpochMs, Duration delta) {
      return timeRange(originTimeEpochMs, delta, null);
    }

    public SignalOnTheFly timeRange(
        OffsetDateTime originTime, Duration delta, Duration alignmentDuration) {
      return timeRange(originTime.toInstant().toEpochMilli(), delta, alignmentDuration);
    }

    public SignalOnTheFly timeRange(OffsetDateTime originTime, Duration delta) {
      return timeRange(originTime, delta, null);
    }
  }

  public static class SignalOnTheFly extends SignalSelectBuild {
    SignalOnTheFly(ISql.SignalSelect signalSelect) {
      super(signalSelect);
    }

    public SignalSelectBuild onTheFly() {
      signalSelect.setOnTheFly(true);
      return new SignalSelectBuild(signalSelect);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class SignalSelectBuild {
    protected final ISql.SignalSelect signalSelect;

    public ISql.SignalSelect build() {
      return signalSelect;
    }
  }

  interface KeysSpecifier {

    KeysFinalSpecifier in(CompositeKey... primaryKeyValues);

    KeysFinalSpecifier in(Comparable... primaryKeyValues);

    KeysFinalSpecifier in(List<?> primaryKeyValues);

    KeysFinalSpecifier eq(Object primaryKeyValue);
  }

  interface KeysFinalSpecifier {}

  @Getter
  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextWhereClauseBuilder implements KeysSpecifier, KeysFinalSpecifier {
    private List<CompositeKey> primaryKeyValues;

    @Override
    public KeysFinalSpecifier in(CompositeKey... primaryKeyValues) {
      this.primaryKeyValues = List.of(primaryKeyValues);
      return this;
    }

    @Override
    public KeysFinalSpecifier in(Comparable... primaryKeyValues) {
      final var keys = new ArrayList<CompositeKey>();
      for (var value : primaryKeyValues) {
        keys.add(CompositeKey.of(value));
      }
      this.primaryKeyValues = Collections.unmodifiableList(keys);
      return this;
    }

    @Override
    public KeysFinalSpecifier in(List<?> primaryKeyValues) {
      if (primaryKeyValues != null) {
        final var keys = new ArrayList<CompositeKey>();
        for (int index = 0; index < primaryKeyValues.size(); ++index) {
          final var item = primaryKeyValues.get(index);
          if (item instanceof CompositeKey) {
            keys.add(((CompositeKey) item));
          } else if ((item instanceof Long)
              || (item instanceof String)
              || (item instanceof Double)
              || (item instanceof Boolean)
              || (item instanceof ByteBuffer)) {
            keys.add(CompositeKey.of(item));
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Key[%d] type invalid. A scalar key must be of type"
                        + " String, Long, Double, Boolean or ByteBuffer",
                    index));
          }
        }
        this.primaryKeyValues = Collections.unmodifiableList(keys);
      }
      return this;
    }

    @Override
    public KeysFinalSpecifier eq(Object primaryKeyValue) {
      if (primaryKeyValue != null) {
        if (primaryKeyValue instanceof CompositeKey) {
          this.primaryKeyValues = List.of(((CompositeKey) primaryKeyValue));
        } else {
          this.primaryKeyValues = List.of(CompositeKey.of(primaryKeyValue));
        }
      }
      return this;
    }
  }

  public static class ContextWhere extends ContextGroupBy {
    ContextWhere(ISql.ContextSelect contextSelect) {
      super(contextSelect);
    }

    public ContextOnTheFly where(KeysFinalSpecifier specifier) {
      contextSelect.setPrimaryKeyValues(
          ((ContextWhereClauseBuilder) specifier).getPrimaryKeyValues());
      return new ContextOnTheFly(contextSelect);
    }

    // A string where clause indicates the new kind of select context request.
    public ContextGroupBy where(String where) {
      contextSelect.getExRequest().setWhere(where);
      return new ContextGroupBy(contextSelect);
    }
  }

  public static class ContextGroupBy extends ContextOrderBy {
    ContextGroupBy(ISql.ContextSelect contextSelect) {
      super(contextSelect);
    }

    public ContextGroupBy groupBy(List<String> groupByColumns) {
      contextSelect.getExRequest().setGroupBy(groupByColumns);
      return new ContextGroupBy(contextSelect);
    }

    public ContextGroupBy groupBy(String... groupByColumns) {
      return groupBy(Arrays.asList(groupByColumns));
    }
  }

  public static class ContextOrderBy extends ContextLimit {
    ContextOrderBy(ISql.ContextSelect contextSelect) {
      super(contextSelect);
    }

    public ContextLimit orderBy(String keyName, Boolean reverse, Boolean caseSensitive) {
      contextSelect.getExRequest().setOrderBy(new SelectOrder(keyName, reverse, caseSensitive));
      return new ContextLimit(contextSelect);
    }

    public ContextLimit orderBy(String keyName, Boolean reverse) {
      return orderBy(keyName, reverse, false);
    }

    public ContextLimit orderBy(String keyName) {
      return orderBy(keyName, false);
    }
  }

  public static class ContextLimit extends ContextOnTheFly {
    ContextLimit(ISql.ContextSelect contextSelect) {
      super(contextSelect);
    }

    public ContextOnTheFly limit(Integer limit) {
      contextSelect.getExRequest().setLimit(limit);
      return new ContextOnTheFly(contextSelect);
    }
  }

  public static class ContextOnTheFly extends ContextSelectBuild {
    ContextOnTheFly(ISql.ContextSelect contextSelect) {
      super(contextSelect);
    }

    public ContextSelectBuild onTheFly() {
      if (SelectContextRequest.isEmpty(contextSelect.getExRequest())) {
        contextSelect.setOnTheFly(true);
      } else {
        contextSelect.getExRequest().setOnTheFly(true);
      }
      return new ContextSelectBuild(contextSelect);
    }

    public ContextSelectBuild onTheFly(boolean enabled) {
      if (SelectContextRequest.isEmpty(contextSelect.getExRequest())) {
        contextSelect.setOnTheFly(enabled);
      } else {
        contextSelect.getExRequest().setOnTheFly(enabled);
      }
      return new ContextSelectBuild(contextSelect);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextSelectBuild {
    protected final ISql.ContextSelect contextSelect;

    public ISql.ContextSelect build() {

      List<String> attributes = new ArrayList<>();
      List<GenericMetric> metrics = new ArrayList<>();
      final var columns = contextSelect.getSelectColumns();
      SessionImpl.getMetricsAndAttributes(columns, attributes, null, metrics);
      if (attributes.isEmpty()) {
        attributes = null;
      }
      if (metrics.isEmpty()) {
        metrics = null;
      }

      // Use new select API if extended clauses were used or if metrics are present.
      if (SelectContextRequest.isEmpty(contextSelect.getExRequest()) && (metrics == null)) {
        return contextSelect;
      }

      contextSelect.setExtendedRequest(true);
      contextSelect.getExRequest().setContext(contextSelect.getContextName());
      contextSelect.getExRequest().setAttributes(attributes);
      contextSelect.getExRequest().setMetrics(metrics);

      return contextSelect;
    }
  }

  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class InsertInto {

    public InsertValues into(String signalName) {
      BasicBuilderValidator validator = new BasicBuilderValidator();
      validator.validateStringParam(signalName, "signalName");

      var insert = new ISql.Insert();
      insert.setSignalName(signalName);
      return new InsertValues(insert);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class InsertValues {
    protected final ISql.Insert insert;

    public InsertBuild csv(String csv) {
      insert.setCsvs(List.of(csv));
      return new InsertBuild(insert);
    }

    public InsertBuild csvBulk(List<String> csvBulk) {
      insert.setCsvs(csvBulk);
      return new InsertBuild(insert);
    }

    public InsertBuild csvBulk(String... csvBulk) {
      insert.setCsvs(Arrays.asList(csvBulk));
      return new InsertBuild(insert);
    }

    public InsertBuild values(List<Object> values) {
      return csv(ProtoInsertQueryBuilder.csvFromValues(values));
    }

    public InsertBuild values(Object... values) {
      return values(Arrays.asList(values));
    }

    public InsertBuild valuesBulk(List<List<Object>> valuesBulk) {
      return csvBulk(
          valuesBulk.stream()
              .map(ProtoInsertQueryBuilder::csvFromValues)
              .collect(Collectors.toList()));
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class InsertBuild {
    protected final ISql.Insert insert;

    public ISql.Insert build() {
      return insert;
    }
  }

  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpsertInto {

    public ContextUpsertValues intoContext(String contextName) {
      BasicBuilderValidator validator = new BasicBuilderValidator();
      validator.validateStringParam(contextName, "contextName");

      var upsert = new ISql.ContextUpsert();
      upsert.setContextName(contextName);
      return new ContextUpsertValues(upsert);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpsertValues {
    protected final ISql.ContextUpsert contextUpsert;

    public ContextUpsertBuild csv(String csv) {
      contextUpsert.setCsvs(List.of(csv));
      return new ContextUpsertBuild(contextUpsert);
    }

    public ContextUpsertBuild csvBulk(List<String> csvBulk) {
      contextUpsert.setCsvs(csvBulk);
      return new ContextUpsertBuild(contextUpsert);
    }

    public ContextUpsertBuild csvBulk(String... csvBulk) {
      contextUpsert.setCsvs(Arrays.asList(csvBulk));
      return new ContextUpsertBuild(contextUpsert);
    }

    public ContextUpsertBuild values(List<Object> values) {
      return csv(ProtoInsertQueryBuilder.csvFromValues(values));
    }

    public ContextUpsertBuild values(Object... values) {
      return values(Arrays.asList(values));
    }

    public ContextUpsertBuild valuesBulk(List<List<Object>> valuesBulk) {
      return csvBulk(
          valuesBulk.stream()
              .map(ProtoInsertQueryBuilder::csvFromValues)
              .collect(Collectors.toList()));
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpsertBuild {
    protected final ISql.ContextUpsert contextUpsert;

    public ISql.ContextUpsert build() {
      return contextUpsert;
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpdateSet {
    protected final ISql.ContextUpdate contextUpdate;

    public ContextUpdateWhere set(Map<String, Object> attributesToUpdate) {
      contextUpdate.setAttributesToUpdate(attributesToUpdate);
      return new ContextUpdateWhere(contextUpdate);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpdateWhere {
    protected final ISql.ContextUpdate contextUpdate;

    public ContextUpdateBuild where(KeysFinalSpecifier specifier) {
      contextUpdate.setPrimaryKeyValues(
          ((ContextWhereClauseBuilder) specifier).getPrimaryKeyValues());
      return new ContextUpdateBuild(contextUpdate);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextUpdateBuild {
    protected final ISql.ContextUpdate contextUpdate;

    public ISql.ContextUpdate build() {
      return contextUpdate;
    }
  }

  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextDeleteFrom {
    public ContextDeleteWhere fromContext(String contextName) {
      var contextDelete = new ISql.ContextDelete();
      contextDelete.setContextName(contextName);
      return new ContextDeleteWhere(contextDelete);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextDeleteWhere {
    protected final ISql.ContextDelete contextDelete;

    public ContextDeleteBuild where(KeysFinalSpecifier specifier) {
      contextDelete.setPrimaryKeyValues(
          ((ContextWhereClauseBuilder) specifier).getPrimaryKeyValues());
      return new ContextDeleteBuild(contextDelete);
    }
  }

  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public static class ContextDeleteBuild {
    protected final ISql.ContextDelete contextDelete;

    public ISql.ContextDelete build() {
      return contextDelete;
    }
  }
}
