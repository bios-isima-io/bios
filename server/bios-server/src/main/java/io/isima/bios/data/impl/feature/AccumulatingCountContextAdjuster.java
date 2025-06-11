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
package io.isima.bios.data.impl.feature;

import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_OPERATION;
import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_TIMESTAMP;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.counters.CounterUpdateOperation;
import io.isima.bios.data.DefaultRecord;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.Event;
import io.isima.bios.models.ObjectListEventValue;
import io.isima.bios.models.isql.Metric;
import io.isima.bios.models.isql.OrderBy;
import io.isima.bios.models.isql.ResponseShape;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.isql.Window;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.recorder.SignalRequestType;
import io.isima.bios.server.handlers.SelectState;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Compiled context fetch adjuster for an AccumulatingCount material. */
public class AccumulatingCountContextAdjuster implements ContextAdjuster {
  private static final Logger logger =
      LoggerFactory.getLogger(AccumulatingCountContextAdjuster.class);

  private final List<String> primaryKeyNames;
  private final List<AttributeDesc> primaryKeyAttributes;
  private final List<String> counterValueNames;
  private final long compactionInterval;
  private final StreamDesc signalDesc;
  private final StreamDesc counterCompactionDesc;
  private final Map<String, Integer> recordDefinition;
  private final int timestampIndex;

  public AccumulatingCountContextAdjuster(
      StreamDesc signalDesc, StreamDesc counterCompactionDesc, StreamDesc contextDesc) {
    primaryKeyNames = contextDesc.getPrimaryKey();
    final var primaryKeyAttributes = new ArrayList<AttributeDesc>();
    for (var primaryKeyName : primaryKeyNames) {
      primaryKeyAttributes.add(contextDesc.findAttribute(primaryKeyName));
    }
    this.primaryKeyAttributes = Collections.unmodifiableList(primaryKeyAttributes);
    this.signalDesc = signalDesc;
    this.counterCompactionDesc = counterCompactionDesc;
    counterValueNames = counterCompactionDesc.getViews().get(0).getAttributes();
    compactionInterval = counterCompactionDesc.getRollupInterval().getValueInMillis();
    final var def = new LinkedHashMap<String, Integer>();
    int index = 0;
    for (var attribute : primaryKeyNames) {
      def.put(attribute, index++);
    }
    timestampIndex = index++;
    def.put(ATTRIBUTE_TIMESTAMP, timestampIndex);
    for (var attribute : counterValueNames) {
      def.put(attribute, index++);
    }
    recordDefinition = Collections.unmodifiableMap(def);
  }

  @Override
  public CompletableFuture<List<Event>> adjust(
      List<List<Object>> primaryKeys,
      List<String> targetAttributes,
      List<Event> initialEntries,
      ExecutionState state) {

    // Scan the initial entries retrieved from the context to determine what to query for the signal
    final Map<List<Object>, Object[]> allEntryValues = new HashMap<>();
    long since = Long.MAX_VALUE;

    for (var entry : initialEntries) {
      if (entry == null) {
        continue;
      }
      final var key = DataUtils.makeCompositeKey(primaryKeyNames, entry);
      final long uuidTimestamp = (Long) entry.get(ATTRIBUTE_TIMESTAMP);
      final long timestamp = Utils.uuidTimestampToMillis(uuidTimestamp);
      since = Math.min(since, timestamp);

      final var values = new Object[recordDefinition.size()];
      int index = 0;
      for (var primaryKeyElement : key) {
        values[index++] = primaryKeyElement;
      }
      values[index++] = uuidTimestamp;
      for (var counterValueName : counterValueNames) {
        final var attributeValue = entry.get(counterValueName);
        values[index++] = attributeValue != null ? (Double) attributeValue : 0;
      }
      allEntryValues.put(key, values);
    }

    // Make a query to the updates signal
    final long until = System.currentTimeMillis();
    final long roughTimeDelta = since != Long.MAX_VALUE ? until - since : until;
    final CompletableFuture<Long> deltaFuture;
    if (roughTimeDelta > compactionInterval * 2) {
      deltaFuture =
          getLastCompactionTime(state)
              .thenApply(
                  (fetchedSince) -> {
                    final var delta = until - fetchedSince + compactionInterval;
                    logger.debug("resolved delta: {}", delta);
                    return delta;
                  });
    } else {
      deltaFuture = CompletableFuture.completedFuture(roughTimeDelta);
    }

    return deltaFuture.thenComposeAsync(
        (timeDelta) -> {
          final var startTime = until - timeDelta;
          final String where = makeFilter(primaryKeys);
          final var statement = new MyStatement(startTime, until, where);
          final var version = new BiosVersion(BuildVersion.VERSION);
          final var tenantName = signalDesc.getParent().getName();
          final var selectState =
              new SelectState("FetchRecentCounterChange", tenantName, statement, 0, version, state);
          selectState.setStreamDesc(signalDesc);
          selectState.setStreamName(signalDesc.getName());
          final var userContext = state.getUserContext();
          final String appName;
          final AppType appType;
          if (userContext != null) {
            appName = userContext.getAppName();
            appType = userContext.getAppType();
          } else {
            appName = "";
            appType = AppType.UNKNOWN;
          }
          final var recorder =
              BiosModules.getMetrics()
                  .getRecorder(
                      tenantName, signalDesc.getName(), appName, appType, SignalRequestType.SELECT);
          selectState.setMetricsTracer(state.getMetricsTracer());
          selectState.setMetricsRecorder(recorder);
          if (selectState.getMetricsTracer() != null) {
            selectState.getMetricsTracer().attachRecorders(List.of(recorder));
          }
          return BiosModules.getDataEngine()
              .select(selectState)
              .thenApply(
                  (none) -> {
                    final var dataWindows = selectState.getResponse().getDataWindows();
                    return dataWindows.get(startTime);
                  })
              .thenApplyAsync(
                  (records) -> {
                    final var keySet = primaryKeys.stream().collect(Collectors.toSet());
                    selectState.getMetricsRecorder().getNumReads().add(records.size());
                    for (var updateRecord : records) {
                      final var primaryKey =
                          DataUtils.makeCompositeKey(primaryKeyNames, updateRecord);
                      if (!keySet.contains(primaryKey)) {
                        // not interested
                        continue;
                      }
                      final var currentTimestamp = updateRecord.getEventId().timestamp();
                      if (allEntryValues.containsKey(primaryKey)
                          && currentTimestamp
                              <= (Long) allEntryValues.get(primaryKey)[timestampIndex]) {
                        // processed already
                        continue;
                      }
                      // update snapshots
                      final var operation = updateRecord.getAttribute(ATTRIBUTE_OPERATION);
                      if (operation != null
                          && operation.asString().equals(CounterUpdateOperation.SET.value())) {
                        setValues(primaryKey, updateRecord, allEntryValues);
                      } else {
                        changeValues(primaryKey, updateRecord, allEntryValues);
                      }
                    }
                    return buildEvents(allEntryValues);
                  },
                  state.getExecutor());
        },
        state.getExecutor());
  }

  private CompletableFuture<Long> getLastCompactionTime(ExecutionState state) {
    return ((DataEngineImpl) BiosModules.getDataEngine())
        .getPostProcessScheduler()
        .getDigestionCoverage(signalDesc, counterCompactionDesc, state)
        .thenApply((spec) -> spec.getDoneUntil());
  }

  private String makeFilter(List<List<Object>> primaryKeys) {
    final var sb = new StringBuilder();
    String and = "";
    for (int i = 0; i < primaryKeyAttributes.size(); ++i) {
      sb.append(and);
      var attribute = primaryKeyAttributes.get(i);
      sb.append(attribute.getName()).append(" IN (");
      final var quote = attribute.getAttributeType() == InternalAttributeType.STRING ? "'" : "";
      String delimiter = "";
      final var dimensionValues = new HashSet<>();
      for (List<Object> primaryKey : primaryKeys) {
        final var dimensionValue = primaryKey.get(i);
        if (!dimensionValues.contains(dimensionValue)) {
          sb.append(delimiter).append(quote).append(dimensionValue).append(quote);
          dimensionValues.add(dimensionValue);
        }
        delimiter = ", ";
      }
      sb.append(")");
      and = " AND ";
    }
    sb.append(and).append("operation IN ('set', 'change'))");
    return sb.toString();
  }

  private void setValues(
      List<Object> primaryKey,
      DefaultRecord updateRecord,
      Map<List<Object>, Object[]> allEntryValues) {
    final var values = new Object[recordDefinition.size()];
    int index = 0;
    for (var primaryKeyElement : primaryKey) {
      values[index++] = primaryKeyElement;
    }
    values[index++] = updateRecord.getEventId().timestamp();
    allEntryValues.put(primaryKey, values);
    for (var counterValueName : counterValueNames) {
      final var attributeValue = updateRecord.getAttribute(counterValueName);
      values[index++] = attributeValue != null ? attributeValue.asDouble() : 0;
    }
  }

  private void changeValues(
      List<Object> primaryKey,
      DefaultRecord updateRecord,
      Map<List<Object>, Object[]> allEntryValues) {
    final var values =
        allEntryValues.computeIfAbsent(
            primaryKey,
            (k) -> {
              final var newValues = new Object[recordDefinition.size()];
              int index = 0;
              for (var primaryKeyElement : primaryKey) {
                newValues[index++] = primaryKeyElement;
              }
              while (++index < newValues.length) {
                newValues[index] = Double.valueOf(0.0);
              }
              return newValues;
            });
    int index = primaryKeyNames.size();
    values[index++] = updateRecord.getEventId().timestamp();
    allEntryValues.put(primaryKey, values);
    for (var counterValueName : counterValueNames) {
      final var attributeValue = updateRecord.getAttribute(counterValueName);
      double value = (Double) values[index];
      value += attributeValue != null ? attributeValue.asDouble() : 0;
      values[index++] = value;
    }
  }

  private List<Event> buildEvents(Map<List<Object>, Object[]> allEntryValues) {
    final Map<String, Integer> recordDefinition = new LinkedHashMap<>(this.recordDefinition);
    return allEntryValues.values().stream()
        .map((values) -> ObjectListEventValue.toEvent(recordDefinition, values))
        .collect(Collectors.toList());
  }

  @AllArgsConstructor
  private class MyStatement implements SelectStatement {
    final long startTime;
    final long endTime;
    final String where;

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public long getEndTime() {
      return endTime;
    }

    @Override
    public boolean isDistinct() {
      return false;
    }

    @Override
    public List<String> getSelectAttributes() {
      return null;
    }

    @Override
    public List<? extends Metric> getMetrics() {
      return List.of();
    }

    @Override
    public String getFrom() {
      return signalDesc.getName();
    }

    @Override
    public String getWhere() {
      return where;
    }

    @Override
    public List<String> getGroupBy() {
      return null;
    }

    @Override
    public OrderBy getOrderBy() {
      return null;
    }

    @Override
    public Window<?> getOneWindow() {
      return null;
    }

    @Override
    public <W extends Window<W>> Window<W> getOneWindow(Class<W> windowType) {
      return null;
    }

    @Override
    public boolean hasLimit() {
      return false;
    }

    @Override
    public int getLimit() {
      return 0;
    }

    @Override
    public boolean hasWindow() {
      return false;
    }

    @Override
    public boolean hasOrderBy() {
      return false;
    }

    @Override
    public boolean isOnTheFly() {
      return false;
    }

    @Override
    public ResponseShape getResponseShape() {
      return ResponseShape.VALUE;
    }
  }
}
