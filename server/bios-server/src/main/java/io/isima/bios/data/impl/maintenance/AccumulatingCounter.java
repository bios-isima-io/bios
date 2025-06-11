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
package io.isima.bios.data.impl.maintenance;

import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_OPERATION;
import static io.isima.bios.feature.CountersConstants.ATTRIBUTE_TIMESTAMP;
import static io.isima.bios.feature.CountersConstants.OPERATION_VALUE_SET;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumulatingCounter {
  private static Logger logger = LoggerFactory.getLogger(AccumulatingCounter.class);

  private final DataEngine dataEngine;

  private final String tenantName;
  private final List<String> counterKeyElements;
  private final String featureAsContextName;
  private final List<String> counterUpdateAttributes;

  private final Integer ordinalForSetOperation;
  long totalLateArrivals;
  long largestGap;

  public AccumulatingCounter(DataEngine dataEngine, StreamDesc signalDesc, StreamDesc rollup) {
    this.dataEngine = dataEngine;
    tenantName = signalDesc.getParent().getName();
    final var view = rollup.getViews().get(0);
    counterKeyElements =
        view.getGroupBy().stream()
            .filter((dimension) -> !ATTRIBUTE_OPERATION.equals(dimension))
            .collect(Collectors.toList());
    featureAsContextName = view.getEffectiveFeatureAsContextName(signalDesc.getName());
    counterUpdateAttributes = Collections.unmodifiableList(view.getAttributes());

    int ordinal = 0;
    for (var attribute : signalDesc.getAttributes()) {
      final var attributeName = attribute.getName();
      if (attributeName.equals(ATTRIBUTE_OPERATION)) {
        for (int i = 0; i < attribute.getEnum().size(); ++i) {
          if (attribute.getEnum().get(i).equals(OPERATION_VALUE_SET)) {
            ordinal = i;
            break;
          }
        }
      }
    }
    ordinalForSetOperation = ordinal;
    totalLateArrivals = 0;
    largestGap = 0;
  }

  public CompletableFuture<Void> count(
      DigestSpecifier specifier, List<Event> srcEvents, ExecutionState state) {
    final var counterEntries = new HashMap<List<Object>, CounterEntry>();

    // collect keys from the updates first, exclude events that are out of the range
    final var events =
        DataMaintenanceUtils.getEventsInRange(
            srcEvents,
            specifier.getStartTime() - specifier.getRefetchTime(),
            specifier.getEndTime());
    final var counterKeys = new HashSet<List<Object>>();
    for (var event : events) {
      final var counterKey = makeCounterKey(event);
      counterKeys.add(counterKey);
    }

    // fetch the corresponding context entries
    final StreamDesc contextDesc;
    try {
      contextDesc = BiosModules.getAdminInternal().getStream(tenantName, featureAsContextName);
    } catch (NoSuchStreamException | NoSuchTenantException e) {
      return CompletableFuture.failedFuture(e);
    }
    final List<List<Object>> primaryKeys = new ArrayList<>(counterKeys);
    final var getContextState = new ContextOpState("fetch counter snapshots", state);
    getContextState.setContextDesc(contextDesc);
    getContextState.setOptions(Set.of(ContextOpOption.IGNORE_CACHE_ONLY));
    final var entriesFuture = new CompletableFuture<List<Event>>();
    dataEngine.getContextEntriesAsync(
        primaryKeys,
        getContextState,
        entriesFuture::complete,
        entriesFuture::completeExceptionally);

    return entriesFuture
        .thenComposeAsync(
            (contextEntries) -> {
              for (var snapshotRecord : contextEntries) {
                if (snapshotRecord != null) {
                  final var counterKey = makeCounterKey(snapshotRecord);
                  mergeRecord(counterKey, snapshotRecord, counterEntries);
                }
              }

              // rollup updates
              final var keysToForgetHistory = new HashSet<List<Object>>();
              for (var event : events) {
                final var counterKey = makeCounterKey(event);
                var counterEntry = counterEntries.get(counterKey);
                final var eventTimestamp = event.getEventId().timestamp();
                if (counterEntry != null && eventTimestamp <= counterEntry.getTimestamp()) {
                  // we assume the event is processed already in this case
                  continue;
                }
                if (counterEntry == null) {
                  counterEntry = new CounterEntry();
                  counterEntry.setKeys(counterKey);
                  counterEntry.setValues(new double[counterUpdateAttributes.size()]);
                  counterEntries.put(counterKey, counterEntry);
                }

                final var operation = event.get(ATTRIBUTE_OPERATION);
                if (operation.equals(ordinalForSetOperation)) {
                  keysToForgetHistory.add(counterKey);
                  for (int i = 0; i < counterUpdateAttributes.size(); ++i) {
                    counterEntry.values[i] = (Double) event.get(counterUpdateAttributes.get(i));
                  }
                } else {
                  for (int i = 0; i < counterUpdateAttributes.size(); ++i) {
                    counterEntry.values[i] += (Double) event.get(counterUpdateAttributes.get(i));
                  }
                }
                counterEntry.setTimestamp(eventTimestamp);
              }
              return CompletableFuture.completedFuture(null);
            },
            state.getExecutor())
        .thenComposeAsync(
            (none) -> {
              if (!counterEntries.isEmpty()) {
                return saveSnapshots(contextDesc, counterEntries, specifier.getEndTime(), state);
              }
              return CompletableFuture.completedFuture(null);
            },
            state.getExecutor());
  }

  private List<Object> makeCounterKey(Event event) {
    final var compositeKey = new Object[counterKeyElements.size()];
    for (int i = 0; i < counterKeyElements.size(); ++i) {
      final var value = event.get(counterKeyElements.get(i));
      compositeKey[i] = value;
    }
    return Arrays.asList(compositeKey);
  }

  private void mergeRecord(
      List<Object> counterKey,
      Event snapshotRecord,
      HashMap<List<Object>, CounterEntry> counterEntries) {
    final var counterEntry =
        counterEntries.computeIfAbsent(
            counterKey,
            (k) -> {
              final var newEntry = new CounterEntry();
              newEntry.setKeys(k);
              newEntry.setValues(new double[counterUpdateAttributes.size()]);
              return newEntry;
            });
    for (int i = 0; i < counterUpdateAttributes.size(); ++i) {
      final var attributeName = counterUpdateAttributes.get(i);
      counterEntry.getValues()[i] += (Double) snapshotRecord.get(attributeName);
    }
    counterEntry.setTimestamp((Long) snapshotRecord.get(ATTRIBUTE_TIMESTAMP));
  }

  private CompletableFuture<Void> saveSnapshots(
      StreamDesc contextDesc,
      HashMap<List<Object>, CounterEntry> counterEntries,
      long until,
      ExecutionState state) {
    final var csvEntries = new ArrayList<String>();
    counterEntries.forEach(
        (counterKey, counterEntry) -> {
          final Event event = new EventJson();
          for (int i = 0; i < counterKeyElements.size(); ++i) {
            event.set(counterKeyElements.get(i), counterEntry.getKeys().get(i));
          }
          for (int i = 0; i < counterUpdateAttributes.size(); ++i) {
            final var attributeName = counterUpdateAttributes.get(i);
            event.set(attributeName, counterEntry.getValues()[i]);
          }
          event.set(ATTRIBUTE_TIMESTAMP, counterEntry.getTimestamp());
          csvEntries.add(DataMaintenanceUtils.toCsv(event, contextDesc));
        });

    return DataMaintenanceUtils.saveContextAsync("counters", contextDesc, csvEntries, until, state);
  }

  @Getter
  @Setter
  private static class CounterEntry {
    private List<Object> keys;
    private double[] values;
    private long timestamp;
  }
}
