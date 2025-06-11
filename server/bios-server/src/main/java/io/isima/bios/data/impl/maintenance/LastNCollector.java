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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.DataUtils;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.LastN;
import io.isima.bios.models.LastNItem;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastNCollector {
  private static final Logger logger = LoggerFactory.getLogger(LastNCollector.class);

  // Parts necessary for collecting LastN
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  private final DataEngine dataEngine;
  private final DataEngineMaintenance maintainer;

  public LastNCollector(DataEngine dataEngine, DataEngineMaintenance maintainer) {
    this.dataEngine = dataEngine;
    this.maintainer = maintainer;
  }

  public CompletableFuture<List<Event>> update(
      List<Event> extractedEvents,
      StreamDesc signalDesc,
      DigestSpecifier specifier,
      ViewDesc viewDesc,
      ExecutionState state) {

    final var contextDesc =
        signalDesc
            .getParent()
            .getStream(viewDesc.getEffectiveFeatureAsContextName(signalDesc.getName()));
    final var contextCassStream = (ContextCassStream) dataEngine.getCassStream(contextDesc);
    final var obsoleteEntryKeys = contextCassStream.fetchObsoleteEntryKeys();

    final var events = DataMaintenanceUtils.getEventsInRange(extractedEvents, specifier);

    if (events.isEmpty() && obsoleteEntryKeys.isEmpty()) {
      return CompletableFuture.completedFuture(List.of());
    }

    final var foreignKeyNames = viewDesc.getGroupBy();
    final var signalValueName = viewDesc.getAttributes().get(0);
    final var primaryKeyNames = contextDesc.getPrimaryKey();
    String contextValueNameTemp = null;
    for (var attribute : contextDesc.getAttributes()) {
      if (!contextDesc.getPrimaryKey().contains(attribute.getName())) {
        contextValueNameTemp = attribute.getName();
        break;
      }
    }
    if (contextValueNameTemp == null) {
      return CompletableFuture.failedFuture(
          new ApplicationException(
              String.format(
                  "LastN context does not have value attribute; tenant=%s, context=%s",
                  contextDesc.getParent().getName(), contextDesc.getName())));
    }
    final var contextValueName = contextValueNameTemp;
    final var limit = Objects.requireNonNullElse(viewDesc.getLastNItems(), 1L);

    final String tenantName = contextDesc.getParent().getName();
    final var contextName = contextDesc.getName();

    final long maintenanceStartTime = System.currentTimeMillis();

    // Build the primary keys for the contexts to upsert/delete
    final Set<List<Object>> primaryKeySet0 = new HashSet<>();
    events.forEach(
        (event) -> primaryKeySet0.add(DataUtils.makeCompositeKey(foreignKeyNames, event)));
    // The obsolete entries found by the context maintenance may be deleted already by other rollup
    // nodes. We check the existence of them again here after the context inter-node mutex lock is
    // obtained.
    primaryKeySet0.addAll(obsoleteEntryKeys);

    // If the key type is string or blob, empty first primary key elements are not allowed,
    // we skip them
    final var keyType = contextDesc.getAttributes().get(0).getAttributeType();
    final boolean isVariableLengthKey = keyType == InternalAttributeType.STRING;
    final Set<List<Object>> primaryKeySet;
    if (isVariableLengthKey) {
      primaryKeySet =
          primaryKeySet0.stream()
              .filter((key) -> !key.get(0).toString().isEmpty())
              .collect(Collectors.toSet());
      // TODO(Naoki): Do something to blob, but does the Set container handle ByteBuffer correctly?
    } else {
      primaryKeySet = primaryKeySet0;
    }

    final var latestContextEntries = new HashMap<List<Object>, List<LastNItem>>();
    final var keysToUpsert = new HashSet<List<Object>>();

    // Get context entries via DataEngine
    final var primaryKeys = List.copyOf(primaryKeySet);
    final var getContextState = new ContextOpState("fetch last N events", state);
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
              for (var contextEntry : contextEntries) {
                if (contextEntry != null) {
                  try {
                    final LastN lastN =
                        mapper.readValue(
                            contextEntry.get(contextValueName).toString(), LastN.class);
                    final var collection = lastN.getCollection();
                    final List<Object> primaryKeyValues =
                        DataUtils.makeCompositeKey(primaryKeyNames, contextEntry);
                    latestContextEntries.put(primaryKeyValues, collection);
                  } catch (JsonProcessingException e) {
                    // TODO(Naoki): Elaborate more
                    logger.error("Failed to decode a last N collection");
                  }
                }
              }

              // Create or update context entries
              final long ttl =
                  Objects.requireNonNullElse(
                      viewDesc.getLastNTtl(), BiosConstants.DEFAULT_LAST_N_COLLECTION_TTL);
              final var since = specifier.getDoneUntil() - ttl;
              for (var signalEvent : events) {
                final List<Object> key = DataUtils.makeCompositeKey(foreignKeyNames, signalEvent);
                if (isVariableLengthKey && "".equals(key.get(0))) {
                  continue;
                }
                final var newItem =
                    new LastNItem(
                        signalEvent.getIngestTimestamp().getTime(),
                        signalEvent.get(signalValueName));
                final var newCollection =
                    mergeNewItem(latestContextEntries.get(key), newItem, since, limit);
                latestContextEntries.put(key, newCollection);
                keysToUpsert.add(key);
              }

              // Upsert created and updated context entries
              final var csvEntries = new ArrayList<String>();
              keysToUpsert.forEach(
                  (key) -> {
                    final var collection = latestContextEntries.remove(key);
                    final Event event = new EventJson();
                    for (int i = 0; i < primaryKeyNames.size(); ++i) {
                      event.set(primaryKeyNames.get(i), key.get(i));
                    }
                    try {
                      final String value = mapper.writeValueAsString(new LastN(collection));
                      event.set(contextValueName, value);
                      csvEntries.add(DataMaintenanceUtils.toCsv(event, contextDesc));
                    } catch (JsonProcessingException e) {
                      // TODO(Naoki): Elaborate more
                      logger.error("Failed to encode a last N collection");
                    }
                  });

              return DataMaintenanceUtils.saveContextAsync(
                      "LastN contexts", contextDesc, csvEntries, System.currentTimeMillis(), state)
                  .thenComposeAsync(
                      (none) ->
                          ExecutionHelper.supply(
                              () -> {
                                final var keysToDelete = new ArrayList<List<Object>>();
                                latestContextEntries.forEach(
                                    (key, collection) -> {
                                      if (!collection.isEmpty()) {
                                        final var lastEntry = collection.get(collection.size() - 1);
                                        logger.debug(
                                            "{}: lastEntry {} ttl {}",
                                            key,
                                            lastEntry.getTimestamp(),
                                            maintenanceStartTime);
                                        if (lastEntry.getTimestamp() + ttl < maintenanceStartTime) {
                                          keysToDelete.add(List.of(key));
                                        }
                                      }
                                    });
                                if (!keysToDelete.isEmpty()) {
                                  final long deleteRequestTime = System.currentTimeMillis();
                                  return DataMaintenanceUtils.deleteContextEntriesAsync(
                                      tenantName,
                                      contextName,
                                      deleteRequestTime,
                                      keysToDelete,
                                      state);
                                }
                                return CompletableFuture.completedFuture(null);
                              }),
                      state.getExecutor());
            },
            state.getExecutor())
        .thenApply((none) -> events);
  }

  /**
   * Merges a new item to a collection.
   *
   * <p>This method assumes that the collection is sorted by timestamp in ascending order.
   *
   * @param collection The existing collection
   * @param newItem Item to be merged
   * @param since The oldest timestamp to keep, inclusive
   * @param limit The maximum size of trimmed collection. If the original collection is larger than
   *     this, older items are dropped.
   * @return Merged collection
   */
  protected static List<LastNItem> mergeNewItem(
      List<LastNItem> collection, LastNItem newItem, long since, long limit) {
    if (collection == null || collection.isEmpty()) {
      return List.of(newItem);
    }

    final var newCollection = new ArrayList<LastNItem>();
    final long newItemTimestamp = newItem.getTimestamp();
    if (newItemTimestamp < since) {
      newItem = null;
    }

    for (int index = 0; index < collection.size(); ++index) {
      final var currentItem = collection.get(index);
      long currentTimestamp = currentItem.getTimestamp();
      final boolean isDuplicate =
          newItem != null && newItem.getValue().equals(currentItem.getValue());
      if (newItem != null) {
        if (newItemTimestamp == currentTimestamp) {
          newCollection.add(newItem);
          newItem = null;
        } else if (newItemTimestamp < currentTimestamp) {
          newCollection.add(newItem);
          newItem = null;
        }
      }
      if (currentTimestamp >= since && !isDuplicate) {
        newCollection.add(currentItem);
      }
    }
    if (newItem != null) {
      newCollection.add(newItem);
    }

    return newCollection.size() <= limit
        ? newCollection
        : newCollection.subList(newCollection.size() - (int) limit, newCollection.size());
  }
}
