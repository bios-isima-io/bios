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
package io.isima.bios.data.impl;

import static io.isima.bios.admin.v1.AdminConstants.CONTEXT_AUDIT_ATTRIBUTE_OPERATION;

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ActivityRecorder;
import io.isima.bios.data.impl.feature.Comparators;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.ObjectListEventValue;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class that generates view functions related to context Indexes. */
public class ContextIndexHelper {
  private static final Logger logger = LoggerFactory.getLogger(ContextIndexHelper.class);

  /** Function that sorts events in ascending order of ingest timestamp. */
  public static final Function<List<Event>, List<Event>> SORT_BY_TIMESTAMP =
      new Function<>() {
        @Override
        public List<Event> apply(List<Event> events) {
          Collections.sort(events, Comparators.TIMESTAMP);
          return events;
        }
      };

  public static Function<List<Event>, List<Event>> createSortFunction() {
    return SORT_BY_TIMESTAMP;
  }

  /**
   * Generates index entries from audit log events.
   *
   * @param contextDesc context descriptor
   * @param feature feature config to calculate events for
   * @return function
   */
  public static HashMap<String, List<Event>> collectIndexEntries(
      StreamDesc contextDesc,
      FeatureDesc feature,
      Range auditTimeRange,
      List<Event> events,
      ActivityRecorder recorder) {
    ActivityRecorder.log(recorder, "--------------------------------------");
    ActivityRecorder.log(recorder, "ContextIndexHelper.collectIndexEntries");
    ActivityRecorder.log(recorder, "events: %s", events);
    final var toAdd = new LinkedHashMap<List<Object>, Event>();
    final var toDelete = new LinkedHashMap<List<Object>, Event>();
    final var oldestEntries = new HashMap<List<Object>, Event>();

    final List<String> primaryKeyNames = contextDesc.getPrimaryKey();
    final long startTime = auditTimeRange.getBegin();
    final long endTime = auditTimeRange.getEnd();

    for (var event : events) {
      // Events are already sorted by timestamp, only process events in specified window
      if (event.getIngestTimestamp().getTime() < startTime) {
        continue;
      }
      if (event.getIngestTimestamp().getTime() >= endTime) {
        break;
      }

      final List<Object> primaryKey = DataUtils.makeCompositeKey(primaryKeyNames, event);

      String op = event.get(CONTEXT_AUDIT_ATTRIBUTE_OPERATION).toString();
      switch (op) {
        case "Update":
          {
            boolean indexChanged = false;
            boolean indexKeyChanged = false;
            final var oldEntry = new EventJson();
            for (var keyName : primaryKeyNames) {
              oldEntry.set(keyName, event.get(keyName));
            }
            oldEntry.setIngestTimestamp(event.getIngestTimestamp());
            for (var dimension : feature.getDimensions()) {
              final var oldValue = event.get(StringUtils.prefixToCamelCase("prev", dimension));
              oldEntry.set(dimension, oldValue);
              if (!Objects.equals(event.get(dimension), oldValue)) {
                indexChanged = true;
                indexKeyChanged = true;
              }
            }
            for (var attributeName : feature.getAttributes()) {
              final var oldValue = event.get(StringUtils.prefixToCamelCase("prev", attributeName));
              oldEntry.set(attributeName, oldValue);
              if (!Objects.equals(event.get(attributeName), oldValue)) {
                indexChanged = true;
              }
            }

            // No need to update if the index values are unchanged
            if (!indexChanged) {
              break;
            }
            // Old index entry has to be removed if the index key has changed;
            // the new entry does not overwrite
            if (indexKeyChanged) {
              toDelete.putIfAbsent(primaryKey, oldEntry);
            } else {
              oldestEntries.putIfAbsent(primaryKey, oldEntry);
            }

            // fall-through
          }
        case "Insert":
          {
            Event eventInMap = toAdd.computeIfAbsent(primaryKey, (key) -> new EventJson());
            eventInMap.setEventId(event.getEventId());
            eventInMap.setIngestTimestamp(event.getIngestTimestamp());
            for (var keyName : primaryKeyNames) {
              eventInMap.set(keyName, event.get(keyName));
            }
            for (var dimName : feature.getDimensions()) {
              eventInMap.set(dimName, event.get(dimName));
            }
            for (var attrName : feature.getAttributes()) {
              eventInMap.set(attrName, event.get(attrName));
            }
            break;
          }
        case "Delete":
          {
            // Cancel any previous addition
            if (toAdd.remove(primaryKey) != null) {
              final var oldest = oldestEntries.get(primaryKey);
              if (oldest != null) {
                toDelete.putIfAbsent(primaryKey, oldest);
              }
              break;
            }

            // Deletion is valid only for the first one
            if (toDelete.containsKey(primaryKey)) {
              break;
            }

            final var oldEntry = new EventJson();
            oldEntry.setEventId(event.getEventId());
            oldEntry.setIngestTimestamp(event.getIngestTimestamp());
            for (var keyName : primaryKeyNames) {
              oldEntry.set(keyName, event.get(keyName));
            }
            for (var dimName : feature.getDimensions()) {
              oldEntry.set(dimName, event.get(dimName));
            }
            for (var attrName : feature.getAttributes()) {
              oldEntry.set(attrName, event.get(attrName));
            }
            toDelete.put(primaryKey, oldEntry);
            break;
          }
        default:
          logger.error(
              "Found Unknown value in auditLog; tenant={}, context={}, _operation={}",
              contextDesc.getParent().getName(),
              contextDesc.getName(),
              op);
          toAdd.remove(primaryKey);
          toDelete.remove(primaryKey);
          break;
      }
    }
    ActivityRecorder.log(recorder, "toAdd: %s", toAdd);
    ActivityRecorder.log(recorder, "toDelete: %s", toDelete);

    HashMap<String, List<Event>> mergedEvents = new HashMap<String, List<Event>>();
    mergedEvents.put("new", new ArrayList<Event>());
    mergedEvents.get("new").addAll(toAdd.values());
    mergedEvents.put("deleted", new ArrayList<Event>());
    mergedEvents.get("deleted").addAll(toDelete.values());
    return mergedEvents;
  }

  /**
   * Method to create a function to generate list of Events for a particular context index feature.
   * This method should only be used for retroactive index population.
   *
   * @param contextDesc context descriptor
   * @param feature feature config to calculate events for
   * @return function
   */
  public static Function<List<Event>, List<Event>> createRetroactiveContextIndexFunction(
      StreamDesc contextDesc, FeatureDesc feature) {

    // Pre-compile schema objects necessary to build ObjectListEventValue entries
    final var allAttributeNames = new ArrayList<String>();
    allAttributeNames.addAll(contextDesc.getPrimaryKey());
    allAttributeNames.addAll(feature.getDimensions());
    allAttributeNames.addAll(feature.getAttributes());

    final var attributeNameToIndex = new HashMap<String, Integer>();
    for (int i = 0; i < allAttributeNames.size(); ++i) {
      attributeNameToIndex.put(allAttributeNames.get(i), i);
    }

    return new Function<>() {
      @Override
      public List<Event> apply(List<Event> events) {
        final var outEvents = new ArrayList<Event>();

        for (var event : events) {
          final var outEvent =
              new ObjectListEventValue(event, allAttributeNames).toEvent(attributeNameToIndex);
          outEvents.add(outEvent);
        }

        return outEvents;
      }
    };
  }
}
