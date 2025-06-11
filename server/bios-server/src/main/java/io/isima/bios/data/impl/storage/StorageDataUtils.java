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
package io.isima.bios.data.impl.storage;

import io.isima.bios.data.ServerRecord;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StorageDataUtils {
  /**
   * Method to convert list of events in Cassandra data types to ones in TFOS data types.
   *
   * <p>The method overwrites input parameter 'events' in place when conversion is necessary.
   *
   * @param events in/out data types
   * @param attributeDescs Corresponding attribute descriptors
   */
  public static void convertToOutputData(
      List<Event> events,
      Collection<? extends AttributeDesc> attributeDescs,
      String tenantName,
      String streamName) {
    if (events.isEmpty() || events.get(0) instanceof ServerRecord.EventAdapter) {
      // no need to convert
      return;
    }
    attributeDescs.stream()
        .filter(desc -> desc.getAttributeType().needsPlane2EngineConversion())
        .forEach(
            desc -> {
              events.forEach(
                  event -> {
                    // This is ugly but the Protobuf Event implementation is broken somewhere. We
                    // don't
                    // invest time here.
                    // Problem: When you set an attribute with a different value type from the
                    // existing one,
                    // all other events that share the column definitions underneath become
                    // unreachable.
                    // Replacing the entire attributes by HashMap would workaround this problem
                    // (somehow)
                    Map<String, Object> attributes = event.getAttributes();
                    if (!(attributes instanceof HashMap)) {
                      attributes = new LinkedHashMap<>(attributes);
                    }
                    final String name = desc.getName();
                    final Object value = event.get(name);
                    if (value != null) {
                      attributes.put(
                          name, Attributes.dataEngineToPlane(value, desc, tenantName, streamName));
                      event.setAttributes(attributes);
                    }
                  });
            });
  }

  /**
   * Search events sorted by timestamp for one with specified timestamp.
   *
   * @param events List of events. The events must be sorted by timestamp.
   * @param target The timestamp to look for.
   * @return Index of the event that has the specified timestamp. The smallest index is returned
   *     when multiple events match the specified timestamp. If no events exist for the target
   *     timestamp, the method returns the index of the first event larger than target. If all
   *     events are smaller than specified timestamp, the method returns events.size(). If the list
   *     events is empty, the method returns 0.
   */
  public static int search(List<Event> events, long target) {
    int left = 0;
    int right = events.size() - 1;
    while (left <= right) {
      int mid = (left + right) / 2;
      final Event event = events.get(mid);
      final long current = event.getIngestTimestamp().getTime();
      if (target <= current) {
        right = mid - 1;
      } else if (target > current) {
        left = mid + 1;
      }
    }
    return left;
  }
}
