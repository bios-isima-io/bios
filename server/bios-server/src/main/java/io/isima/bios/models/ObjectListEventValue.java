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

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;

public class ObjectListEventValue {
  private long timestamp;
  private UUID eventId;
  private final Object[] values;

  public ObjectListEventValue(Event event, List<String> attributeNames) {
    this.timestamp = event.getIngestTimestamp().getTime();
    this.eventId = event.getEventId();
    this.values = new Object[attributeNames.size()];
    int index = 0;
    for (var name : attributeNames) {
      values[index++] = event.get(name);
    }
  }

  private ObjectListEventValue(Object[] values) {
    this.values = values;
  }

  public Event toEvent(Map<String, Integer> attributeNameToIndex) {
    return new ObjectListEvent(attributeNameToIndex);
  }

  public static Event toEvent(Map<String, Integer> attributeNameToIndex, Object[] values) {
    if (requireNonNull(attributeNameToIndex).size() > requireNonNull(values).length) {
      throw new IllegalArgumentException(
          "Number of values must not be less than attributeNameToIndex");
    }
    return new ObjectListEventValue(values).toEvent(attributeNameToIndex);
  }

  /**
   * Event implementation backed by ObjectListEventValue.
   *
   * <p>Note that this class is not thread safe.
   */
  public class ObjectListEvent implements Event {
    final Map<String, Integer> attributeNameToIndex;
    final boolean[] masks;
    int numMasks;

    public ObjectListEvent(Map<String, Integer> attributeNameToIndex) {
      this.attributeNameToIndex = attributeNameToIndex;
      this.masks = new boolean[values.length];
      this.numMasks = 0;
    }

    @Override
    public UUID getEventId() {
      return eventId;
    }

    @Override
    public Event setEventId(UUID eid) {
      eventId = eid;
      return this;
    }

    @Override
    public Date getIngestTimestamp() {
      return new Date(timestamp);
    }

    @Override
    public Event setIngestTimestamp(Date ingestionTimestamp) {
      timestamp = ingestionTimestamp.getTime();
      return this;
    }

    @Override
    public Map<String, Object> getAttributes() {
      return new Attributes();
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
      for (var name : attributes.keySet()) {
        set(name, attributes.get(name));
      }
    }

    @Override
    public Map<String, Object> getAny() {
      return getAttributes();
    }

    @Override
    public void set(String name, Object value) {
      final var index = attributeNameToIndex.get(name);
      if (index == null) {
        return;
      }
      values[index] = value;
      if (masks[index]) {
        --numMasks;
      }
      masks[index] = false;
    }

    @Override
    public Object get(String name) {
      final var index = attributeNameToIndex.get(name);
      return index != null && !masks[index] ? values[index] : null;
    }

    public List<Object> getValueList() {
      return List.of(values);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("{");
      String delim = "";
      if (eventId != null) {
        sb.append("eventId=").append(eventId);
        delim = ", ";
      }
      if (timestamp > 0) {
        sb.append(delim).append("ingestTimestamp=").append(timestamp);
        delim = ", ";
      }
      sb.append(delim).append(new Attributes());
      return sb.toString();
    }

    private class Attributes implements Map<String, Object> {
      @Override
      public int size() {
        return attributeNameToIndex.size() - numMasks;
      }

      @Override
      public boolean isEmpty() {
        return size() == 0;
      }

      @Override
      public boolean containsKey(Object key) {
        final var index = attributeNameToIndex.get(key);
        return index != null && !masks[index];
      }

      @Override
      public boolean containsValue(Object value) {
        boolean contains = false;
        for (int i = 0; i < values.length; ++i) {
          if (Objects.equals(value, values[i])) {
            contains |= !masks[i];
          }
        }
        return contains;
      }

      @Override
      public Object get(Object key) {
        final var index = attributeNameToIndex.get(key);
        return index != null && !masks[index] ? values[index] : null;
      }

      @Override
      public Object put(String key, Object value) {
        set(key, value);
        return value;
      }

      /** Removes an entry. */
      @Override
      public Object remove(Object key) {
        final var index = attributeNameToIndex.get(key);
        if (index == null) {
          return null;
        }
        if (masks[index]) {
          return null;
        }
        masks[index] = true;
        ++numMasks;
        return values[index];
      }

      @Override
      public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + ".putAll");
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + ".clear");
      }

      @Override
      public Set<String> keySet() {
        return attributeNameToIndex.entrySet().stream()
            .filter((entry) -> !masks[entry.getValue()])
            .map((entry) -> entry.getKey())
            .collect(Collectors.toSet());
      }

      @Override
      public Collection<Object> values() {
        return attributeNameToIndex.entrySet().stream()
            .filter((entry) -> !masks[entry.getValue()])
            .map((entry) -> values[entry.getValue()])
            .collect(Collectors.toList());
      }

      @Override
      public Set<Entry<String, Object>> entrySet() {
        return attributeNameToIndex.entrySet().stream()
            .filter((entry) -> !masks[entry.getValue()])
            .map((entry) -> toEntry(entry.getKey()))
            .collect(Collectors.toSet());
      }

      private Map.Entry<String, Object> toEntry(String attributeName) {
        return new MyEntry(attributeName);
      }

      private class MyEntry implements Map.Entry<String, Object> {
        private final String name;

        public MyEntry(String name) {
          this.name = name;
        }

        @Override
        public String getKey() {
          return name;
        }

        @Override
        public Object getValue() {
          return get(name);
        }

        @Override
        public Object setValue(Object value) {
          set(name, value);
          return value;
        }

        @Override
        public boolean equals(Object other) {
          if (!(other instanceof Map.Entry)) {
            return false;
          }
          final var that = (Map.Entry) other;
          return Objects.equals(this.getKey(), that.getKey())
              && Objects.equals(this.getValue(), that.getValue());
        }

        @Override
        public int hashCode() {
          return (getKey() == null ? 0 : getKey().hashCode())
              ^ (getValue() == null ? 0 : getValue().hashCode());
        }
      }

      @Override
      public String toString() {
        final var joiner = new StringJoiner(", ", "[", "]");
        forEach((key, value) -> joiner.add(key + "=" + value));
        return joiner.toString();
      }
    }
  }
}
