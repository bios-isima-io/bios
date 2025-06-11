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
package io.isima.bios.data;

import io.isima.bios.models.Event;
import io.isima.bios.models.ServerAttributeConfig;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class ServerRecord extends DefaultRecord {

  public ServerRecord(Map<String, ColumnDefinition> columnDefinitions) {
    super(columnDefinitions);
  }

  public EventAdapter asEvent(boolean convertAllowedValues) {
    return new EventAdapter(this, convertAllowedValues);
  }

  public static class EventAdapter implements Event {

    private final ServerRecord me;
    private final boolean convertAllowedValues;

    public EventAdapter(ServerRecord me, boolean convertAllowedValues) {
      this.me = me;
      this.convertAllowedValues = convertAllowedValues;
    }

    @Override
    public UUID getEventId() {
      return me.uuid;
    }

    @Override
    public Event setEventId(UUID eventId) {
      me.setEventId(eventId);
      return this;
    }

    @Override
    public Date getIngestTimestamp() {
      return me.timestamp != null ? new Date(me.timestamp) : new Date(0);
    }

    @Override
    public Event setIngestTimestamp(Date ingestionTimestamp) {
      me.setTimestamp(ingestionTimestamp.getTime());
      return this;
    }

    @Override
    public Map<String, Object> getAttributes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getAny() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void set(String name, Object value) {
      final var definition = me.validateDefinition(name, null, value);
      if (convertAllowedValues) {
        if (definition != null && definition.getAttribute() != null) {
          final var attribute = definition.getAttribute();
          if (attribute != null) {
            if (attribute.getAllowedValues() != null && value instanceof Integer) {
              me.set(definition, attribute.getAllowedValues().get((Integer) value).asString());
              return;
            }
          }
        }
      }
      me.set(name, value);
    }

    @Override
    public Object get(String name) {
      final var attr = me.getAttribute(name);
      if (attr == null) {
        return null;
      }
      if (convertAllowedValues) {
        final ServerAttributeConfig attribute =
            (ServerAttributeConfig) attr.getDefinition().getAttribute();
        if (attribute.hasAllowedValues()) {
          return attribute.getEnumIndex(attr.asString());
        }
      }
      return attr.asObject();
    }

    public ServerRecord asRecord() {
      return me;
    }

    @Override
    public String toString() {
      final String comma = ",";
      final StringBuilder sb = new StringBuilder("{");
      boolean appendDelim = false;
      if (getEventId() != null) {
        sb.append("eventId=").append(getEventId());
        appendDelim = true;
      }
      if (me.timestamp != null) {
        if (appendDelim) {
          sb.append(comma);
        }
        sb.append("ingestTimestamp=").append(me.timestamp);
        appendDelim = true;
      }
      if (appendDelim) {
        sb.append(", [");
      } else {
        sb.append("[");
      }
      appendDelim = false;
      for (String key : me.definitions.keySet()) {
        if (appendDelim) {
          sb.append(comma);
        }
        sb.append(key).append("=").append(get(key));
        appendDelim = true;
      }
      return sb.append("]}").toString();
    }
  }
}
