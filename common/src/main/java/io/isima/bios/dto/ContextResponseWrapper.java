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
package io.isima.bios.dto;

import io.isima.bios.models.Attribute;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValue;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextEntryRecord;
import io.isima.bios.models.ContextSelectResponse;
import io.isima.bios.models.Record;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.ToString;

/** Transforms GetContext output to a list of records for easier external consumption. */
@ToString
public final class ContextResponseWrapper extends ContextSelectResponse {
  private final List<DefaultContextRecord> records;

  public ContextResponseWrapper(GetContextEntriesResponse response) {
    this.records =
        response.getEntries().stream()
            .map((x) -> new DefaultContextRecord(x, response.getDefinitions()))
            .collect(Collectors.toList());
  }

  public ContextResponseWrapper(SelectContextEntriesResponse response) {
    this.records =
        response.getEntries().stream()
            .map((x) -> new DefaultContextRecord(x, response.getDefinitions()))
            .collect(Collectors.toList());
  }

  @Override
  public List<? extends Record> getRecords() {
    return records;
  }

  private static class DefaultContextRecord implements Record {
    private final Map<String, ContextAttribute> attributeMap;
    private final Long timeStamp;

    private DefaultContextRecord(ContextEntryRecord record, List<AttributeConfig> definitions) {
      final var size = record.getAttributes().size();
      if (size > 0) {
        this.attributeMap =
            IntStream.range(0, size)
                .mapToObj(
                    (i) ->
                        new ContextAttribute(
                            record.getAttributes().get(i),
                            definitions.get(i).getType(),
                            definitions.get(i).getName()))
                .collect(Collectors.toMap(Attribute::name, k -> k));
      } else {
        this.attributeMap = Collections.emptyMap();
      }
      this.timeStamp = record.getTimestamp();
    }

    private DefaultContextRecord(
        Map<String, Object> entry, Map<String, AttributeType> definitions) {
      this.attributeMap =
          entry.entrySet().stream()
              .map(
                  (x) ->
                      new ContextAttribute(x.getValue(), definitions.get(x.getKey()), x.getKey()))
              .collect(Collectors.toMap(Attribute::name, k -> k));
      this.timeStamp = 0L;
    }

    @Override
    public UUID getEventId() {
      return null;
    }

    @Override
    public Long getTimestamp() {
      return timeStamp;
    }

    @Override
    public Collection<? extends Attribute> attributes() {
      return attributeMap.values();
    }

    @Override
    public AttributeValue getAttribute(String name) {
      return attributeMap.get(name);
    }

    @Override
    public String toString() {
      return attributeMap.toString();
    }
  }

  private static class ContextAttribute extends AttributeValueGeneric implements Attribute {
    private final String name;

    private ContextAttribute(Object value, AttributeType type, String name) {
      super(value, type);
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return asString();
    }
  }
}
