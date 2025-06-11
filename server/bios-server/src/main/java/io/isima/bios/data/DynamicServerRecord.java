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

import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.Event;
import io.isima.bios.models.ServerAttributeConfig;
import io.isima.bios.models.proto.DataProto;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/**
 * Server record with dynamically-configured column definitions.
 *
 * <p>Use this class instead of {@link ServerRecord} when record schema is unknown at the time of
 * record creation, such as select operation. Instantiate a record by using child class
 * DynamicServerRecord.DynamicEventFactory which shares a common set of column definitions.
 */
public class DynamicServerRecord extends ServerRecord {
  public static DynamicEventFactory newEventFactory(StreamDesc streamDesc) {
    return new DynamicEventFactory(streamDesc);
  }

  private final ColumnDefinitionsBuilder builder;

  private final StreamDesc streamDesc;

  protected DynamicServerRecord(ColumnDefinitionsBuilder builder, StreamDesc streamDesc) {
    super(builder.getDefinitions());
    this.builder = builder;
    this.streamDesc = streamDesc;
  }

  @Override
  protected ColumnDefinition validateDefinition(
      String name, DataProto.AttributeType expectedType, Object value) {
    Objects.requireNonNull(name);
    if (builder == null) {
      throw new IllegalStateException("The object is built in read only mode");
    }
    if (!definitions.containsKey(name)) {
      ServerAttributeConfig attr = streamDesc.getBiosAttribute(name);
      if (attr == null) {
        if (expectedType != null) {
          attr = new ServerAttributeConfig(name, AttributeType.fromProto(expectedType));
        } else if (value != null) {
          attr = new ServerAttributeConfig(name, AttributeType.infer(value));
        } else {
          throw new IllegalArgumentException("Invalid attribute name: " + name);
        }
      }
      builder.addAttribute(attr);
    }
    final var definition = definitions.get(name);
    if (expectedType != null && definition.getType() != expectedType) {
      throw new IllegalArgumentException("Invalid attribute type: " + name);
    }
    return definition;
  }

  public static class DynamicEventFactory implements EventFactory {

    private final ColumnDefinitionsBuilder builder;

    @Getter private final Map<String, ColumnDefinition> columnDefinitions;

    private final StreamDesc streamDesc;

    private DynamicEventFactory(StreamDesc streamDesc) {
      this.streamDesc = streamDesc;
      builder = new ColumnDefinitionsBuilder();
      columnDefinitions = builder.getDefinitions();
    }

    // caution: thread unsafe
    @Override
    public Event create() {
      return new DynamicServerRecord(builder, streamDesc).asEvent(true);
    }
  }
}
