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

import io.isima.bios.models.proto.DataProto;
import java.util.HashMap;
import java.util.Map;

/** Enum to indicate the type of biOS data representation. */
public enum ContentRepresentation {
  CSV(DataProto.ContentRepresentation.CSV),
  UNTYPED(DataProto.ContentRepresentation.UNTYPED);

  private final DataProto.ContentRepresentation proto;

  ContentRepresentation(DataProto.ContentRepresentation proto) {
    this.proto = proto;
  }

  private static final Map<DataProto.ContentRepresentation, ContentRepresentation> mapFromProto =
      createMapFromProto();

  private static Map<DataProto.ContentRepresentation, ContentRepresentation> createMapFromProto() {
    final var map = new HashMap<DataProto.ContentRepresentation, ContentRepresentation>();
    for (final var value : values()) {
      map.put(value.proto, value);
    }
    return map;
  }

  public static ContentRepresentation fromProto(DataProto.ContentRepresentation proto) {
    return mapFromProto.get(proto);
  }

  public DataProto.ContentRepresentation toProto() {
    return proto;
  }
}
