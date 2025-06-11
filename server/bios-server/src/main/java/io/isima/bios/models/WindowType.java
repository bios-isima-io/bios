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

/** Enum that denotes select window types. */
public enum WindowType {
  GLOBAL(DataProto.WindowType.GLOBAL_WINDOW),
  TUMBLING(DataProto.WindowType.TUMBLING_WINDOW),
  SLIDING(DataProto.WindowType.SLIDING_WINDOW);

  private static final Map<DataProto.WindowType, WindowType> proto2bios;

  static {
    proto2bios = new HashMap<>();
    for (var entry : values()) {
      proto2bios.put(entry.typeProto, entry);
    }
  }

  private final DataProto.WindowType typeProto;

  private WindowType(DataProto.WindowType typeProto) {
    this.typeProto = typeProto;
  }

  public DataProto.WindowType toProto() {
    return typeProto;
  }

  /**
   * Converts ProtoBuf {@link DataProto.WindowType} to this enum.
   *
   * @param typeProto The source message
   * @return The converted enum
   */
  public static WindowType valueOf(DataProto.WindowType typeProto) {
    final var result = proto2bios.get(typeProto);
    if (result == null) {
      throw new IllegalArgumentException("Unknown protobuf type: " + typeProto);
    }
    return result;
  }
}
