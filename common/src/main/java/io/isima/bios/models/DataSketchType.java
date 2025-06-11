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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

/** The list of data sketches that can be built as part of signal features. */
@Getter
public enum DataSketchType {
  NONE((byte) 0),
  MOMENTS((byte) 1),
  QUANTILES((byte) 2),
  DISTINCT_COUNT((byte) 3),
  SAMPLE_COUNTS((byte) 4),
  LAST_N((byte) 5) {
    @Override
    public boolean isGenericDigestor() {
      return true;
    }
  },
  ;
  // Planned to add soon: FREQUENT_ITEMS;

  private final byte proxy;

  private static final Map<Byte, DataSketchType> proxyToDataSketchTypeMap =
      createProxyToDataSketchTypeMap();

  DataSketchType(byte proxy) {
    this.proxy = proxy;
  }

  private static Map<Byte, DataSketchType> createProxyToDataSketchTypeMap() {
    final var map = new HashMap<Byte, DataSketchType>();

    for (DataSketchType type : values()) {
      map.put(type.proxy, type);
    }

    return map;
  }

  public static DataSketchType fromProxy(byte proxy) {
    return proxyToDataSketchTypeMap.get(proxy);
  }

  public boolean hasBlob() {
    if (this == NONE) {
      throw new UnsupportedOperationException();
    }
    if (this == MOMENTS) {
      return false;
    }
    return true;
  }

  /**
   * "Generic digestor" means a replacement of rollup.
   *
   * <p>It is different from a regular data sketch that is managed per-attribute basis. A generic
   * digestor is attached more tightly to the feature/signal.
   *
   * @return
   */
  public boolean isGenericDigestor() {
    return false;
  }

  private static final EnumStringifier<DataSketchType> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  public static DataSketchType forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
