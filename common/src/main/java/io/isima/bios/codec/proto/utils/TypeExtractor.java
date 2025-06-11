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
package io.isima.bios.codec.proto.utils;

import io.isima.bios.models.proto.DataProto;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TypeExtractor {
  private static final Map<Class, DataProto.AttributeType> attributeTypeMap;

  static {
    Map<Class, DataProto.AttributeType> m = new HashMap<>();
    m.put(Long.class, DataProto.AttributeType.INTEGER);
    m.put(Double.class, DataProto.AttributeType.DECIMAL);
    m.put(String.class, DataProto.AttributeType.STRING);
    m.put(Boolean.class, DataProto.AttributeType.BOOLEAN);
    m.put(ByteBuffer.class, DataProto.AttributeType.BLOB);

    attributeTypeMap = Collections.unmodifiableMap(m);
  }

  /**
   * Extracts the protobuf representation of any attribute.
   *
   * <p>For Protobuf to support any object its type should be known.
   *
   * @param value actual value object from which type should be extracted
   * @return attribute type known by protobuf, UNKNOWN otherwise
   */
  public static DataProto.AttributeType extractType(Object value) {
    Class<?> clazz = value.getClass();
    DataProto.AttributeType type = null;
    while (type == null && clazz != Object.class) {
      type = attributeTypeMap.get(clazz);
      clazz = clazz.getSuperclass();
    }
    return (type == null) ? DataProto.AttributeType.UNKNOWN : type;
  }
}
