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
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.utils.EnumStringifier;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/** Attribute value type. */
public enum AttributeType {
  STRING(DataProto.AttributeType.STRING) {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return src;
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      return value != null ? value.asString() : null;
    }
  },
  INTEGER(DataProto.AttributeType.INTEGER) {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Long.valueOf(src);
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      return value != null ? Long.valueOf(value.asLong()) : null;
    }
  },
  DECIMAL(DataProto.AttributeType.DECIMAL) {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Double.valueOf(src);
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      return value != null ? Double.valueOf(value.asDouble()) : null;
    }
  },
  BLOB(DataProto.AttributeType.BLOB) {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Base64.getDecoder().decode(src.trim());
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      return value != null ? ByteBuffer.wrap(value.asByteArray()) : null;
    }
  },
  BOOLEAN(DataProto.AttributeType.BOOLEAN) {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Boolean.valueOf(src);
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      return value != null ? Boolean.valueOf(value.asBoolean()) : null;
    }
  },
  UNSUPPORTED(DataProto.AttributeType.STRING) {
    @Override
    Object fromString(String src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object attributeAsObject(AttributeValue value) {
      throw new UnsupportedOperationException();
    }
  };

  private final DataProto.AttributeType proto;

  AttributeType(DataProto.AttributeType proto) {
    this.proto = proto;
  }

  private static final Map<DataProto.AttributeType, AttributeType> mapFromProto =
      createMapFromProto();

  private static Map<DataProto.AttributeType, AttributeType> createMapFromProto() {
    final var map = new HashMap<DataProto.AttributeType, AttributeType>();
    for (final var value : values()) {
      map.put(value.proto, value);
    }
    return map;
  }

  public static AttributeType fromProto(DataProto.AttributeType protoType) {
    return mapFromProto.get(protoType);
  }

  public DataProto.AttributeType toProto() {
    return proto;
  }

  public static AttributeType infer(Object value) {
    if (value instanceof String || value instanceof Integer) {
      return STRING;
    }
    if (value instanceof Long) {
      return INTEGER;
    }
    if (value instanceof Double) {
      return DECIMAL;
    }
    if (value instanceof Boolean) {
      return BOOLEAN;
    }
    if (value instanceof ByteBuffer) {
      return BLOB;
    }
    return UNSUPPORTED;
  }

  private static final EnumStringifier<AttributeType> stringifier = new EnumStringifier<>(values());

  /**
   * Package-scope method that is used for converting string to the data of the entry's type.
   *
   * <p>The method is used for deserializing a JSON data element of the type.
   *
   * @param src Source string
   * @return Data of the entry's type.
   * @throws NullPointerException when the src is null.
   */
  abstract Object fromString(String src);

  /**
   * Per-type function to retrieve an attribute value as an Object.
   *
   * @param attributeValue The attribute value
   * @return The value as object
   */
  public abstract Object attributeAsObject(AttributeValue attributeValue);

  /**
   * Package-scope method that is used for deserializing the enum entry.
   *
   * @param value JSON data element
   * @return The decoded entry
   */
  @JsonCreator
  static AttributeType destringify(String value) {
    return stringifier.destringify(value);
  }

  /**
   * Package-scope method that is used for serializing the enum entry.
   *
   * @return The encoded string.
   */
  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
