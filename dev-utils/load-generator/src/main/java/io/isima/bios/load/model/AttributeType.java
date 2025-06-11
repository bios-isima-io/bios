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
package io.isima.bios.load.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Base64;

/**
 * Attribute value type.
 */
public enum AttributeType {
  STRING {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return src;
    }
  },
  INTEGER {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Long.valueOf(src);
    }
  },
  DECIMAL {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Double.valueOf(src);
    }
  },
  BLOB {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Base64.getDecoder().decode(src.trim());
    }
  },
  BOOLEAN {
    @Override
    Object fromString(String src) {
      if (src == null) {
        throw new NullPointerException();
      }
      return Boolean.valueOf(src);
    }
  };

  private static final EnumStringifier<AttributeType> stringifier = new EnumStringifier<>(values());

  /**
   * Package-scope method that is used for converting string to the data of the entry's type.
   *
   * <p>
   * The method is used for deserializing a JSON data element of the type.
   * </p>
   *
   * @param src Source string
   * @return Data of the entry's type.
   * @throws NullPointerException when the src is null.
   */
  abstract Object fromString(String src);

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
  String stringify() {
    return stringifier.stringify(this);
  }
}
