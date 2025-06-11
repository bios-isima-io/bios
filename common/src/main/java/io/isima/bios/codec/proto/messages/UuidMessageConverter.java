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
package io.isima.bios.codec.proto.messages;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public class UuidMessageConverter {

  public static ByteString toProtoUuid(UUID original) {
    final var buffer = ByteBuffer.allocate(16);
    buffer.putLong(original.getMostSignificantBits());
    buffer.putLong(original.getLeastSignificantBits());
    buffer.flip();
    return ByteString.copyFrom(buffer);
  }

  public static UUID fromProtoUuid(ByteString src) {
    Objects.requireNonNull(src);
    if (src.isEmpty()) {
      return null;
    }
    if (src.size() != 16) {
      throw new IllegalArgumentException("UUID byte length must be 16");
    }
    final var buf = ByteBuffer.wrap(src.toByteArray());
    return new UUID(buf.getLong(), buf.getLong());
  }
}
