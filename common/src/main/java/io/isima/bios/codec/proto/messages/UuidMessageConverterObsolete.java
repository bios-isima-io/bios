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

import io.isima.bios.models.proto.DataProto;
import java.util.UUID;

/**
 * Converts proto to UUID and vice versa in a machine independent form.
 *
 * <p>Caches the builder to allow for multiple UUID conversions in the same thread.
 */
public class UuidMessageConverterObsolete {
  private final DataProto.Uuid.Builder uuidBuilder;

  public UuidMessageConverterObsolete() {
    uuidBuilder = DataProto.Uuid.newBuilder();
  }

  /**
   * Converts to binary representation of UUID.
   *
   * @param uuid Actual UUID
   * @return binary representation of UUID
   */
  public DataProto.Uuid toProtoUuid(UUID uuid) {
    return uuidBuilder
        .setHi(uuid.getMostSignificantBits())
        .setLo(uuid.getLeastSignificantBits())
        .build();
  }

  /**
   * Converts to object representation of UUID.
   *
   * @param protoUuid binary representation of UUID
   * @return object representation of UUID
   */
  public static UUID fromProtoUuid(DataProto.Uuid protoUuid) {
    return new UUID(protoUuid.getHi(), protoUuid.getLo());
  }
}
