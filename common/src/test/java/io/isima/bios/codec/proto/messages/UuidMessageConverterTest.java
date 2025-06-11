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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.isima.bios.models.proto.DataProto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;

public class UuidMessageConverterTest {

  // UUID source for performance test
  private static List<UUID> uuidSrc;

  @BeforeClass
  public static void setUpClass() {
    final int numEntries = 1000000;
    uuidSrc = new ArrayList<>(numEntries);
    final var random = new Random();
    for (int i = 0; i < numEntries; ++i) {
      final var src = new byte[16];
      random.nextBytes(src);
      uuidSrc.add(UUID.randomUUID());
    }
  }

  @Test
  public void testConversion() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, -1, -2, -3, -4, 9, 10, 11, 12, -9, -10, -11, -12};
    ByteString uuidSrc = ByteString.copyFrom(data);
    UUID uuid = UuidMessageConverter.fromProtoUuid(uuidSrc);
    assertEquals("01020304-fffe-fdfc-090a-0b0cf7f6f5f4", uuid.toString());

    ByteString uuidEncoded = UuidMessageConverter.toProtoUuid(uuid);
    assertArrayEquals(data, uuidEncoded.toByteArray());
  }

  @Test
  public void bytesPerformance() throws InvalidProtocolBufferException {
    final var encoded = new ArrayList<byte[]>(uuidSrc.size());
    // final long start = System.currentTimeMillis();
    for (UUID uuid : uuidSrc) {
      final var converted = UuidMessageConverter.toProtoUuid(uuid);
      final var record = DataProto.Record.newBuilder().setEventId(converted).build();
      encoded.add(record.toByteArray());
    }
    // long checkpoint = System.currentTimeMillis();

    final var restored = new ArrayList<UUID>(uuidSrc.size());
    for (byte[] src : encoded) {
      final var decoded = DataProto.Record.parseFrom(src);
      restored.add(UuidMessageConverter.fromProtoUuid(decoded.getEventId()));
    }
    /*
     * System.out.println("bytes: elapsed=" + (checkpoint - start) + ", "
     * + (System.currentTimeMillis() - checkpoint));
     */

    assertEquals(uuidSrc, restored);
  }

  @Test
  public void uuidPerformance() throws InvalidProtocolBufferException {
    final var encoded = new ArrayList<byte[]>(uuidSrc.size());
    // final long start = System.currentTimeMillis();
    for (UUID uuid : uuidSrc) {
      final var converted = new UuidMessageConverterObsolete().toProtoUuid(uuid);
      encoded.add(converted.toByteArray());
    }
    // final long checkpoint = System.currentTimeMillis();

    final var restored = new ArrayList<UUID>(uuidSrc.size());
    for (byte[] src : encoded) {
      final var decoded = DataProto.Uuid.parseFrom(src);
      restored.add(UuidMessageConverterObsolete.fromProtoUuid(decoded));
    }

    /*
     * System.out.println(
     * "Uuid: elapsed=" + (checkpoint - start) + ", " + (System.currentTimeMillis() - checkpoint));
     */

    assertEquals(uuidSrc, restored);
  }
}
