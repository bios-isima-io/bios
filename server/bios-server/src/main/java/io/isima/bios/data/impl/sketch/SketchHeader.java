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
package io.isima.bios.data.impl.sketch;

import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SketchHeader {
  protected final int version;

  public static SketchHeader fromBytes(byte[] headerBytes) {
    final var headerBuffer = ByteBuffer.wrap(headerBytes);
    final int version = headerBuffer.getInt();
    final SketchHeader header = new SketchHeader(version);
    return header;
  }

  public ByteBuffer toBytes() {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(this.version);

    buffer.flip();
    return buffer;
  }
}
