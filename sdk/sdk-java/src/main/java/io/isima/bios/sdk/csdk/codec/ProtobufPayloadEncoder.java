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
package io.isima.bios.sdk.csdk.codec;

import com.google.protobuf.Message;
import java.nio.ByteBuffer;

/** Converts an incoming response ByteBuffer to protobuf and then to the desired class */
public class ProtobufPayloadEncoder {
  public static <T extends Message> ByteBuffer encode(T data) {
    if (data == null) {
      throw new IllegalArgumentException("input data object must not be null");
    }
    byte[] byteArray = data.toByteArray();
    ByteBuffer buf = ByteBuffer.allocateDirect(byteArray.length + 1);
    buf.put(byteArray);
    buf.flip();
    return buf;
  }
}
