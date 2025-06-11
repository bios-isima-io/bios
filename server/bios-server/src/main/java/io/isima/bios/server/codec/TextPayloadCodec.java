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
package io.isima.bios.server.codec;

import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.BiosServerException;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.Objects;

public class TextPayloadCodec implements PayloadCodec {

  @Override
  public <T> T decode(ByteBuf payload, Class<T> clazz) throws TfosException {
    Objects.requireNonNull(payload);
    if (clazz != String.class) {
      throw new IllegalArgumentException(
          "Test/Plain codec supports only String class as the content type");
    }
    // TODO Do something if any non UTF-8 charset is necessary. We support only UTF-8 because of the
    // decoder interface.
    return clazz.cast(payload.toString(0, payload.readableBytes(), Charset.forName("UTF-8")));
  }

  @Override
  public <T> void encode(T content, ByteBuf payload) throws BiosServerException {
    Objects.requireNonNull(content);
    Objects.requireNonNull(payload);
    payload.writeBytes(content.toString().getBytes());
  }
}
