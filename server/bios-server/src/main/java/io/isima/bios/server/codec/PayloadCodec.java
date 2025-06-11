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

/** Interface that provides functionality of encoding and decoding content against Netty ByteBuf. */
public interface PayloadCodec {

  /**
   * Decodes a <code>ByteBuf</code> payload to a content object.
   *
   * @param <T> Content type
   * @param payload Source byte buffer
   * @param contentClass Class object of the content type.
   * @return Decoded content
   * @throws BiosServerException thrown to indicate a decoding error has occurred.
   */
  <T> T decode(ByteBuf payload, Class<T> contentClass) throws TfosException;

  /**
   * Encodes a content object to a <code>ByteBuf</code> payload.
   *
   * @param <T> Content type
   * @param content Source content object.
   * @param payload The payload to encode into.
   * @throws BiosServerException thrown to indicate an encoding error has occurred.
   */
  <T> void encode(T content, ByteBuf payload) throws BiosServerException;
}
