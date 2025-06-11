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

import com.fasterxml.jackson.core.type.TypeReference;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.nio.ByteBuffer;

public interface PayloadCodec {

  <T> ByteBuffer encode(T data) throws BiosClientException;

  /**
   * Decodes a data byte buffer to an object to specified type.
   *
   * @param payload Source data byte buffer.
   * @param clazz Target class
   * @return The decoded object of the target class on successful decoding, or null when the length
   *     of the input stream is zero.
   * @throws BiosClientException When input data stream is invalid.
   */
  <T> T decode(ByteBuffer payload, Class<T> clazz) throws BiosClientException;

  /**
   * Decodes a data byte buffer to an object to specified type.
   *
   * @param payload Source data byte buffer.
   * @param typeReference Type reference of target class.
   * @return The decoded object of the target class on successful decoding, or null when the length
   *     of the input stream is zero.
   * @throws BiosClientException When input data stream is invalid.
   */
  <T> T decode(ByteBuffer payload, TypeReference<T> typeReference) throws BiosClientException;
}
