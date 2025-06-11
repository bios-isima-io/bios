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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class JsonPayloadCodec implements PayloadCodec {

  final ObjectMapper mapper;

  public JsonPayloadCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public <T> ByteBuffer encode(T data) throws BiosClientException {
    if (data == null) {
      throw new IllegalArgumentException("input data object must not be null");
    }
    // TODO(Naoki): This implementation copies serialized data
    try {
      byte[] byteArray = mapper.writeValueAsBytes(data);
      // The serialized payload must be terminated by null character
      // since some libraries in C-SDK handle the payload as a C string.
      // The output ByteBuffer has limit = byteArray length but
      // capacity = limit + 1
      ByteBuffer buf = ByteBuffer.allocateDirect(byteArray.length + 1);
      buf.put(byteArray);
      buf.put((byte) 0);
      buf.clear();
      buf.limit(byteArray.length);
      return buf;
    } catch (JsonProcessingException e) {
      throw new BiosClientException(e);
    }
  }

  @Override
  public <T> T decode(ByteBuffer payload, Class<T> clazz) throws BiosClientException {
    if (payload == null || clazz == null) {
      throw new IllegalArgumentException("parameters must not be null");
    }
    if (payload.limit() - payload.position() <= 0) {
      return null;
    }
    InputStream stream = new ByteBufferBackedInputStream(payload);
    try {
      T decoded = mapper.readValue(stream, clazz);
      return decoded;
    } catch (IOException e) {
      throw new BiosClientException(e);
    }
  }

  @Override
  public <T> T decode(ByteBuffer payload, TypeReference<T> typeReference)
      throws BiosClientException {
    if (payload == null || typeReference == null) {
      throw new IllegalArgumentException("parameters must not be null");
    }
    if (payload.limit() - payload.position() <= 0) {
      return null;
    }
    InputStream stream = new ByteBufferBackedInputStream(payload);
    try {
      T decoded = mapper.readValue(stream, typeReference);
      return decoded;
    } catch (IOException e) {
      throw new BiosClientException(e);
    }
  }
}
