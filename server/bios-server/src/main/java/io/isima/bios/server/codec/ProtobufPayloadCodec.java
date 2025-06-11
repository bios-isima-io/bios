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

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.exceptions.InternalServerErrorException;
import io.isima.bios.models.proto.DataProto.InsertBulkRequest;
import io.isima.bios.models.proto.DataProto.InsertBulkSuccessResponse;
import io.isima.bios.models.proto.DataProto.InsertRequest;
import io.isima.bios.models.proto.DataProto.InsertSuccessResponse;
import io.isima.bios.models.proto.DataProto.SelectRequest;
import io.isima.bios.models.proto.DataProto.SelectResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ProtobufPayloadCodec implements PayloadCodec {

  private static final Map<Class<? extends Message>, Supplier<? extends Builder>> builderProviders;

  static {
    // TODO can we register these dynamically?
    final var temp = new HashMap<Class<? extends Message>, Supplier<? extends Builder>>();
    temp.put(InsertRequest.class, InsertRequest::newBuilder);
    temp.put(InsertSuccessResponse.class, InsertSuccessResponse::newBuilder);
    temp.put(InsertBulkRequest.class, InsertBulkRequest::newBuilder);
    temp.put(InsertBulkSuccessResponse.class, InsertBulkSuccessResponse::newBuilder);
    temp.put(SelectRequest.class, SelectRequest::newBuilder);
    temp.put(SelectResponse.class, SelectResponse::newBuilder);
    builderProviders = Collections.unmodifiableMap(temp);
  }

  @Override
  public <T> T decode(ByteBuf payload, Class<T> clazz) throws InvalidRequestException {
    final var provider = builderProviders.get(clazz);
    if (provider == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported decoding content %s. Register the class to %s",
              clazz, ProtobufPayloadCodec.class));
    }
    final var builder = provider.get();
    final var input = new ByteBufInputStream(payload);
    try {
      return clazz.cast(builder.mergeFrom(input).build());
    } catch (IOException e) {
      throw new InvalidRequestException("Malformed protobuf payload: " + e.getMessage());
    }
  }

  @Override
  public <T> void encode(T content, ByteBuf payload) throws BiosServerException {
    if (!(content instanceof Message)) {
      throw new IllegalArgumentException(
          String.format(
              "Protobuf codec encodes only instance of %s; given %s is not",
              Message.class, content.getClass()));
    }
    final var message = Message.class.cast(content);
    final var output = new ByteBufOutputStream(payload);
    try {
      message.writeTo(output);
    } catch (IOException e) {
      throw new InternalServerErrorException("Encoding protobuf message failed", e);
    }
  }
}
