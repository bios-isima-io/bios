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

import com.google.protobuf.InvalidProtocolBufferException;
import io.isima.bios.codec.proto.wrappers.ProtoInsertBulkResponse;
import io.isima.bios.codec.proto.wrappers.ProtoInsertResponse;
import io.isima.bios.codec.proto.wrappers.ProtoSelectResponse;
import io.isima.bios.models.MultiSelectResponse;
import io.isima.bios.models.Record;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.nio.ByteBuffer;

public class ProtobufDecoders {
  public static MultiSelectResponse decodeSelectResponse(ByteBuffer payload)
      throws BiosClientException {
    try {
      DataProto.SelectResponse resp =
          (payload == null)
              ? DataProto.SelectResponse.newBuilder().build()
              : DataProto.SelectResponse.parseFrom(payload);
      return new ProtoSelectResponse(resp);
    } catch (InvalidProtocolBufferException e) {
      throw new BiosClientException("Invalid response from server");
    }
  }

  public static Record decodeInsertResponse(ByteBuffer payload) throws BiosClientException {
    try {
      DataProto.InsertSuccessResponse resp =
          (payload == null)
              ? DataProto.InsertSuccessResponse.newBuilder().build()
              : DataProto.InsertSuccessResponse.parseFrom(payload);
      return new ProtoInsertResponse(resp);
    } catch (InvalidProtocolBufferException e) {
      throw new BiosClientException("Invalid response from server");
    }
  }

  public static ProtoInsertBulkResponse decodeInsertBulkResponse(ByteBuffer payload)
      throws BiosClientException {
    try {
      DataProto.InsertBulkSuccessResponse resp =
          (payload == null)
              ? DataProto.InsertBulkSuccessResponse.newBuilder().build()
              : DataProto.InsertBulkSuccessResponse.parseFrom(payload);
      return new ProtoInsertBulkResponse(resp);
    } catch (InvalidProtocolBufferException e) {
      throw new BiosClientException("Invalid response from server");
    }
  }
}
