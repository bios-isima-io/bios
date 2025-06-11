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
package io.isima.bios.data.payload;

import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.dto.bulk.InsertBulkSuccessResponse;
import io.isima.bios.models.InsertBulkEachResult;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.Record;
import io.isima.bios.models.SelectResponseRecords;
import io.isima.bios.models.proto.DataProto;
import java.util.List;
import java.util.Objects;

/** Utility class that provides methods for miscellaneous payload data manipulations. */
public class PayloadUtils {

  /**
   * Converts an insert response protocol buffer message to a BIOS object.
   *
   * @param protoResp InsertSuccessResponse protobuf message
   * @return InsertResponseRecord BIOS2 object
   */
  public static InsertResponseRecord toBiosInsertResponse(
      DataProto.InsertSuccessResponse protoResp) {
    return new InsertResponseRecord(
        UuidMessageConverter.fromProtoUuid(protoResp.getEventId()), protoResp.getInsertTimestamp());
  }

  /**
   * Converts an insert response BIOS object to a protocol buffer message.
   *
   * @param record BIOS insert response record
   * @return InsertSuccessResponse protobuf message
   */
  public static DataProto.InsertSuccessResponse toProtoInsertResponse(InsertResponseRecord record) {
    return DataProto.InsertSuccessResponse.newBuilder()
        .setEventId(UuidMessageConverter.toProtoUuid(record.getEventId()))
        .setInsertTimestamp(record.getTimeStamp())
        .build();
  }

  private static DataProto.InsertSuccessResponse toProtoInsertResponse(Record record) {
    return DataProto.InsertSuccessResponse.newBuilder()
        .setEventId(UuidMessageConverter.toProtoUuid(record.getEventId()))
        .setInsertTimestamp(record.getTimestamp())
        .build();
  }

  public static DataProto.InsertSuccessResponse toProtoInsertResponse(IngestResponse response) {
    return DataProto.InsertSuccessResponse.newBuilder()
        .setEventId(UuidMessageConverter.toProtoUuid(response.getEventId()))
        .setInsertTimestamp(response.getIngestTimestamp().getTime())
        .build();
  }

  public static DataProto.InsertBulkSuccessResponse toProtoInsertBulkResponse(
      List<InsertBulkEachResult> results) {
    final var builder = DataProto.InsertBulkSuccessResponse.newBuilder();

    results.forEach(
        (result) -> {
          final var record = result.getRecord();
          assert record != null;
          builder.addResponses(toProtoInsertResponse(record));
        });

    return builder.build();
  }

  public static DataProto.InsertBulkSuccessResponse toProtoInsertBulkResponse(
      InsertBulkSuccessResponse results) {
    final var builder = DataProto.InsertBulkSuccessResponse.newBuilder();

    results
        .getResults()
        .forEach(
            (record) -> {
              builder.addResponses(toProtoInsertResponse(record));
            });

    return builder.build();
  }

  public static DataProto.SelectResponse toProtoMultiSelectResponse(
      List<SelectResponseRecords> biosResponses) {
    Objects.requireNonNull(biosResponses);
    final var builder = DataProto.SelectResponse.newBuilder();
    for (var response : biosResponses) {
      builder.addResponses(toProtoSelectResponse(response));
    }
    return builder.build();
  }

  public static DataProto.SelectQueryResponse toProtoSelectResponse(
      SelectResponseRecords biosResponse) {
    Objects.requireNonNull(biosResponse);
    final var builder = DataProto.SelectQueryResponse.newBuilder();
    for (var definition : biosResponse.getDefinitions().values()) {
      builder.addDefinitions(definition.asProtoMessage());
    }
    biosResponse
        .getDataWindows()
        .forEach(
            (ts, dataWindow) -> {
              final var rb = DataProto.QueryResult.newBuilder();
              rb.setWindowBeginTime(ts);
              // TODO(Naoki): Rebuilding the list here can be inefficient.
              dataWindow.forEach((record) -> rb.addRecords(record.asProtoRecord()));
              builder.addData(rb);
            });
    return builder.build();
  }
}
