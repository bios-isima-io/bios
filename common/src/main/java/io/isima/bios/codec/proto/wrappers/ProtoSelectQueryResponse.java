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
package io.isima.bios.codec.proto.wrappers;

import io.isima.bios.codec.proto.RecordProtobufReader;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.models.SelectResponse;
import io.isima.bios.models.proto.DataProto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Protobuf implementation of {@link SelectResponse}.
 *
 * <p>Used when incoming query responses are protobuf messages.
 */
public class ProtoSelectQueryResponse extends SelectResponse {
  // Make everything immutable for thread safety
  private final List<DataProto.ColumnDefinition> definitions;
  private final int requestQueryNum;
  private final List<DataWindow> queryResults;

  ProtoSelectQueryResponse(DataProto.SelectQueryResponse queryResponse) {
    this.definitions = queryResponse.getDefinitionsList();
    this.requestQueryNum = queryResponse.getRequestQueryNum();

    // window order is guaranteed inside protobuf.
    final var srcDataList = queryResponse.getDataList();
    final var tmpList = new ArrayList<DataWindow>(srcDataList.size());
    for (var windowDataSource : srcDataList) {
      tmpList.add(new ProtoSelectQueryResult(windowDataSource));
    }

    queryResults = Collections.unmodifiableList(tmpList);
  }

  @Override
  public List<DataWindow> getDataWindows() {
    return queryResults;
  }

  @Override
  public int getRequestQueryNum() {
    return requestQueryNum;
  }

  private final class ProtoSelectQueryResult implements DataWindow {
    private final DataProto.QueryResult result;
    private final List<Record> records;

    private ProtoSelectQueryResult(DataProto.QueryResult queryResult) {
      this.result = queryResult;
      final var tmpList = new ArrayList<Record>(result.getRecordsList().size());
      for (var dataProto : result.getRecordsList()) {
        tmpList.add(new RecordProtobufReader(dataProto, definitions));
      }
      this.records = Collections.unmodifiableList(tmpList);
    }

    @Override
    public long getWindowBeginTime() {
      return result.getWindowBeginTime();
    }

    @Override
    public List<Record> getRecords() {
      return records;
    }
  }
}
