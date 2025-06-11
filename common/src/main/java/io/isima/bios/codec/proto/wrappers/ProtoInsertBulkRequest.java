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

import io.isima.bios.dto.bulk.InsertBulkRequest;
import io.isima.bios.dto.bulk.RecordWithId;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.proto.DataProto;
import java.util.List;
import java.util.stream.Collectors;

public final class ProtoInsertBulkRequest implements InsertBulkRequest<String> {
  public String signal;
  public DataProto.InsertBulkRequest bulkRequest;
  public List<RecordWithId<String>> csvRecords;

  public ProtoInsertBulkRequest(DataProto.InsertBulkRequest request) {
    this.signal = request.getSignal();
    this.bulkRequest = request;
    this.csvRecords =
        this.bulkRequest.getRecordList().stream()
            .filter((x) -> x.getEventId().size() > 0 && x.getStringValuesCount() == 1)
            .map(record -> new CsvRecord(record))
            .collect(Collectors.toList());
  }

  public ProtoInsertBulkRequest() {}

  @Override
  public ContentRepresentation getContentRepresentation() {
    return ContentRepresentation.fromProto(bulkRequest.getContentRep());
  }

  @Override
  public String getSignalName() {
    return signal;
  }

  @Override
  public Long getSchemaVersion() {
    return bulkRequest.getSchemaVersion() == 0 ? null : bulkRequest.getSchemaVersion();
  }

  @Override
  public List<RecordWithId<String>> getRecords() {
    return csvRecords;
  }
}
