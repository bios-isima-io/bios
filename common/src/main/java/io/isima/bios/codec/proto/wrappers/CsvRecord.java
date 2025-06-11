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

import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.dto.bulk.RecordWithId;
import io.isima.bios.models.proto.DataProto;
import java.util.UUID;

public final class CsvRecord implements RecordWithId<String> {
  private final UUID uuid;
  private final String csv;

  public CsvRecord(DataProto.Record record) {
    this.uuid = UuidMessageConverter.fromProtoUuid(record.getEventId());
    this.csv = record.getStringValues(0);
  }

  public CsvRecord(com.google.protobuf.ByteString eventId, String csv) {
    this.uuid = UuidMessageConverter.fromProtoUuid(eventId);
    this.csv = csv;
  }

  @Override
  public UUID getRecordId() {
    return uuid;
  }

  @Override
  public String getRecord() {
    return csv;
  }
}
