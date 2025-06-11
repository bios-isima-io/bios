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
package io.isima.bios.data.impl.storage;

import io.isima.bios.data.StreamId;
import io.isima.bios.models.TenantInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
public class TableInfo {
  private final String keyspace;
  private final String tableName;
  private final TenantInfo tenantInfo;
  private final StreamId streamId;

  @Setter private String parentStream;
  @Setter private Long parentStreamVersion;

  public TableInfo(String keyspace, String tableName, TenantInfo tenantInfo, StreamId streamId) {
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.tenantInfo = tenantInfo;
    this.streamId = streamId;
  }
}
