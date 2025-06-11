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
package io.isima.bios.models;

import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.models.isql.ISqlResponseType;
import java.util.Collections;
import java.util.List;

public class InsertResponse implements ISqlResponse {

  public InsertResponse() {
    //
  }

  private List<Record> records;

  @Override
  public final ISqlResponseType getResponseType() {
    return ISqlResponseType.INSERT;
  }

  public InsertResponse(List<Record> records) {
    this.records = Collections.unmodifiableList(records);
  }

  @Override
  public List<Record> getRecords() {
    return records;
  }

  @Override
  public List<DataWindow> getDataWindows() {
    return List.of();
  }
}
