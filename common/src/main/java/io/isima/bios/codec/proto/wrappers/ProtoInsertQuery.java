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

import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.isql.InsertStatement;
import java.util.List;

public class ProtoInsertQuery implements InsertStatement {
  private final boolean bulk;
  private final String csv;
  private final List<String> csvBulk;
  private final String signalName;

  public ProtoInsertQuery(String into, String csv) {
    this.signalName = into;
    this.bulk = false;
    this.csv = csv;
    this.csvBulk = null;
  }

  public ProtoInsertQuery(String into, List<String> csvBulk) {
    this.signalName = into;
    this.bulk = true;
    this.csv = null;
    this.csvBulk = csvBulk;
  }

  @Override
  public ContentRepresentation getContentRepresentation() {
    // currently only CSV is supported
    return ContentRepresentation.CSV;
  }

  @Override
  public boolean isBulk() {
    return bulk;
  }

  @Override
  public String getCsv() {
    return this.csv;
  }

  @Override
  public List<String> getCsvBulk() {
    return this.csvBulk;
  }

  @Override
  public String getSignalName() {
    return this.signalName;
  }
}
