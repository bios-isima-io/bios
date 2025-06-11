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
package io.isima.bios.deli.importer;

/**
 * Keywords used for CDC Importer.
 */
public class CdcKeywords {
  // CDC Connector properties (MySQL specific?)
  public static final String AFTER = "after";
  public static final String BEFORE = "before";
  public static final String COLLECTION = "collection";
  public static final String DB = "db";
  public static final String OP = "op";
  public static final String PAYLOAD = "payload";
  public static final String SOURCE = "source";
  public static final String TABLE = "table";
  public static final String TS_MS = "ts_ms";

  public static final String C = "c";
  public static final String R = "r";
  public static final String U = "u";
  public static final String D = "d";

  // Additional attribute names
  public static final String OPERATION_LATENCY = "operationLatency";
  public static final String OPERATION_TYPE = "operationType";
  public static final String OPERATION_RESULT = "operationResult";
  public static final String TIMESTAMP = "timestamp";
  public static final String SQL_STATEMENT = "sqlStatement";
  public static final String DELTA_CHANGES = "deltaChanges";
  public static final String TABLE_NAME = "tableName";
}
