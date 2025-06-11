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
package io.isima.bios.data.impl;

import com.datastax.driver.core.Statement;

/**
 * Immutable query info carrier used for the secondary step of an index based SUMMARIZE operation
 * against a view table.
 */
class SummarizeViewQueryInfo {
  /** Generated statement for the target view table. */
  private final Statement statement;

  /** The timestamp that is associated with this query. Used to build the result object. */
  private final Long timestamp;

  /**
   * The class constructor.
   *
   * @param statement Statement used for the query
   * @param timestamp Timestamp associated with the query
   */
  public SummarizeViewQueryInfo(Statement statement, Long timestamp) {
    this.statement = statement;
    this.timestamp = timestamp;
  }

  public Statement getStatement() {
    return statement;
  }

  public Long getTimestamp() {
    return timestamp;
  }
}
