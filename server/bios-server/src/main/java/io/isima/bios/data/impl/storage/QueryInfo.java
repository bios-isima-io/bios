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

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.isima.bios.models.Range;
import java.util.Collection;
import lombok.Getter;

/** Immutable query info carrier used for EXTRACT operation. */
@Getter
public class QueryInfo {
  /** Cassandra statement used for this query. */
  private final Statement statement;

  private final Range timeRange;

  /** CassStream object for this query. */
  private final CassStream cassStream;

  private final Collection<CassAttributeDesc> attributes;

  public QueryInfo(
      Statement statement,
      Range timeRange,
      CassStream cassStream,
      Collection<CassAttributeDesc> attributes) {
    this.statement = statement;
    this.timeRange = timeRange;
    this.cassStream = cassStream;
    this.attributes = attributes;
  }

  public QueryInfo(
      String statement,
      Range timeRange,
      CassStream cassStream,
      Collection<CassAttributeDesc> attributes) {
    this.statement = new SimpleStatement(statement);
    this.timeRange = timeRange;
    this.cassStream = cassStream;
    this.attributes = attributes;
  }

  public <T extends CassStream> T getCassStream() {
    return (T) cassStream;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("{stream=")
            .append(cassStream.getStreamName())
            .append(", schemaVersion=")
            .append(cassStream.getSchemaVersion())
            .append(", statement=")
            .append(statement)
            .append("}");
    return sb.toString();
  }
}
