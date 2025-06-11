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
package io.isima.bios.models.isql;

import io.isima.bios.models.ContentRepresentation;
import java.util.List;

/**
 * Select query on a context.
 *
 * <p>Since a select on a context may be entirely different and may evolve independently of select
 * queries on a signal, a separate interface is provided for ContextSelect.
 */
public interface ContextSelectStatement extends ISqlStatement {
  /**
   * Return how the content is represented.
   *
   * @return content representation
   */
  ContentRepresentation getContentRepresentation();

  /**
   * Returns name of the context table from which to select records.
   *
   * @return signal table name
   */
  String getContextName();

  /**
   * Returns the where clause in the query (selection criteria).
   *
   * @return where clause of the query as a string.
   */
  List<List<Object>> getPrimaryKeys();

  /**
   * Returns attributes in the projection.
   *
   * <p>Currently only primary key is supported as an attribute
   *
   * @return list of attributes projected in the query
   */
  List<String> getSelectAttributes();

  @Override
  default StatementType getStatementType() {
    return StatementType.CONTEXT_SELECT;
  }
}
