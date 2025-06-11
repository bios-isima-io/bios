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
package io.isima.bios.data.filter;

import com.datastax.driver.core.Row;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.CqlFilterCompiler;
import io.isima.bios.errors.exception.InvalidFilterException;
import lombok.Getter;
import org.apache.cassandra.cql3.SingleColumnRelation;

public class FilterElement {
  @Getter private final String column;

  private final FilterTerm term;

  public FilterElement(CassAttributeDesc attributeDesc, SingleColumnRelation relation)
      throws InvalidFilterException {
    column = attributeDesc.getColumn();
    term = CqlFilterCompiler.createTerm(relation, attributeDesc);
  }

  public boolean test(Row row) {
    return test(row.getObject(column));
  }

  public boolean test(Object value) {
    return term.test(value);
  }
}
