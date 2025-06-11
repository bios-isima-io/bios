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

public interface InsertStatement extends AsyncExecutableISqlStatement {
  ContentRepresentation getContentRepresentation();

  boolean isBulk();

  String getCsv();

  List<String> getCsvBulk();

  String getSignalName();

  @Override
  default StatementType getStatementType() {
    return StatementType.INSERT;
  }

  interface Into {
    /**
     * Specify name of the signal to insert.
     *
     * @param into Name of the signal
     * @return Next step in the query builder process
     */
    InsertData into(String into);
  }

  interface InsertData {
    InsertBuilder csv(String text);

    InsertBuilder csvBulk(List<String> texts);

    InsertBuilder csvBulk(String... texts);

    InsertBuilder values(List<Object> values);

    InsertBuilder values(Object... values);

    InsertBuilder valuesBulk(List<List<Object>> valuesList);
  }

  interface LinkedBuilder extends Into, InsertData {}
}
